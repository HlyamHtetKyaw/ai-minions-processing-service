package com.aiminions.processingservice.jobs.balancedsync;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aiminions.processingservice.jobs.balancedsync.SilenceAnchorChunker.SceneChunk;

public final class AnchorSyncPlanner {

	private static final Logger log = LoggerFactory.getLogger(AnchorSyncPlanner.class);

	/**
	 * Practical bias so the encoded video timeline is slightly longer than decoded audio EOF;
	 * {@code -shortest} then trims redundant video tail instead of clipping narration.
	 * (Tunable for deployment if needed.)
	 */
	private static final long FRAME_QUANTIZATION_BUFFER_MS = 100L;

	private static final double MULTIPLIER_WARN_EXTREME = 4.0d;

	private AnchorSyncPlanner() {}

	public enum SegmentKind { TALK, GAP }

	public record SegmentPlan(
			int index,
			SegmentKind kind,
			long srcStartMs,
			long srcEndMs,
			long targetDurationMs,
			double setptsMultiplier
	) {
	}

	public record PlanResult(
			List<SegmentPlan> segments,
			int originalChunkCount,
			int voiceChunkCount,
			long originalEndMs,
			long voiceEndMs
	) {}

	/**
	 * Builds a segment-by-segment plan following the business rules:
	 * - Chunk by anchor gaps ({@code anchorGapMs}) in both SRTs.
	 * - Match chunks sequentially.
	 * - Talking chunks: retime video with {@code setpts} multiplier {@code voiceDur/originalDur}.
	 * - Anchor gaps: keep video at 1.0× (no setpts stretch), but re-encode for concat stability.
	 * - If Original has more chunks than Voice: drop remaining video (pairs up to {@code common}).
	 *
	 * @param leadPadMs     lead video padding (ms) applied on segment index 0 in the pipeline — single source of truth
	 * @param trueVoiceDurationMs physical narration length from ffprobe — used to stretch the last TALK segment instead of tail tpad
	 */
	public static PlanResult build(
			List<SrtParser.Cue> originalCues,
			List<SrtParser.Cue> voiceCues,
			long anchorGapMs,
			long trueVoiceDurationMs,
			long leadPadMs
	) {
		List<SceneChunk> oChunks = SilenceAnchorChunker.chunk(originalCues, anchorGapMs);
		List<SceneChunk> vChunks = SilenceAnchorChunker.chunk(voiceCues, anchorGapMs);
		if (oChunks.isEmpty() || vChunks.isEmpty()) {
			return new PlanResult(List.of(), oChunks.size(), vChunks.size(), 0, 0);
		}

		int common = Math.min(oChunks.size(), vChunks.size());
		List<SegmentPlan> out = new ArrayList<>();
		int segIdx = 0;

		for (int i = 0; i < common; i++) {
			SceneChunk o = oChunks.get(i);
			SceneChunk v = vChunks.get(i);
			long oDur = Math.max(1, o.durationMs());
			long vDur = Math.max(1, v.durationMs());
			double mul = ((double) vDur) / ((double) oDur);
			if (!Double.isFinite(mul) || mul <= 0.000001d) mul = 1d;

			out.add(new SegmentPlan(
					segIdx++,
					SegmentKind.TALK,
					o.startMs(),
					o.endMs(),
					vDur,
					mul
			));

			// Gap after this chunk (except after last mapped chunk): keep original timing.
			if (i + 1 < common) {
				SceneChunk next = oChunks.get(i + 1);
				long gapStart = o.endMs();
				long gapEnd = next.startMs();
				long gapDur = Math.max(0, gapEnd - gapStart);
				if (gapDur > 0) {
					out.add(new SegmentPlan(
							segIdx++,
							SegmentKind.GAP,
							gapStart,
							gapEnd,
							gapDur,
							1.0d
					));
				}
			}
		}

		long voiceEndMs = lastEndMs(voiceCues);
		long originalEndMs = lastEndMs(originalCues);

		stretchLastTalkForTrueVoice(out, trueVoiceDurationMs, leadPadMs);

		return new PlanResult(out, oChunks.size(), vChunks.size(), originalEndMs, voiceEndMs);
	}

	/**
	 * Walks backwards for last {@link SegmentKind#TALK}; aligns its output duration to physical voice length.
	 */
	private static void stretchLastTalkForTrueVoice(List<SegmentPlan> out, long trueVoiceDurationMs, long leadPadMs) {
		int lastTalkIdx = -1;
		for (int i = out.size() - 1; i >= 0; i--) {
			if (out.get(i).kind() == SegmentKind.TALK) {
				lastTalkIdx = i;
				break;
			}
		}
		if (lastTalkIdx < 0) {
			log.warn("balanced-sync plan: no TALK segment; skipping true-voice tail stretch");
			return;
		}

		long prior = Math.max(0L, leadPadMs);
		for (int i = 0; i < lastTalkIdx; i++) {
			prior += Math.max(0L, out.get(i).targetDurationMs());
		}

		long requiredFinalChunkMs = trueVoiceDurationMs - prior + FRAME_QUANTIZATION_BUFFER_MS;

		SegmentPlan lastTalk = out.get(lastTalkIdx);
		if (requiredFinalChunkMs <= lastTalk.targetDurationMs()) {
			log.debug(
					"balanced-sync tail: no stretch (requiredFinal={} ms <= paired target={} ms; prior={} ms, trueVoice={} ms)",
					requiredFinalChunkMs,
					lastTalk.targetDurationMs(),
					prior,
					trueVoiceDurationMs);
			return;
		}

		long srcDurationMs = Math.max(1L, lastTalk.srcEndMs() - lastTalk.srcStartMs());
		double multiplier = ((double) requiredFinalChunkMs) / ((double) srcDurationMs);
		if (!Double.isFinite(multiplier) || multiplier <= 0d) {
			log.warn("balanced-sync tail: invalid multiplier {}; keeping paired segment", multiplier);
			return;
		}

		log.info(
				"balanced-sync tail: stretching last TALK idx={}: targetMs {}→{}, srcDurMs={}, setptsMultiplier={}",
				lastTalkIdx,
				lastTalk.targetDurationMs(),
				requiredFinalChunkMs,
				srcDurationMs,
				multiplier);
		if (multiplier > MULTIPLIER_WARN_EXTREME) {
			log.warn(
					"balanced-sync tail: extreme slowdown (multiplier {} > {}); preview may look very slow-motion",
					multiplier,
					MULTIPLIER_WARN_EXTREME);
		}

		out.set(
				lastTalkIdx,
				new SegmentPlan(
						lastTalk.index(),
						lastTalk.kind(),
						lastTalk.srcStartMs(),
						lastTalk.srcEndMs(),
						requiredFinalChunkMs,
						multiplier));
	}

	private static long lastEndMs(List<SrtParser.Cue> cues) {
		if (cues == null || cues.isEmpty()) return 0;
		long max = 0;
		for (SrtParser.Cue c : cues) {
			if (c == null) continue;
			max = Math.max(max, c.endMs());
		}
		return max;
	}

}
