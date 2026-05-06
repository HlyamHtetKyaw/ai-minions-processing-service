package com.aiminions.processingservice.jobs.balancedsync;

import java.util.ArrayList;
import java.util.List;

import com.aiminions.processingservice.jobs.balancedsync.SilenceAnchorChunker.SceneChunk;

public final class AnchorSyncPlanner {

	private AnchorSyncPlanner() {}

	public enum SegmentKind { TALK, GAP }

	public record SegmentPlan(
			int index,
			SegmentKind kind,
			long srcStartMs,
			long srcEndMs,
			long targetDurationMs,
			double setptsMultiplier,
			long holdTailMs
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
	 * - Chunk by anchor gaps (> anchorGapMs) in both SRTs.\n
	 * - Match chunks sequentially.\n
	 * - Talking chunks: retime video with setpts multiplier = voiceDur/originalDur.\n
	 * - Anchor gaps: keep video at 1.0x (no setpts), but re-encode for concat stability.\n
	 * - If Voice has more chunks: hold last frame (tpad) for remaining audio time.\n
	 * - If Original has more chunks: drop remaining video.\n
	 */
	public static PlanResult build(
			List<SrtParser.Cue> originalCues,
			List<SrtParser.Cue> voiceCues,
			long anchorGapMs
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
					mul,
					0
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
							1.0d,
							0
					));
				}
			}
		}

		long voiceEndMs = lastEndMs(voiceCues);
		long originalEndMs = lastEndMs(originalCues);
		long plannedVideoMs = estimatePlannedDurationMs(out);

		// If voice is longer than the planned video, hold last frame (tail pad) on final segment.
		long extra = Math.max(0, voiceEndMs - plannedVideoMs);
		if (extra > 0 && !out.isEmpty()) {
			SegmentPlan last = out.get(out.size() - 1);
			out.set(out.size() - 1, new SegmentPlan(
					last.index(),
					last.kind(),
					last.srcStartMs(),
					last.srcEndMs(),
					last.targetDurationMs(),
					last.setptsMultiplier(),
					extra
			));
		}

		return new PlanResult(out, oChunks.size(), vChunks.size(), originalEndMs, voiceEndMs);
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

	private static long estimatePlannedDurationMs(List<SegmentPlan> plans) {
		long sum = 0;
		for (SegmentPlan p : plans) {
			if (p == null) continue;
			long dur = p.kind() == SegmentKind.TALK ? Math.max(0, p.targetDurationMs()) : Math.max(0, p.targetDurationMs());
			sum += dur;
			sum += Math.max(0, p.holdTailMs());
		}
		return sum;
	}
}

