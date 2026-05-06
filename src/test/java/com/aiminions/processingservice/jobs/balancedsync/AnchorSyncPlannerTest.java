package com.aiminions.processingservice.jobs.balancedsync;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AnchorSyncPlannerTest {

	@Test
	void chunksWithGapOver1500msCreateTwoTalkingSegmentsAndGap() {
		List<SrtParser.Cue> original = List.of(
				new SrtParser.Cue(0, 1000, "a"),
				new SrtParser.Cue(3000, 4000, "b")
		);
		List<SrtParser.Cue> voice = List.of(
				new SrtParser.Cue(0, 2000, "va"),
				new SrtParser.Cue(4000, 6000, "vb")
		);

		/* prior before last talk: 2000 + 2000 gap = 4000; true voice <= 5899 + buffer keeps last talk paired at 2000 ms */
		var plan = AnchorSyncPlanner.build(original, voice, 1500, 5899L, 0L);
		assertEquals(2, plan.originalChunkCount());
		assertEquals(2, plan.voiceChunkCount());
		assertEquals(3, plan.segments().size(), "talk1 + gap1 + talk2");
		assertEquals(AnchorSyncPlanner.SegmentKind.TALK, plan.segments().get(0).kind());
		assertEquals(AnchorSyncPlanner.SegmentKind.GAP, plan.segments().get(1).kind());
		assertEquals(AnchorSyncPlanner.SegmentKind.TALK, plan.segments().get(2).kind());
		assertEquals(2000L, plan.segments().get(2).targetDurationMs());
	}

	@Test
	void trueVoiceLongerThanSrtTimelineStretchesLastTalkSegmentInsteadOfHoldTail() {
		List<SrtParser.Cue> original = List.of(
				new SrtParser.Cue(0, 1000, "a")
		);
		List<SrtParser.Cue> voice = List.of(
				new SrtParser.Cue(0, 1000, "va"),
				new SrtParser.Cue(3000, 4000, "vb")
		);
		long trueVoiceMs = 5000L;
		var plan = AnchorSyncPlanner.build(original, voice, 1500, trueVoiceMs, 0L);
		assertEquals(1, plan.segments().size());
		var seg = plan.segments().get(0);
		assertEquals(AnchorSyncPlanner.SegmentKind.TALK, seg.kind());
		long required = trueVoiceMs - 0L + 100L;
		assertEquals(required, seg.targetDurationMs());
		assertEquals((double) required / 1000d, seg.setptsMultiplier(), 1e-6);
	}

	@Test
	void shortTrueVoiceLeavesPairedMultiplierUnchanged() {
		List<SrtParser.Cue> original = List.of(
				new SrtParser.Cue(0, 1000, "a"),
				new SrtParser.Cue(4000, 5000, "b")
		);
		List<SrtParser.Cue> voice = List.of(
				new SrtParser.Cue(0, 1000, "va")
		);
		long trueVoiceMs = 890L;
		var plan = AnchorSyncPlanner.build(original, voice, 1500, trueVoiceMs, 0L);
		assertEquals(2, plan.originalChunkCount());
		assertEquals(1, plan.voiceChunkCount());
		assertEquals(1, plan.segments().size());
		assertEquals(1000L, plan.segments().get(0).targetDurationMs());
		assertEquals(1.0d, plan.segments().get(0).setptsMultiplier(), 1e-9);
	}

	@Test
	void leadPadMsIncludedInPriorSoTailRequirementAccountsForStartupPad() {
		List<SrtParser.Cue> original = List.of(new SrtParser.Cue(0, 5000, "a"));
		List<SrtParser.Cue> voice = List.of(new SrtParser.Cue(2000, 7000, "va"));
		long leadPadMs = 2000L;
		long pairedTargetMs = 5000L;
		long trueVoiceMs = pairedTargetMs + leadPadMs + 500L;
		var plan = AnchorSyncPlanner.build(original, voice, 1500, trueVoiceMs, leadPadMs);
		assertEquals(1, plan.segments().size());
		long expectedRequiredMs = trueVoiceMs - leadPadMs + 100L;
		assertEquals(expectedRequiredMs, plan.segments().get(0).targetDurationMs());
	}
}
