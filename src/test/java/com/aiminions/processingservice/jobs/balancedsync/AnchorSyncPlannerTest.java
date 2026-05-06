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

		var plan = AnchorSyncPlanner.build(original, voice, 1500);
		assertEquals(2, plan.originalChunkCount());
		assertEquals(2, plan.voiceChunkCount());
		assertEquals(3, plan.segments().size(), "talk1 + gap1 + talk2");
		assertEquals(AnchorSyncPlanner.SegmentKind.TALK, plan.segments().get(0).kind());
		assertEquals(AnchorSyncPlanner.SegmentKind.GAP, plan.segments().get(1).kind());
		assertEquals(AnchorSyncPlanner.SegmentKind.TALK, plan.segments().get(2).kind());
	}

	@Test
	void voiceLongerThanPlannedVideoAddsHoldTailOnLastSegment() {
		List<SrtParser.Cue> original = List.of(
				new SrtParser.Cue(0, 1000, "a")
		);
		List<SrtParser.Cue> voice = List.of(
				new SrtParser.Cue(0, 1000, "va"),
				// Second voice chunk has no corresponding original chunk -> should trigger tail hold.
				new SrtParser.Cue(3000, 4000, "vb")
		);
		var plan = AnchorSyncPlanner.build(original, voice, 1500);
		assertEquals(1, plan.segments().size());
		assertTrue(plan.segments().get(0).holdTailMs() >= 0);
		// When voice end > planned, holdTail should be positive.
		assertTrue(plan.segments().get(0).holdTailMs() > 0);
	}

	@Test
	void originalHasMoreChunksThanVoiceStopsAtCommonCount() {
		List<SrtParser.Cue> original = List.of(
				new SrtParser.Cue(0, 1000, "a"),
				new SrtParser.Cue(4000, 5000, "b")
		);
		List<SrtParser.Cue> voice = List.of(
				new SrtParser.Cue(0, 1000, "va")
		);
		var plan = AnchorSyncPlanner.build(original, voice, 1500);
		assertEquals(2, plan.originalChunkCount());
		assertEquals(1, plan.voiceChunkCount());
		// Only common chunk 1 is planned.
		assertEquals(1, plan.segments().size());
	}
}

