package com.aiminions.processingservice.jobs.balancedsync;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BalancedSyncPipelineTest {

	@Test
	void needsUniformPrepass_falseWhenDurationsInvalid() {
		assertFalse(BalancedSyncPipeline.needsUniformPrepass(0, 10_000));
		assertFalse(BalancedSyncPipeline.needsUniformPrepass(10_000, 0));
	}

	@Test
	void needsUniformPrepass_falseInsideNeutralBand() {
		assertFalse(BalancedSyncPipeline.needsUniformPrepass(10_000, 10_000));
		assertFalse(BalancedSyncPipeline.needsUniformPrepass(10_000, 13_400));
		assertFalse(BalancedSyncPipeline.needsUniformPrepass(10_000, 7_400));
	}

	@Test
	void needsUniformPrepass_trueWhenVoiceMuchLonger() {
		assertTrue(BalancedSyncPipeline.needsUniformPrepass(10_000, 13_501));
	}

	@Test
	void needsUniformPrepass_trueWhenVoiceMuchShorter() {
		assertTrue(BalancedSyncPipeline.needsUniformPrepass(10_000, 7_399));
	}
}
