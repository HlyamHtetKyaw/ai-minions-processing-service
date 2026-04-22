package com.aiminions.processingservice.jobs.balancedsync;

import com.fasterxml.jackson.annotation.JsonProperty;

public record BalancedSyncJobMessage(
		@JsonProperty("jobId") Long jobId,
		@JsonProperty("userId") Long userId,
		@JsonProperty("aiGenerationId") Long aiGenerationId,
		@JsonProperty("videoStorageUrl") String videoStorageUrl,
		@JsonProperty("videoS3Key") String videoS3Key,
		@JsonProperty("voiceStorageUrl") String voiceStorageUrl,
		@JsonProperty("voiceS3Key") String voiceS3Key,
		@JsonProperty("targetDurationSec") Double targetDurationSec,
		@JsonProperty("videoRate") Double videoRate,
		@JsonProperty("voiceRate") Double voiceRate,
		@JsonProperty("protectFlip") Boolean protectFlip,
		@JsonProperty("protectHueDeg") Double protectHueDeg
) {
}

