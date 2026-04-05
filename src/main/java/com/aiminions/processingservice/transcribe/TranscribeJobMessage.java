package com.aiminions.processingservice.transcribe;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Mirrors main-service {@code TranscribeJobMessage} JSON published to {@code ai-minions:jobs:transcribe}.
 */
public record TranscribeJobMessage(
		@JsonProperty("jobId") Long jobId,
		@JsonProperty("userId") Long userId,
		@JsonProperty("aiGenerationId") Long aiGenerationId,
		@JsonProperty("storageUrl") String storageUrl,
		@JsonProperty("s3Key") String s3Key,
		@JsonProperty("sourceType") String sourceType) {
}
