package com.aiminions.processingservice.jobs.transcribe;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TranscribeJobMessage(
		@JsonProperty("jobId") Long jobId,
		@JsonProperty("userId") Long userId,
		@JsonProperty("aiGenerationId") Long aiGenerationId,
		@JsonProperty("storageUrl") String storageUrl,
		@JsonProperty("s3Key") String s3Key,
		@JsonProperty("sourceType") String sourceType) {
}
