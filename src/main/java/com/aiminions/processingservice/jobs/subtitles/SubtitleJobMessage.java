package com.aiminions.processingservice.jobs.subtitles;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SubtitleJobMessage(
		@JsonProperty("jobId") Long jobId,
		@JsonProperty("userId") Long userId,
		@JsonProperty("aiGenerationId") Long aiGenerationId,
		@JsonProperty("storageUrl") String storageUrl,
		@JsonProperty("s3Key") String s3Key,
		@JsonProperty("sourceType") String sourceType,
		@JsonProperty("targetLanguage") String targetLanguage,
		@JsonProperty("style") String style,
		@JsonProperty("translatedText") String translatedText,
		@JsonProperty("userGeminiApiKey") String userGeminiApiKey,
		@JsonProperty("voiceOverStorageUrl") String voiceOverStorageUrl,
		@JsonProperty("voiceOverPlaybackRate") Double voiceOverPlaybackRate) {
}

