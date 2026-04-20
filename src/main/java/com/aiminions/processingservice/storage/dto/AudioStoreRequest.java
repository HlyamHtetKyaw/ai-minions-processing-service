package com.aiminions.processingservice.storage.dto;

public record AudioStoreRequest(
		byte[] audioBytes,
		String keyHint,
		String contentType
) {
}
