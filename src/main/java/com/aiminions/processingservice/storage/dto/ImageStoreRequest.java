package com.aiminions.processingservice.storage.dto;

public record ImageStoreRequest(
		byte[] imageBytes,
		String keyHint
) {
}
