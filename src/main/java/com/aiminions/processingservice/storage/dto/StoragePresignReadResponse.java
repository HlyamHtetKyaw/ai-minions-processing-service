package com.aiminions.processingservice.storage.dto;

public record StoragePresignReadResponse(
		String readUrl,
		String key
) {
}
