package com.aiminions.processingservice.storage.dto;

public record StoragePresignUploadResponse(
		String uploadUrl,
		String readUrl,
		String key
) {
}
