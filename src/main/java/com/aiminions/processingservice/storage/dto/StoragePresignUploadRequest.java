package com.aiminions.processingservice.storage.dto;

public record StoragePresignUploadRequest(
		String keyHint,
		String contentType
) {
}
