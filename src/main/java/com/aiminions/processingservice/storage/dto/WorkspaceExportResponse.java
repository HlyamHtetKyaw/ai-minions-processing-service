package com.aiminions.processingservice.storage.dto;

public record WorkspaceExportResponse(
		String storageUrl,
		String readUrl,
		String key
) {
}
