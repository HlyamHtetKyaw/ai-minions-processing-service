package com.aiminions.processingservice.storage.dto;

import com.fasterxml.jackson.databind.JsonNode;

public record WorkspaceExportRequest(
		Long userId,
		JsonNode payload
) {
}
