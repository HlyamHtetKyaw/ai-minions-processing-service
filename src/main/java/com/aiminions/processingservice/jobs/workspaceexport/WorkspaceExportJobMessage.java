package com.aiminions.processingservice.jobs.workspaceexport;

import com.fasterxml.jackson.annotation.JsonProperty;

public record WorkspaceExportJobMessage(
        @JsonProperty("jobId") Long jobId,
        @JsonProperty("userId") Long userId,
        @JsonProperty("aiGenerationId") Long aiGenerationId,
        @JsonProperty("payloadJson") String payloadJson
) {
}
