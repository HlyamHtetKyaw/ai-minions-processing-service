package com.aiminions.processingservice.jobs.workspaceexport;

import com.aiminions.processingservice.integration.MainServiceWorkerClient;
import com.aiminions.processingservice.jobs.transcribe.GenerationStatusPublisher;
import com.aiminions.processingservice.storage.WorkspaceExportService;
import com.aiminions.processingservice.storage.dto.WorkspaceExportResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class WorkspaceExportPipeline {
    private final WorkspaceExportService workspaceExportService;
    private final ObjectMapper objectMapper;
    private final GenerationStatusPublisher generationStatusPublisher;
    private final MainServiceWorkerClient mainServiceWorkerClient;

    public void run(WorkspaceExportJobMessage msg) {
        long jobId = msg.jobId() != null ? msg.jobId() : msg.aiGenerationId();
        try {
            generationStatusPublisher.publishProcessing(jobId, "workspace_export_started");
            JsonNode payload = objectMapper.readTree(msg.payloadJson());
            generationStatusPublisher.publishProcessing(jobId, "workspace_export_encoding");
            WorkspaceExportResponse res = workspaceExportService.exportVideo(msg.userId(), payload);
            ObjectNode output = objectMapper.createObjectNode();
            output.put("type", "workspace_export");
            ObjectNode result = output.putObject("result");
            result.put("storageUrl", res.storageUrl());
            result.put("readUrl", res.readUrl());
            result.put("s3Key", res.key());
            generationStatusPublisher.publishProcessing(jobId, "workspace_export_uploading");
            String outputJson = objectMapper.writeValueAsString(output);
            generationStatusPublisher.publishCompleted(jobId, outputJson);
            mainServiceWorkerClient.notifyCompletion(jobId, "completed", null, outputJson);
        } catch (Exception ex) {
            String err = ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
            generationStatusPublisher.publishFailed(jobId, err);
            try {
                mainServiceWorkerClient.notifyCompletion(jobId, "failed", err, null);
            } catch (Exception notifyEx) {
                log.error("Failed callback for workspace export {}", jobId, notifyEx);
            }
            throw new IllegalStateException(ex);
        }
    }
}
