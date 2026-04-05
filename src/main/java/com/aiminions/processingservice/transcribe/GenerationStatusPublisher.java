package com.aiminions.processingservice.transcribe;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.aiminions.processingservice.config.ProcessingProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class GenerationStatusPublisher {

	private final StringRedisTemplate redis;
	private final ProcessingProperties processingProperties;
	private final ObjectMapper objectMapper;

	public void publishRaw(long jobId, String json) {
		try {
			redis.convertAndSend(processingProperties.generationStatusChannel(jobId), json);
		} catch (Exception e) {
			log.error(
					"Failed to publish generation status to Redis channel {} for job {} — SSE on main-service may hang until timeout. Cause: {}",
					processingProperties.generationStatusChannel(jobId),
					jobId,
					e.toString());
		}
	}

	public void publishProcessing(long jobId, String stage) {
		try {
			ObjectNode n = objectMapper.createObjectNode();
			n.put("status", "processing");
			n.put("stage", stage);
			n.put("jobId", jobId);
			n.put("generationId", jobId);
			publishRaw(jobId, objectMapper.writeValueAsString(n));
		} catch (JsonProcessingException e) {
			log.warn("Failed to serialize processing status", e);
		}
	}

	public void publishCompleted(long jobId, String outputDataJson) {
		try {
			ObjectNode n = objectMapper.createObjectNode();
			n.put("status", "completed");
			n.put("jobId", jobId);
			n.put("generationId", jobId);
			if (outputDataJson != null && !outputDataJson.isBlank()) {
				n.set("outputData", objectMapper.readTree(outputDataJson));
			}
			publishRaw(jobId, objectMapper.writeValueAsString(n));
		} catch (JsonProcessingException e) {
			log.warn("Failed to serialize completed status", e);
		}
	}

	public void publishFailed(long jobId, String message) {
		try {
			ObjectNode n = objectMapper.createObjectNode();
			n.put("status", "failed");
			n.put("jobId", jobId);
			n.put("generationId", jobId);
			if (message != null) {
				n.put("message", message);
			}
			publishRaw(jobId, objectMapper.writeValueAsString(n));
		} catch (JsonProcessingException e) {
			log.warn("Failed to serialize failed status", e);
		}
	}
}
