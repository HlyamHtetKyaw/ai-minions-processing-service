package com.aiminions.processingservice.jobs.transcribe;

import java.time.Duration;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.aiminions.processingservice.config.ProcessingProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class GenerationStatusPublisher {

	private static final Duration PROGRESS_SNAPSHOT_TTL = Duration.ofHours(6);

	private final StringRedisTemplate redis;
	private final ProcessingProperties processingProperties;
	private final ObjectMapper objectMapper;

	public void publishRaw(long jobId, String json) {
		try {
			redis.convertAndSend(processingProperties.generationStatusChannel(jobId), json);
			syncProgressSnapshotKey(jobId, json);
		} catch (Exception e) {
			log.error(
					"Failed to publish generation status to Redis channel {} for job {} — SSE on main-service may hang until timeout. Cause: {}",
					processingProperties.generationStatusChannel(jobId),
					jobId,
					e.toString());
		}
	}

	/**
	 * Pub/sub is fire-and-forget; SSE clients that connect late never see earlier stages. Mirror the latest
	 * {@code processing} payload under a key so main-service can emit it once on subscribe.
	 */
	private void syncProgressSnapshotKey(long jobId, String json) {
		try {
			String key = processingProperties.generationProgressLastKey(jobId);
			JsonNode root = objectMapper.readTree(json);
			String st = root.path("status").asText("").toLowerCase();
			if ("processing".equals(st)) {
				redis.opsForValue().set(key, json, PROGRESS_SNAPSHOT_TTL);
			} else if ("completed".equals(st) || "failed".equals(st) || "error".equals(st)) {
				redis.delete(key);
			}
		} catch (Exception e) {
			log.debug("Could not sync generation progress snapshot key for job {}", jobId, e);
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
