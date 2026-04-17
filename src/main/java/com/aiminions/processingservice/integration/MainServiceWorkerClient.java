package com.aiminions.processingservice.integration;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import com.aiminions.processingservice.config.ProcessingProperties;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class MainServiceWorkerClient {

	/** Must match main-service {@code WorkerGenerationController#WORKER_TOKEN_HEADER}. */
	private static final String WORKER_TOKEN_HEADER = "X-Worker-Token";

	private final ProcessingProperties processingProperties;
	private RestClient restClient;

	@PostConstruct
	void init() {
		this.restClient = RestClient.builder()
				.baseUrl(processingProperties.getMainServiceBaseUrl().trim())
				.build();
		String token = processingProperties.getWorkerToken();
		if (token == null || token.isBlank()) {
			log.error(
					"app.processing.worker-token / WORKER_INTERNAL_TOKEN is not set. "
							+ "Main-service DB will never move from PENDING to FAILED/SUCCESS for worker jobs. "
							+ "Spring Boot does not load a .env file unless you export variables or use a tool that does — "
							+ "set the same token as main-service app.worker.token (e.g. export WORKER_INTERNAL_TOKEN=...).");
		}
	}

	public void notifyCompletion(long generationId, String status, String message, String outputData) {
		notifyCompletion(generationId, status, message, outputData, null, null, null, null);
	}

	public void notifyCompletion(
			long generationId,
			String status,
			String message,
			String outputData,
			BigDecimal tokenIn,
			BigDecimal tokenOut,
			BigDecimal mbAudio,
			BigDecimal mbVideo) {
		String token = processingProperties.getWorkerToken();
		if (token == null || token.isBlank()) {
			log.warn("app.processing.worker-token is not set; skipping completion callback for generation {}", generationId);
			return;
		}
		Map<String, Object> body = new LinkedHashMap<>();
		body.put("status", status);
		if (message != null && !message.isBlank()) {
			body.put("message", message);
		}
		if (outputData != null && !outputData.isBlank()) {
			body.put("outputData", outputData);
		}
		if (tokenIn != null) body.put("tokenIn", normalize(tokenIn));
		if (tokenOut != null) body.put("tokenOut", normalize(tokenOut));
		if (mbAudio != null) body.put("mbAudio", normalize(mbAudio));
		if (mbVideo != null) body.put("mbVideo", normalize(mbVideo));
		try {
			restClient.post()
					.uri("/api/v1/internal/worker/generations/{id}/completion", generationId)
					.header(WORKER_TOKEN_HEADER, token.trim())
					.contentType(MediaType.APPLICATION_JSON)
					.body(body)
					.retrieve()
					.toBodilessEntity();
		} catch (Exception e) {
			log.error("Main-service completion callback failed for generation {}", generationId, e);
			throw e;
		}
	}

	private static BigDecimal normalize(BigDecimal n) {
		if (n == null) return null;
		if (n.signum() < 0) return BigDecimal.ZERO;
		return n.setScale(6, RoundingMode.HALF_UP);
	}
}
