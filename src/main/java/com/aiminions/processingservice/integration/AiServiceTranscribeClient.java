package com.aiminions.processingservice.integration;

import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import com.aiminions.processingservice.config.ProcessingProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * HTTP client for AI service transcribe (multipart). Uses AI service route
 * {@code /ai_service/api/v1/feature/generate}; {@code featureType} in the JSON body is the
 * AI-service operation discriminator, not a database table.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class AiServiceTranscribeClient {

	private static final String GENERATE_PATH = "/ai_service/api/v1/feature/generate";

	private final ProcessingProperties processingProperties;
	private final ObjectMapper objectMapper;

	private RestClient restClient;

	@PostConstruct
	void init() {
		this.restClient = RestClient.builder()
				.baseUrl(processingProperties.getAiServiceBaseUrl().trim())
				.build();
	}

	public JsonNode requestTranscriptionWithAudio(long generationId, byte[] audioWav) throws JsonProcessingException {
		log.info(
				"[transcribe][ai-client][request] generationId={} aiBaseUrl={} audioBytes={} path={}",
				generationId,
				processingProperties.getAiServiceBaseUrl(),
				audioWav != null ? audioWav.length : 0,
				GENERATE_PATH);
		ObjectNode payload = objectMapper.createObjectNode();
		payload.put("operation", "transcribe");
		payload.put("generationId", generationId);

		ObjectNode requestRoot = objectMapper.createObjectNode();
		requestRoot.put("featureType", "TRANSCRIBE");
		requestRoot.put("provider", "GEMINI");
		requestRoot.set("payload", payload);
		String requestJson = objectMapper.writeValueAsString(requestRoot);

		MultipartBodyBuilder mb = new MultipartBodyBuilder();
		HttpHeaders jsonHeaders = new HttpHeaders();
		jsonHeaders.setContentType(MediaType.APPLICATION_JSON);
		mb.part("request", new HttpEntity<>(requestJson, jsonHeaders));
		mb.part("audio", new ByteArrayResource(audioWav) {
			@Override
			public String getFilename() {
				return "cleaned.wav";
			}
		}, MediaType.parseMediaType("audio/wav"));

		String raw = restClient.post()
				.uri(GENERATE_PATH)
				.contentType(MediaType.MULTIPART_FORM_DATA)
				.body(mb.build())
				.retrieve()
				.body(String.class);
		log.debug(
				"[transcribe][ai-client][raw-response] generationId={} responseBytes={}",
				generationId,
				raw != null ? raw.length() : 0);
		if (raw == null || raw.isBlank()) {
			throw new IllegalStateException("Empty response from AI service");
		}
		JsonNode root = objectMapper.readTree(raw);
		if (root.path("success").asInt() != 1) {
			log.error(
					"[transcribe][ai-client][error] generationId={} code={} message={}",
					generationId,
					root.path("code").asInt(),
					root.path("message").asText("unknown"));
			throw new IllegalStateException(
					"AI service error: " + root.path("message").asText("unknown") + " (code=" + root.path("code").asInt() + ")");
		}
		JsonNode data = root.get("data");
		if (data == null || data.isNull()) {
			throw new IllegalStateException("AI service response missing data");
		}
		log.info(
				"[transcribe][ai-client][ok] generationId={} usedProvider={} featureType={}",
				generationId,
				data.path("usedProvider").asText(""),
				data.path("featureType").asText(""));
		return data;
	}
}
