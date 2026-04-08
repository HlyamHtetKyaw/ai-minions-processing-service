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

@Component
@RequiredArgsConstructor
@Slf4j
public class AiFeatureServiceClient {

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
		if (raw == null || raw.isBlank()) {
			throw new IllegalStateException("Empty response from AI service");
		}
		JsonNode root = objectMapper.readTree(raw);
		if (root.path("success").asInt() != 1) {
			throw new IllegalStateException(
					"AI service error: " + root.path("message").asText("unknown") + " (code=" + root.path("code").asInt() + ")");
		}
		JsonNode data = root.get("data");
		if (data == null || data.isNull()) {
			throw new IllegalStateException("AI service response missing data");
		}
		return data;
	}
}
