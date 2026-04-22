package com.aiminions.processingservice.integration;

import com.aiminions.processingservice.config.ProcessingProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * HTTP client for AI service subtitles (multipart). Uses AI service route
 * {@code /ai_service/api/v1/feature/generate}.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class AiServiceSubtitlesClient {

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

	public JsonNode requestSubtitleCuesWithAudio(
			long generationId,
			byte[] audioWav,
			long chunkOffsetMs,
			long chunkDurationMs,
			int chunkIndex,
			String targetLanguage,
			String style
	) throws JsonProcessingException {
		ObjectNode payload = objectMapper.createObjectNode();
		payload.put("operation", "subtitles_srt_cues");
		payload.put("generationId", generationId);
		payload.put("chunkOffsetMs", chunkOffsetMs);
		payload.put("chunkDurationMs", chunkDurationMs);
		payload.put("chunkIndex", chunkIndex);
		payload.put("targetLanguage", targetLanguage == null ? "my" : targetLanguage.trim());
		payload.put("styleProfile", normalizeStyleProfile(style));

		ObjectNode requestRoot = objectMapper.createObjectNode();
		requestRoot.put("featureType", "SUBTITLES");
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
				return "chunk-" + chunkIndex + ".wav";
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

	private static String normalizeStyleProfile(String style) {
		String s = style == null ? "" : style.trim();
		if (s.isBlank()) {
			return "caption_rules_v1";
		}
		if ("toon_caption_rules".equalsIgnoreCase(s)) {
			return "caption_rules_v1";
		}
		return s;
	}
}

