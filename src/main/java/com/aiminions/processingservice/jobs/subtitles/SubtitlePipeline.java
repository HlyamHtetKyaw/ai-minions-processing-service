package com.aiminions.processingservice.jobs.subtitles;

import com.aiminions.processingservice.config.ProcessingProperties;
import com.aiminions.processingservice.integration.AiServiceSubtitlesClient;
import com.aiminions.processingservice.integration.MainServiceWorkerClient;
import com.aiminions.processingservice.jobs.transcribe.GenerationStatusPublisher;
import com.aiminions.processingservice.media.ffmpeg.FfmpegRunner;
import com.aiminions.processingservice.storage.ObjectStorageTransferService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

@Service
@RequiredArgsConstructor
@Slf4j
public class SubtitlePipeline {

	private static final long CHUNK_SECONDS = 25;
	private static final String DEFAULT_TARGET_LANGUAGE = "my";
	private static final String DEFAULT_STYLE = "caption_rules_v1";

	private final ObjectStorageTransferService objectStorageTransferService;
	private final FfmpegRunner ffmpegRunner;
	private final ProcessingProperties processingProperties;
	private final GenerationStatusPublisher generationStatusPublisher;
	private final MainServiceWorkerClient mainServiceWorkerClient;
	private final AiServiceSubtitlesClient aiServiceSubtitlesClient;
	private final ObjectMapper objectMapper;

	public void run(SubtitleJobMessage msg) {
		long jobId = msg.jobId() != null ? msg.jobId() : msg.aiGenerationId();
		Path workDir = null;
		try {
			workDir = Files.createTempDirectory("subtitles-" + jobId + "-");

			generationStatusPublisher.publishProcessing(jobId, "download");
			Path input = objectStorageTransferService.download(msg.storageUrl(), workDir);
			BigDecimal inputMb = bytesToMbSafe(input);

			String sourceType = msg.sourceType() == null ? "audio" : msg.sourceType().trim().toLowerCase(Locale.ROOT);
			Path normalized = workDir.resolve("normalized.wav");

			if ("video".equals(sourceType)) {
				generationStatusPublisher.publishProcessing(jobId, "extract_audio");
				ffmpegRunner.run(List.of(
						"-y",
						"-i",
						input.toString(),
						"-vn",
						"-acodec",
						"pcm_s16le",
						"-ar",
						"44100",
						"-ac",
						"1",
						normalized.toString()));
			} else {
				generationStatusPublisher.publishProcessing(jobId, "normalize_audio");
				ffmpegRunner.run(List.of(
						"-y",
						"-i",
						input.toString(),
						"-acodec",
						"pcm_s16le",
						"-ar",
						"44100",
						"-ac",
						"1",
						normalized.toString()));
			}

			generationStatusPublisher.publishProcessing(jobId, "segment_audio");
			Path chunksDir = workDir.resolve("chunks");
			Files.createDirectories(chunksDir);
			Path chunkPattern = chunksDir.resolve("chunk-%03d.wav");
			ffmpegRunner.run(List.of(
					"-y",
					"-i",
					normalized.toString(),
					"-f",
					"segment",
					"-segment_time",
					String.valueOf(CHUNK_SECONDS),
					"-reset_timestamps",
					"1",
					"-acodec",
					"pcm_s16le",
					"-ar",
					"44100",
					"-ac",
					"1",
					chunkPattern.toString()));

			List<Path> chunks = Files.list(chunksDir)
					.filter(p -> p.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".wav"))
					.sorted(Comparator.comparing(p -> p.getFileName().toString()))
					.toList();
			if (chunks.isEmpty()) {
				throw new IllegalStateException("No audio chunks were created");
			}

			String targetLanguage = normalizeOrDefault(msg.targetLanguage(), DEFAULT_TARGET_LANGUAGE);
			String style = normalizeStyleProfile(normalizeOrDefault(msg.style(), DEFAULT_STYLE));

			List<SrtFormatter.Cue> allCues = new ArrayList<>();
			for (int i = 0; i < chunks.size(); i++) {
				generationStatusPublisher.publishProcessing(jobId, "ai_subtitles");
				Path chunk = chunks.get(i);
				byte[] bytes = Files.readAllBytes(chunk);
				long offsetMs = (long) i * CHUNK_SECONDS * 1000L;
				long durationMs = CHUNK_SECONDS * 1000L;
				JsonNode aiData = aiServiceSubtitlesClient.requestSubtitleCuesWithAudio(
						jobId,
						bytes,
						offsetMs,
						durationMs,
						i,
						targetLanguage,
						style);
				ArrayNode cues = (ArrayNode) aiData.path("result").path("cues");
				if (cues == null || cues.isMissingNode() || !cues.isArray()) {
					continue;
				}
				for (JsonNode c : cues) {
					long startMs = c.path("startMs").asLong(-1);
					long endMs = c.path("endMs").asLong(-1);
					String text = c.path("text").asText("");
					if (startMs < 0 || endMs < 0) continue;
					allCues.add(new SrtFormatter.Cue(startMs + offsetMs, endMs + offsetMs, text));
				}
			}

			List<SrtFormatter.Cue> merged = mergeCues(allCues);
			String srt = SrtFormatter.toSrt(merged);
			if (srt.isBlank()) {
				throw new IllegalStateException("No subtitle cues were generated");
			}

			generationStatusPublisher.publishProcessing(jobId, "upload_srt");
			Path out = workDir.resolve("subtitles.srt");
			Files.writeString(out, srt);

			String keyHint = "subtitles/" + msg.userId() + "/outputs/" + jobId + ".srt";
			ObjectStorageTransferService.StoredObject stored = objectStorageTransferService.uploadFile(
					out,
					keyHint,
					"application/x-subrip");

			ObjectNode outputDataNode = objectMapper.createObjectNode();
			outputDataNode.put("type", "subtitles");
			outputDataNode.put("language", targetLanguage);
			outputDataNode.put("style", style);
			outputDataNode.put("srtKey", stored.key());
			outputDataNode.put("srtStorageUrl", stored.storageUrl());
			outputDataNode.put("cues", merged.size());

			String outputData = objectMapper.writeValueAsString(outputDataNode);
			generationStatusPublisher.publishCompleted(jobId, outputData);

			BigDecimal mbAudio = "video".equals(sourceType) ? null : inputMb;
			BigDecimal mbVideo = "video".equals(sourceType) ? inputMb : null;
			mainServiceWorkerClient.notifyCompletion(jobId, "completed", null, outputData, null, null, mbAudio, mbVideo);

			log.info("Subtitle job {} completed (cues={}, key={})", jobId, merged.size(), stored.key());
		} catch (JsonProcessingException e) {
			log.error("Subtitle job {} failed (JSON)", jobId, e);
			failJob(jobId, "AI response parse error: " + e.getMessage());
		} catch (Exception e) {
			log.error("Subtitle job {} failed", jobId, e);
			String err = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
			failJob(jobId, err);
		} finally {
			if (workDir != null) {
				try {
					FileSystemUtils.deleteRecursively(workDir);
				} catch (Exception e) {
					log.warn("Could not delete work dir {}: {}", workDir, e.getMessage());
				}
			}
		}
	}

	private void failJob(long jobId, String err) {
		try {
			mainServiceWorkerClient.notifyCompletion(jobId, "failed", err, null);
		} catch (Exception ex) {
			log.error("Could not report failure to main service for job {}", jobId, ex);
		}
		generationStatusPublisher.publishFailed(jobId, err);
	}

	private static List<SrtFormatter.Cue> mergeCues(List<SrtFormatter.Cue> in) {
		if (in == null || in.isEmpty()) return List.of();
		List<SrtFormatter.Cue> items = in.stream()
				.filter(c -> c != null && c.text() != null && !c.text().trim().isBlank())
				.sorted(Comparator.comparingLong(SrtFormatter.Cue::startMs).thenComparingLong(SrtFormatter.Cue::endMs))
				.toList();

		List<SrtFormatter.Cue> out = new ArrayList<>(items.size());
		long lastEnd = -1;
		String lastText = null;
		for (SrtFormatter.Cue c : items) {
			long start = Math.max(0, c.startMs());
			long end = Math.max(start + 300, c.endMs());
			String text = c.text().trim();
			if (lastText != null && text.equals(lastText) && Math.abs(start - lastEnd) <= 300) {
				// likely overlap duplicate from chunk boundary
				continue;
			}
			if (lastEnd >= 0 && start < lastEnd) {
				start = lastEnd;
				if (end <= start) {
					end = start + 500;
				}
			}
			out.add(new SrtFormatter.Cue(start, end, text));
			lastEnd = end;
			lastText = text;
		}
		return out;
	}

	private static BigDecimal bytesToMbSafe(Path file) {
		try {
			long bytes = Files.size(file);
			if (bytes <= 0) return BigDecimal.ZERO;
			return BigDecimal.valueOf(bytes)
					.divide(BigDecimal.valueOf(1024L * 1024L), 6, RoundingMode.HALF_UP);
		} catch (Exception e) {
			return null;
		}
	}

	private static String normalizeOrDefault(String value, String def) {
		String v = value == null ? "" : value.trim();
		return v.isBlank() ? def : v;
	}

	private static String normalizeStyleProfile(String style) {
		if (style == null) {
			return DEFAULT_STYLE;
		}
		String s = style.trim();
		if (s.isBlank()) {
			return DEFAULT_STYLE;
		}
		if ("toon_caption_rules".equalsIgnoreCase(s)) {
			return "caption_rules_v1";
		}
		return s;
	}
}

