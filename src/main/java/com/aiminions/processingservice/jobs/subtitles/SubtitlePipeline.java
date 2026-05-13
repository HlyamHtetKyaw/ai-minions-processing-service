package com.aiminions.processingservice.jobs.subtitles;

import com.aiminions.processingservice.config.ProcessingProperties;
import com.aiminions.processingservice.integration.AiServiceSubtitlesClient;
import com.aiminions.processingservice.integration.MainServiceWorkerClient;
import com.aiminions.processingservice.jobs.balancedsync.SrtParser;
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
			long sourceDurationMs = ffmpegRunner.getMediaDurationMillis(input);

			String sourceType = msg.sourceType() == null ? "audio" : msg.sourceType().trim().toLowerCase(Locale.ROOT);
			Path normalized = workDir.resolve("normalized.wav");

			boolean viralVoiceSyncedTimeline = matchesSyncedVoiceSubtitles(sourceType, msg);
			boolean originalAudioPlaybackTempo = appliesOriginalAudioPlaybackTempo(sourceType, msg);
			if (viralVoiceSyncedTimeline) {
				generationStatusPublisher.publishProcessing(jobId, "extract_voice_synced_audio");
				Path voiceFile = objectStorageTransferService.download(msg.voiceOverStorageUrl(), workDir);
				long videoMs = sourceDurationMs;
				double videoDurSec = Math.max(0.04d, videoMs / 1000.0d);
				double playbackRate = msg.voiceOverPlaybackRate();
				if (!Double.isFinite(playbackRate)) {
					playbackRate = 1d;
				}
				playbackRate = Math.max(0.5d, Math.min(5d, playbackRate));
				String tempo = buildAtempoFilter(playbackRate);
				List<String> voiceNormalize = new ArrayList<>();
				voiceNormalize.add("-y");
				voiceNormalize.add("-i");
				voiceNormalize.add(voiceFile.toString());
				voiceNormalize.add("-af");
				voiceNormalize.add(tempo);
				voiceNormalize.add("-t");
				voiceNormalize.add(String.format(Locale.US, "%.4f", videoDurSec));
				voiceNormalize.add("-vn");
				voiceNormalize.add("-acodec");
				voiceNormalize.add("pcm_s16le");
				voiceNormalize.add("-ar");
				voiceNormalize.add("44100");
				voiceNormalize.add("-ac");
				voiceNormalize.add("1");
				voiceNormalize.add(normalized.toString());
				ffmpegRunner.run(voiceNormalize);
				log.info("Subtitle job {} using viral synced voice timeline (playbackRate={}, videoDurSec={})",
						jobId, playbackRate, videoDurSec);
			} else if ("video".equals(sourceType)) {
				double videoDurSec = Math.max(0.04d, sourceDurationMs / 1000.0d);
				if (originalAudioPlaybackTempo) {
					generationStatusPublisher.publishProcessing(jobId, "extract_original_audio_tempo");
					double playbackRate = clampPlaybackRate(msg.voiceOverPlaybackRate());
					String tempo = buildAtempoFilter(playbackRate);
					List<String> extractOriginal = new ArrayList<>();
					extractOriginal.add("-y");
					extractOriginal.add("-i");
					extractOriginal.add(input.toString());
					extractOriginal.add("-vn");
					extractOriginal.add("-af");
					extractOriginal.add(tempo);
					extractOriginal.add("-t");
					extractOriginal.add(String.format(Locale.US, "%.4f", videoDurSec));
					extractOriginal.add("-acodec");
					extractOriginal.add("pcm_s16le");
					extractOriginal.add("-ar");
					extractOriginal.add("44100");
					extractOriginal.add("-ac");
					extractOriginal.add("1");
					extractOriginal.add(normalized.toString());
					ffmpegRunner.run(extractOriginal);
					log.info(
							"Subtitle job {} normalized original audio with FE sync rate before AI (playbackRate={}, videoDurSec={})",
							jobId,
							playbackRate,
							videoDurSec);
				} else {
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
				}
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
						style,
						msg.userGeminiApiKey());
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

			List<SrtFormatter.Cue> merged = mergeCues(allCues, sourceDurationMs);

			String srt = SrtFormatter.toSrt(merged);
			String translatedText = msg.translatedText() == null ? "" : msg.translatedText().trim();
			if (!translatedText.isBlank() && !srt.isBlank()) {
				try {
					generationStatusPublisher.publishProcessing(jobId, "ai_refine_srt");
					JsonNode refinedData = aiServiceSubtitlesClient.requestRefinedSrt(
							jobId,
							srt,
							translatedText,
							targetLanguage,
							style,
							msg.userGeminiApiKey());
					String refinedSrt = refinedData.path("result").path("srtText").asText("").trim();
					if (!refinedSrt.isBlank()) {
						log.info("Subtitle job {} refined first-pass SRT with translated text ({} chars)",
								jobId, translatedText.length());
						srt = refinedSrt;
					} else {
						log.warn("Subtitle job {} received empty refined SRT, keeping first-pass output", jobId);
					}
				} catch (Exception refineEx) {
					log.warn("Subtitle job {} failed second-pass SRT refine, keeping first-pass output: {}",
							jobId, refineEx.getMessage());
				}
			}
			if (srt.isBlank()) {
				throw new IllegalStateException("No subtitle cues were generated");
			}

			List<SrtFormatter.Cue> publishedCues = resolveNonOverlappingTimeline(fromSrtParserCues(SrtParser.parse(srt)), sourceDurationMs);
			if (publishedCues.isEmpty()) {
				throw new IllegalStateException("No subtitle cues after timeline normalization");
			}
			srt = SrtFormatter.toSrt(publishedCues);

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
			outputDataNode.put("cues", publishedCues.size());

			String outputData = objectMapper.writeValueAsString(outputDataNode);
			generationStatusPublisher.publishCompleted(jobId, outputData);

			BigDecimal mbAudio = "video".equals(sourceType) ? null : inputMb;
			BigDecimal mbVideo = "video".equals(sourceType) ? inputMb : null;
			mainServiceWorkerClient.notifyCompletion(jobId, "completed", null, outputData, null, null, mbAudio, mbVideo);

			log.info("Subtitle job {} completed (cues={}, key={})", jobId, merged.size(), stored.key());
		} catch (JsonProcessingException e) {
			log.error("Subtitle job {} failed (JSON)", jobId, e);
			failJob(jobId, "AI response parse error: " + e.getMessage());
			throw new IllegalStateException(e);
		} catch (Exception e) {
			log.error("Subtitle job {} failed", jobId, e);
			String err = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
			failJob(jobId, err);
			throw new IllegalStateException(e);
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

	private static List<SrtFormatter.Cue> mergeCues(List<SrtFormatter.Cue> in, long timelineMaxMs) {
		if (in == null || in.isEmpty()) {
			return List.of();
		}
		List<SrtFormatter.Cue> items = in.stream()
				.filter(c -> c != null && c.text() != null && !c.text().trim().isBlank())
				.sorted(Comparator.comparingLong(SrtFormatter.Cue::startMs).thenComparingLong(SrtFormatter.Cue::endMs))
				.toList();

		List<SrtFormatter.Cue> deduped = new ArrayList<>(items.size());
		for (SrtFormatter.Cue c : items) {
			String text = c.text().trim();
			if (!deduped.isEmpty()) {
				SrtFormatter.Cue prev = deduped.get(deduped.size() - 1);
				if (text.equals(prev.text().trim()) && Math.abs(c.startMs() - prev.endMs()) <= 300) {
					continue;
				}
			}
			deduped.add(c);
		}
		return resolveNonOverlappingTimeline(deduped, timelineMaxMs);
	}

	/**
	 * Converts parser cues; used after AI refine may re-introduce overlaps.
	 */
	private static List<SrtFormatter.Cue> fromSrtParserCues(List<SrtParser.Cue> parsed) {
		if (parsed == null || parsed.isEmpty()) {
			return List.of();
		}
		List<SrtFormatter.Cue> out = new ArrayList<>(parsed.size());
		for (SrtParser.Cue c : parsed) {
			if (c == null || c.text() == null || c.text().trim().isBlank()) {
				continue;
			}
			out.add(new SrtFormatter.Cue(c.startMs(), c.endMs(), c.text()));
		}
		return out;
	}

	/**
	 * Clips each cue so it ends before the next cue starts (models often emit one block spanning minutes).
	 */
	private static List<SrtFormatter.Cue> resolveNonOverlappingTimeline(List<SrtFormatter.Cue> sorted, long timelineMaxMs) {
		long gapMs = 50L;
		long minDisplayMs = 320L;
		if (sorted.isEmpty()) {
			return List.of();
		}
		List<SrtFormatter.Cue> m = new ArrayList<>(sorted);
		m.sort(Comparator.comparingLong(SrtFormatter.Cue::startMs).thenComparingLong(SrtFormatter.Cue::endMs));

		long capEnd = timelineMaxMs > 0 ? timelineMaxMs : Long.MAX_VALUE;
		List<SrtFormatter.Cue> out = new ArrayList<>(m.size());
		long prevEnd = -gapMs;

		for (int i = 0; i < m.size(); i++) {
			SrtFormatter.Cue c = m.get(i);
			String text = c.text().trim();
			long rawNextStart = i + 1 < m.size() ? m.get(i + 1).startMs() : Long.MAX_VALUE;

			long st = Math.max(0L, Math.max(c.startMs(), prevEnd + gapMs));
			long maxEndByNext = rawNextStart - gapMs;
			long maxEndTotal = Math.min(maxEndByNext, capEnd);

			long en = Math.max(c.endMs(), st + minDisplayMs);
			en = Math.min(en, maxEndTotal);

			if (en < st + minDisplayMs) {
				st = Math.max(prevEnd + gapMs, Math.min(st, Math.max(0L, maxEndTotal - minDisplayMs)));
				en = Math.min(maxEndTotal, st + minDisplayMs);
			}
			if (en <= st || st >= capEnd) {
				continue;
			}
			out.add(new SrtFormatter.Cue(st, en, text));
			prevEnd = en;
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

	private static boolean matchesSyncedVoiceSubtitles(String sourceType, SubtitleJobMessage msg) {
		if (!"video".equals(sourceType)) {
			return false;
		}
		String vu = msg.voiceOverStorageUrl();
		if (vu == null || vu.isBlank()) {
			return false;
		}
		Double r = msg.voiceOverPlaybackRate();
		return r != null && Double.isFinite(r) && r > 0;
	}

	/**
	 * FE sync rate (e.g. 0.9×) applied to extracted original soundtrack so AI timings match what the user aligned.
	 */
	private static boolean appliesOriginalAudioPlaybackTempo(String sourceType, SubtitleJobMessage msg) {
		if (!"video".equals(sourceType)) {
			return false;
		}
		String vu = msg.voiceOverStorageUrl();
		if (vu != null && !vu.isBlank()) {
			return false;
		}
		Double r = msg.voiceOverPlaybackRate();
		if (r == null || !Double.isFinite(r) || r <= 0) {
			return false;
		}
		double clamped = clampPlaybackRate(r);
		return Math.abs(clamped - 1d) > 1e-4d;
	}

	private static double clampPlaybackRate(Double r) {
		double v = r != null && Double.isFinite(r) ? r : 1d;
		return Math.max(0.5d, Math.min(5d, v));
	}

	/** Same chaining rules as workspace export ({@code atempo} segments in [0.5, 2.0]). */
	private static String buildAtempoFilter(double speed) {
		double remaining = speed;
		List<String> chain = new ArrayList<>();
		while (remaining > 2.0d) {
			chain.add("atempo=2.0");
			remaining /= 2.0d;
		}
		while (remaining < 0.5d) {
			chain.add("atempo=0.5");
			remaining *= 2.0d;
		}
		chain.add("atempo=" + String.format(Locale.US, "%.4f", remaining));
		return String.join(",", chain);
	}
}

