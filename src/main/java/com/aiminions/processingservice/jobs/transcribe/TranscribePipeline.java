package com.aiminions.processingservice.jobs.transcribe;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;

import com.aiminions.processingservice.config.ProcessingProperties;
import com.aiminions.processingservice.integration.AiServiceTranscribeClient;
import com.aiminions.processingservice.integration.MainServiceWorkerClient;
import com.aiminions.processingservice.media.ffmpeg.FfmpegRunner;
import com.aiminions.processingservice.storage.ObjectStorageTransferService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class TranscribePipeline {

	private final ObjectStorageTransferService objectStorageTransferService;
	private final FfmpegRunner ffmpegRunner;
	private final ProcessingProperties processingProperties;
	private final GenerationStatusPublisher generationStatusPublisher;
	private final MainServiceWorkerClient mainServiceWorkerClient;
	private final AiServiceTranscribeClient aiServiceTranscribeClient;
	private final ObjectMapper objectMapper;

	public void run(TranscribeJobMessage msg) {
		long jobId = msg.jobId() != null ? msg.jobId() : msg.aiGenerationId();
		Path workDir = null;
		try {
			log.info(
					"[transcribe][start] jobId={} userId={} aiGenerationId={} sourceType={} storageUrl={} s3Key={}",
					jobId,
					msg.userId(),
					msg.aiGenerationId(),
					msg.sourceType(),
					msg.storageUrl(),
					msg.s3Key());
			workDir = Files.createTempDirectory("transcribe-" + jobId + "-");
			generationStatusPublisher.publishProcessing(jobId, "download");
			Path input = objectStorageTransferService.download(msg.storageUrl(), workDir);
			BigDecimal inputMb = bytesToMbSafe(input);
			log.info(
					"[transcribe][downloaded] jobId={} inputPath={} inputMb={}",
					jobId,
					input,
					inputMb);

			Path normalized = workDir.resolve("normalized.wav");
			String sourceType = msg.sourceType() == null ? "audio" : msg.sourceType().trim().toLowerCase();
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
				log.info("[transcribe][extract_audio][done] jobId={} normalizedPath={}", jobId, normalized);
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
				log.info("[transcribe][normalize_audio][done] jobId={} normalizedPath={}", jobId, normalized);
			}

			Path cleaned = workDir.resolve("cleaned.wav");
			generationStatusPublisher.publishProcessing(jobId, "silence_removal");
			String silenceremove = String.format(
					Locale.US,
					"silenceremove=start_periods=1:start_duration=0.1:start_threshold=-60dB:stop_periods=-1:stop_duration=%.3f:stop_threshold=%s:detection=peak",
					processingProperties.getSilenceStopDurationSeconds(),
					processingProperties.getSilenceStopThreshold());
			ffmpegRunner.run(List.of("-y", "-i", normalized.toString(), "-af", silenceremove, cleaned.toString()));
			log.info("[transcribe][silence_removal][done] jobId={} cleanedPath={}", jobId, cleaned);

			generationStatusPublisher.publishProcessing(jobId, "ai_transcription");
			byte[] cleanedWav = Files.readAllBytes(cleaned);
			log.info("[transcribe][ai_request] jobId={} cleanedWavBytes={}", jobId, cleanedWav.length);
			JsonNode aiData = aiServiceTranscribeClient.requestTranscriptionWithAudio(jobId, cleanedWav);
			log.info(
					"[transcribe][ai_response] jobId={} usedProvider={} featureType={} hasResult={}",
					jobId,
					aiData.path("usedProvider").asText(""),
					aiData.path("featureType").asText(""),
					aiData.hasNonNull("result"));

			ObjectNode outputDataNode = objectMapper.createObjectNode();
			outputDataNode.put("type", "transcribe");
			outputDataNode.set("result", aiData.get("result"));
			if (aiData.hasNonNull("usedProvider")) {
				outputDataNode.put("usedProvider", aiData.get("usedProvider").asText());
			}
			if (aiData.hasNonNull("featureType")) {
				outputDataNode.put("featureType", aiData.get("featureType").asText());
			}
			String transcriptText = aiData.path("result").path("text").asText("");
			outputDataNode.put("text", transcriptText);

			String outputData = objectMapper.writeValueAsString(outputDataNode);

			generationStatusPublisher.publishCompleted(jobId, outputData);
			BigDecimal mbAudio = "video".equals(sourceType) ? null : inputMb;
			BigDecimal mbVideo = "video".equals(sourceType) ? inputMb : null;
			BigDecimal tokenIn = aiData.path("result").path("tokenIn").isNumber()
					? aiData.path("result").path("tokenIn").decimalValue()
					: null;
			BigDecimal tokenOut = aiData.path("result").path("tokenOut").isNumber()
					? aiData.path("result").path("tokenOut").decimalValue()
					: null;
			mainServiceWorkerClient.notifyCompletion(jobId, "completed", null, outputData, tokenIn, tokenOut, mbAudio, mbVideo);
			log.info("Transcribe job {} completed (transcript length {})", jobId, transcriptText.length());
		} catch (JsonProcessingException e) {
			log.error("Transcribe job {} failed (JSON)", jobId, e);
			failJob(jobId, "AI response parse error: " + e.getMessage());
			throw new IllegalStateException(e);
		} catch (Exception e) {
			log.error("Transcribe job {} failed", jobId, e);
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
}
