package com.aiminions.processingservice.jobs.balancedsync;

import com.aiminions.processingservice.integration.MainServiceWorkerClient;
import com.aiminions.processingservice.jobs.transcribe.GenerationStatusPublisher;
import com.aiminions.processingservice.media.ffmpeg.FfmpegRunner;
import com.aiminions.processingservice.storage.ObjectStorageTransferService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalDouble;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@RequiredArgsConstructor
@Slf4j
public class BalancedSyncPipeline {

	private final ObjectStorageTransferService objectStorageTransferService;
	private final FfmpegRunner ffmpegRunner;
	private final GenerationStatusPublisher generationStatusPublisher;
	private final MainServiceWorkerClient mainServiceWorkerClient;
	private final ObjectMapper objectMapper;

	private static final double MIN_VIDEO_RATE = 0.60d;
	private static final double MAX_VIDEO_RATE = 1.10d;
	private static final double MIN_VOICE_RATE = 0.85d;
	private static final double MAX_VOICE_RATE = 1.40d;
	private static final double BALANCED_ALPHA = 0.70d;

	private static final Pattern DURATION = Pattern.compile("Duration:\\s*(\\d+):(\\d+):(\\d+(?:\\.\\d+)?)");

	public void run(BalancedSyncJobMessage msg) {
		long jobId = msg.jobId() != null ? msg.jobId() : msg.aiGenerationId();
		Path workDir = null;
		try {
			workDir = Files.createTempDirectory("balanced-sync-" + jobId + "-");
			generationStatusPublisher.publishProcessing(jobId, "download");
			Path video = objectStorageTransferService.download(require(msg.videoStorageUrl(), "videoStorageUrl"), workDir);
			Path voice = objectStorageTransferService.download(require(msg.voiceStorageUrl(), "voiceStorageUrl"), workDir);

			// Prefer worker-measured durations for perfect alignment (browser/container metadata can drift).
			generationStatusPublisher.publishProcessing(jobId, "probe");
			double vd = probeDurationSeconds(video).orElse(0d);
			double ad = probeDurationSeconds(voice).orElse(0d);

			double videoRate;
			double voiceRate;
			double targetDurationSec;
			if (vd > 0.01d && ad > 0.01d) {
				SolvedRates solved = solveRates(vd, ad);
				videoRate = solved.videoRate();
				voiceRate = solved.voiceRate();
				targetDurationSec = solved.targetDurationSec();
			} else {
				// Fallback to provided values.
				videoRate = safeRate(msg.videoRate());
				voiceRate = safeRate(msg.voiceRate());
				targetDurationSec = safeDuration(msg.targetDurationSec());
				if (targetDurationSec <= 0 && vd > 0.01d) {
					targetDurationSec = Math.max(0.01d, vd / Math.max(0.000001d, videoRate));
				}
			}

			Path out = workDir.resolve("balanced-" + System.currentTimeMillis() + ".mp4");
			generationStatusPublisher.publishProcessing(jobId, "ffmpeg");
			ffmpegRunner.run(buildFfmpegArgs(
					video, voice, out,
					videoRate, voiceRate, targetDurationSec,
					msg.protectFlip(), msg.protectHueDeg()));

			generationStatusPublisher.publishProcessing(jobId, "upload");
			String keyHint = "video-editor/" + (msg.userId() == null ? "unknown" : msg.userId()) + "/balanced-sync/"
					+ jobId + ".mp4";
			ObjectStorageTransferService.StoredObject stored = objectStorageTransferService.uploadFile(out, keyHint, "video/mp4");
			String readUrl = objectStorageTransferService.presignWorkspaceRead(stored.key());

			ObjectNode output = objectMapper.createObjectNode();
			output.put("type", "balanced_sync");
			ObjectNode result = output.putObject("result");
			result.put("storageUrl", stored.storageUrl());
			result.put("readUrl", readUrl);
			result.put("s3Key", stored.key());
			result.put("videoRate", round6(videoRate));
			result.put("voiceRate", round6(voiceRate));
			result.put("targetDurationSec", round6(targetDurationSec));
			if (vd > 0) result.put("measuredVideoDurationSec", round6(vd));
			if (ad > 0) result.put("measuredVoiceDurationSec", round6(ad));

			String outputJson = objectMapper.writeValueAsString(output);
			generationStatusPublisher.publishCompleted(jobId, outputJson);
			mainServiceWorkerClient.notifyCompletion(jobId, "completed", null, outputJson);
			log.info("Balanced sync job {} completed: {}", jobId, stored.key());
		} catch (Exception e) {
			log.error("Balanced sync job {} failed", jobId, e);
			failJob(jobId, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
			throw new IllegalStateException(e);
		} finally {
			if (workDir != null) {
				try {
					FileSystemUtils.deleteRecursively(workDir);
				} catch (Exception ex) {
					log.warn("Could not delete work dir {}: {}", workDir, ex.getMessage());
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

	private static String require(String v, String name) {
		if (v == null || v.isBlank()) throw new IllegalArgumentException(name + " is required");
		return v.trim();
	}

	private static double safeRate(Double n) {
		if (n == null || !Double.isFinite(n) || n <= 0) return 1d;
		return Math.max(0.25d, Math.min(8d, n));
	}

	private static double safeDuration(Double n) {
		if (n == null || !Double.isFinite(n) || n <= 0) return 0d;
		return Math.max(0.01d, Math.min(6 * 60 * 60, n)); // cap to 6h
	}

	private OptionalDouble probeDurationSeconds(Path file) {
		if (file == null) return OptionalDouble.empty();
		try {
			// `ffmpeg -i` exits non-zero; we only parse its output.
			FfmpegRunner.RunResult r = ffmpegRunner.runRaw(List.of("-i", file.toString()), 2, TimeUnit.MINUTES);
			String out = r.output();
			Matcher m = DURATION.matcher(out);
			if (!m.find()) return OptionalDouble.empty();
			double h = Double.parseDouble(m.group(1));
			double min = Double.parseDouble(m.group(2));
			double sec = Double.parseDouble(m.group(3));
			double total = h * 3600d + min * 60d + sec;
			if (!Double.isFinite(total) || total <= 0) return OptionalDouble.empty();
			return OptionalDouble.of(total);
		} catch (Exception e) {
			return OptionalDouble.empty();
		}
	}

	private static SolvedRates solveRates(double videoDurationSec, double voiceDurationSec) {
		double vd = Math.max(0.01d, videoDurationSec);
		double ad = Math.max(0.01d, voiceDurationSec);
		double r = ad / vd;
		if (!Double.isFinite(r) || r <= 0.000001d) r = 1d;

		double alpha = clamp(BALANCED_ALPHA, 0.01d, 0.99d);
		double targetVideoRate = 1d / Math.pow(r, alpha);

		double vMinFromVoice = (MIN_VOICE_RATE * vd) / ad;
		double vMaxFromVoice = (MAX_VOICE_RATE * vd) / ad;
		double feasibleMin = Math.max(MIN_VIDEO_RATE, Math.min(vMinFromVoice, vMaxFromVoice));
		double feasibleMax = Math.min(MAX_VIDEO_RATE, Math.max(vMinFromVoice, vMaxFromVoice));
		if (feasibleMin > feasibleMax) {
			feasibleMin = 1d;
			feasibleMax = 1d;
		}

		double videoRate = clamp(targetVideoRate, feasibleMin, feasibleMax);
		double voiceRate = (ad * videoRate) / vd;
		double targetDuration = vd / Math.max(0.000001d, videoRate);
		return new SolvedRates(videoRate, voiceRate, targetDuration);
	}

	private static double clamp(double v, double min, double max) {
		if (!Double.isFinite(v)) return 1d;
		return Math.max(min, Math.min(max, v));
	}

	private record SolvedRates(double videoRate, double voiceRate, double targetDurationSec) {}

	private static double round6(double v) {
		return Math.round(v * 1_000_000d) / 1_000_000d;
	}

	private static String fmt(double n) {
		return String.format(Locale.US, "%.6f", n);
	}

	private static String buildAtempoFilter(double rate) {
		// FFmpeg atempo accepts [0.5, 2.0] per filter; chain multiple to reach wider rates.
		double r = rate;
		List<String> parts = new ArrayList<>();
		while (r > 2.0d) {
			parts.add("atempo=2.0");
			r /= 2.0d;
		}
		while (r < 0.5d) {
			parts.add("atempo=0.5");
			r /= 0.5d;
		}
		parts.add("atempo=" + fmt(r));
		return String.join(",", parts);
	}

	private static List<String> buildFfmpegArgs(
			Path video,
			Path voice,
			Path out,
			double videoRate,
			double voiceRate,
			double targetDurationSec,
			Boolean protectFlip,
			Double protectHueDeg
	) {
		List<String> args = new ArrayList<>();
		args.add("-y");
		args.add("-i");
		args.add(video.toString());
		args.add("-i");
		args.add(voice.toString());

		List<String> vOps = new ArrayList<>();
		vOps.add("setpts=" + fmt(1d / videoRate) + "*PTS");
		if (Boolean.TRUE.equals(protectFlip)) {
			vOps.add("hflip");
		}
		if (protectHueDeg != null && Double.isFinite(protectHueDeg) && Math.abs(protectHueDeg) > 0.0001d) {
			vOps.add("hue=h=" + fmt(protectHueDeg));
		}

		String vChain = String.join(",", vOps);
		if (targetDurationSec > 0) {
			vChain = vChain + ",trim=duration=" + fmt(targetDurationSec) + ",setpts=PTS-STARTPTS";
		}

		String aChain = buildAtempoFilter(voiceRate);
		if (targetDurationSec > 0) {
			// Guarantee audio matches video duration exactly (pad with silence if needed, trim if longer).
			aChain = aChain
					+ ",apad=pad_dur=" + fmt(targetDurationSec)
					+ ",atrim=duration=" + fmt(targetDurationSec)
					+ ",asetpts=PTS-STARTPTS";
		}

		String filter = "[0:v]" + vChain + "[v];" +
				"[1:a]" + aChain + "[a]";

		args.add("-filter_complex");
		args.add(filter);
		args.add("-map");
		args.add("[v]");
		args.add("-map");
		args.add("[a]");
		args.add("-c:v");
		args.add("libx264");
		args.add("-preset");
		args.add("veryfast");
		args.add("-crf");
		args.add("23");
		args.add("-c:a");
		args.add("aac");
		args.add("-movflags");
		args.add("+faststart");
		args.add(out.toString());
		return args;
	}
}

