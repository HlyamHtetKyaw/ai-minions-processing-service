package com.aiminions.processingservice.jobs.balancedsync;

import com.aiminions.processingservice.integration.MainServiceWorkerClient;
import com.aiminions.processingservice.jobs.transcribe.GenerationStatusPublisher;
import com.aiminions.processingservice.media.ffmpeg.FfmpegRunner;
import com.aiminions.processingservice.jobs.subtitles.SrtFormatter;
import com.aiminions.processingservice.storage.ObjectStorageTransferService;
import com.aiminions.processingservice.jobs.balancedsync.AnchorSyncPlanner.PlanResult;
import com.aiminions.processingservice.jobs.balancedsync.AnchorSyncPlanner.SegmentPlan;
import com.aiminions.processingservice.jobs.balancedsync.AnchorSyncPlanner.SegmentKind;
import com.aiminions.processingservice.config.ProcessingProperties;
import com.aiminions.processingservice.integration.AiServiceSubtitlesClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
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
	private final ProcessingProperties processingProperties;
	private final GenerationStatusPublisher generationStatusPublisher;
	private final MainServiceWorkerClient mainServiceWorkerClient;
	private final ObjectMapper objectMapper;
	private final AiServiceSubtitlesClient aiServiceSubtitlesClient;

	private static final double MIN_VIDEO_RATE = 0.60d;
	private static final double MAX_VIDEO_RATE = 1.10d;
	private static final double MIN_VOICE_RATE = 0.85d;
	private static final double MAX_VOICE_RATE = 1.40d;
	private static final double BALANCED_ALPHA = 0.70d;

	private static final Pattern DURATION = Pattern.compile("Duration:\\s*(\\d+):(\\d+):(\\d+(?:\\.\\d+)?)");
	private static final long ANCHOR_GAP_MS = 1500L;
	private static final long SUBTITLES_CHUNK_SECONDS = 25L;

	public void run(BalancedSyncJobMessage msg) {
		long jobId = msg.jobId() != null ? msg.jobId() : msg.aiGenerationId();
		Path workDir = null;
		try {
			workDir = Path.of(System.getProperty("java.io.tmpdir"), "balanced-sync-" + jobId);
			Files.createDirectories(workDir);
			generationStatusPublisher.publishProcessing(jobId, "download");
			Path video = objectStorageTransferService.download(require(msg.videoStorageUrl(), "videoStorageUrl"), workDir);
			Path voice = objectStorageTransferService.download(require(msg.voiceStorageUrl(), "voiceStorageUrl"), workDir);

			Path originalSrt;
			if (msg.originalVideoSrtStorageUrl() != null && !msg.originalVideoSrtStorageUrl().isBlank()) {
				originalSrt = objectStorageTransferService.download(msg.originalVideoSrtStorageUrl(), workDir);
			} else {
				generationStatusPublisher.publishProcessing(jobId, "gen_original_srt");
				originalSrt = generateAndUploadSrt(jobId, msg.userId(), "original", video, "video", workDir);
			}

			Path voiceSrt;
			if (msg.voiceOverSrtStorageUrl() != null && !msg.voiceOverSrtStorageUrl().isBlank()) {
				voiceSrt = objectStorageTransferService.download(msg.voiceOverSrtStorageUrl(), workDir);
			} else {
				generationStatusPublisher.publishProcessing(jobId, "gen_voice_srt");
				voiceSrt = generateAndUploadSrt(jobId, msg.userId(), "voice", voice, "audio", workDir);
			}

			generationStatusPublisher.publishProcessing(jobId, "parse_srt");
			String originalSrtText = Files.readString(originalSrt, StandardCharsets.UTF_8);
			String voiceSrtText = Files.readString(voiceSrt, StandardCharsets.UTF_8);
			List<SrtParser.Cue> originalCues = SrtParser.parse(originalSrtText);
			List<SrtParser.Cue> voiceCues = SrtParser.parse(voiceSrtText);
			long trueVoiceDurationMs = ffmpegRunner.getMediaDurationMillis(voice);
			long leadPadMs = Math.max(0L, firstStartMs(voiceCues) - firstStartMs(originalCues));
			log.info(
					"balanced-sync job {} voice physical duration {} ms (ffprobe), leadPadMs {} ms",
					jobId,
					trueVoiceDurationMs,
					leadPadMs);
			PlanResult plan = AnchorSyncPlanner.build(originalCues, voiceCues, ANCHOR_GAP_MS, trueVoiceDurationMs, leadPadMs);
			if (plan.segments().isEmpty()) {
				throw new IllegalStateException("No scene chunks found (empty SRT or anchor chunking produced 0 segments)");
			}

			Path out = workDir.resolve("balanced-" + System.currentTimeMillis() + ".mp4");
			generationStatusPublisher.publishProcessing(jobId, "ffmpeg_segments");
			runAnchorSyncFfmpeg(video, voice, out, workDir, plan, msg.protectFlip(), msg.protectHueDeg(), leadPadMs);

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
			result.put("originalChunkCount", plan.originalChunkCount());
			result.put("voiceChunkCount", plan.voiceChunkCount());

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

	private Path generateAndUploadSrt(
			long balancedSyncJobId,
			Long userId,
			String kind,
			Path input,
			String sourceType,
			Path workDir
	) throws Exception {
		if (userId == null) {
			throw new IllegalArgumentException("userId is required to generate subtitles");
		}
		String st = sourceType == null ? "audio" : sourceType.trim().toLowerCase(Locale.ROOT);
		String targetLanguage = "my";
		String style = "caption_rules_v1";

		Path subtitleDir = workDir.resolve("subtitles-" + kind);
		Files.createDirectories(subtitleDir);

		Path normalized = subtitleDir.resolve("normalized.wav");
		if ("video".equals(st)) {
			ffmpegRunner.run(List.of(
					"-y",
					"-i", input.toString(),
					"-vn",
					"-acodec", "pcm_s16le",
					"-ar", "44100",
					"-ac", "1",
					normalized.toString()
			));
		} else {
			ffmpegRunner.run(List.of(
					"-y",
					"-i", input.toString(),
					"-acodec", "pcm_s16le",
					"-ar", "44100",
					"-ac", "1",
					normalized.toString()
			));
		}

		Path chunksDir = subtitleDir.resolve("chunks");
		Files.createDirectories(chunksDir);
		Path chunkPattern = chunksDir.resolve("chunk-%03d.wav");
		ffmpegRunner.run(List.of(
				"-y",
				"-i", normalized.toString(),
				"-f", "segment",
				"-segment_time", String.valueOf(SUBTITLES_CHUNK_SECONDS),
				"-reset_timestamps", "1",
				"-acodec", "pcm_s16le",
				"-ar", "44100",
				"-ac", "1",
				chunkPattern.toString()
		));

		List<Path> chunks = Files.list(chunksDir)
				.filter(p -> p.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".wav"))
				.sorted(Comparator.comparing(p -> p.getFileName().toString()))
				.toList();
		if (chunks.isEmpty()) {
			throw new IllegalStateException("No audio chunks were created for subtitles (" + kind + ")");
		}

		List<SrtFormatter.Cue> allCues = new ArrayList<>();
		for (int i = 0; i < chunks.size(); i++) {
			Path chunk = chunks.get(i);
			byte[] bytes = Files.readAllBytes(chunk);
			long offsetMs = (long) i * SUBTITLES_CHUNK_SECONDS * 1000L;
			long durationMs = SUBTITLES_CHUNK_SECONDS * 1000L;
			var aiData = aiServiceSubtitlesClient.requestSubtitleCuesWithAudio(
					balancedSyncJobId,
					bytes,
					offsetMs,
					durationMs,
					i,
					targetLanguage,
					style
			);
			var cues = aiData.path("result").path("cues");
			if (cues == null || !cues.isArray()) continue;
			for (var c : cues) {
				long startMs = c.path("startMs").asLong(-1);
				long endMs = c.path("endMs").asLong(-1);
				String text = c.path("text").asText("");
				if (startMs < 0 || endMs < 0) continue;
				allCues.add(new SrtFormatter.Cue(startMs + offsetMs, endMs + offsetMs, text));
			}
		}
		if (allCues.isEmpty()) {
			throw new IllegalStateException("No subtitle cues were generated (" + kind + ")");
		}

		// Reuse the same merge heuristics as subtitles pipeline (minimal subset).
		List<SrtFormatter.Cue> merged = allCues.stream()
				.filter(c -> c != null && c.text() != null && !c.text().trim().isBlank())
				.sorted(Comparator.comparingLong(SrtFormatter.Cue::startMs).thenComparingLong(SrtFormatter.Cue::endMs))
				.toList();

		String srtText = SrtFormatter.toSrt(merged);
		Path out = subtitleDir.resolve(kind + ".srt");
		Files.writeString(out, srtText, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

		String keyHint = "subtitles/" + userId + "/balanced-sync/" + balancedSyncJobId + "-" + kind + ".srt";
		objectStorageTransferService.uploadFile(out, keyHint, "application/x-subrip");
		return out;
	}

	private void runAnchorSyncFfmpeg(
			Path video,
			Path voice,
			Path out,
			Path workDir,
			PlanResult plan,
			Boolean protectFlip,
			Double protectHueDeg,
			long leadPadMs
	) throws Exception {
		Path segDir = workDir.resolve("segments");
		Files.createDirectories(segDir);

		int threads = Math.max(0, processingProperties.getWorkspaceExportFfmpegThreads());
		String preset = processingProperties.getWorkspaceExportPreset();
		if (preset == null || preset.isBlank()) preset = "veryfast";
		int crf = Math.max(10, Math.min(35, processingProperties.getWorkspaceExportCrf()));

		List<Path> segmentFiles = new ArrayList<>();
		for (int i = 0; i < plan.segments().size(); i++) {
			SegmentPlan sp = plan.segments().get(i);
			Path segOut = segDir.resolve(String.format(Locale.ROOT, "seg-%03d.mp4", i));
			segmentFiles.add(segOut);

			double ss = Math.max(0d, sp.srcStartMs() / 1000d);
			double to = Math.max(ss, sp.srcEndMs() / 1000d);

			List<String> vf = new ArrayList<>();
			// Optional video-only start padding on the first segment.
			if (i == 0 && leadPadMs > 0) {
				vf.add("tpad=start_mode=clone:start_duration=" + fmt(leadPadMs / 1000d));
			}
			if (sp.kind() == SegmentKind.TALK) {
				vf.add("setpts=" + fmt(sp.setptsMultiplier()) + "*PTS");
				if (sp.setptsMultiplier() > 2.0d) {
					// Slow-motion: add interpolation for smoothness.
					vf.add("minterpolate=fps=60");
				}
			}
			if (Boolean.TRUE.equals(protectFlip)) {
				vf.add("hflip");
			}
			if (protectHueDeg != null && Double.isFinite(protectHueDeg) && Math.abs(protectHueDeg) > 0.0001d) {
				vf.add("hue=h=" + fmt(protectHueDeg));
			}
			// Ensure each segment has consistent timestamps starting at 0 for concat.
			vf.add("setpts=PTS-STARTPTS");

			List<String> args = new ArrayList<>();
			args.add("-y");
			if (threads > 0) {
				args.add("-threads");
				args.add(String.valueOf(threads));
				args.add("-filter_threads");
				args.add(String.valueOf(threads));
				args.add("-filter_complex_threads");
				args.add(String.valueOf(threads));
			}
			args.add("-ss");
			args.add(fmt(ss));
			args.add("-to");
			args.add(fmt(to));
			args.add("-i");
			args.add(video.toString());
			args.add("-an");
			args.add("-vf");
			args.add(String.join(",", vf));
			args.add("-c:v");
			args.add("libx264");
			args.add("-preset");
			args.add(preset.trim());
			args.add("-crf");
			args.add(String.valueOf(crf));
			args.add("-pix_fmt");
			args.add("yuv420p");
			args.add(segOut.toString());

			ffmpegRunner.run(args);
		}

		Path concatList = workDir.resolve("concat.txt");
		StringBuilder sb = new StringBuilder(segmentFiles.size() * 64);
		for (Path p : segmentFiles) {
			sb.append("file '").append(p.toAbsolutePath().toString().replace("'", "\\'")).append("'\n");
		}
		Files.writeString(concatList, sb.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

		Path silentMaster = workDir.resolve("silent-master.mp4");
		ffmpegRunner.run(List.of(
				"-y",
				"-f", "concat",
				"-safe", "0",
				"-i", concatList.toString(),
				"-c", "copy",
				silentMaster.toString()
		));

		muxBalancedOutputNoClip(silentMaster, voice, out, threads, preset, crf);
	}

	/**
	 * Mux stitched silent video + narration without {@code -shortest} clipping.
	 * <ul>
	 * <li>If video ends before voice: clone-pad video tail until voice ends (+ small slack).</li>
	 * <li>If voice ends before video: tempo-stretch audio so its timeline matches video (no video cut).</li>
	 * <li>If durations already align: stream copy mux.</li>
	 * </ul>
	 */
	private void muxBalancedOutputNoClip(
			Path silentMaster,
			Path voice,
			Path out,
			int threads,
			String preset,
			int crf
	) throws Exception {
		long videoMs = ffmpegRunner.getMediaDurationMillis(silentMaster);
		long voiceMs = ffmpegRunner.getMediaDurationMillis(voice);
		if (videoMs <= 0 || voiceMs <= 0) {
			throw new IllegalStateException("Could not probe video/voice duration for mux (videoMs=" + videoMs + ", voiceMs=" + voiceMs + ")");
		}

		final long epsilonMs = 80L;
		final long slackPadMs = 250L;

		boolean videoShorter = videoMs + epsilonMs < voiceMs;
		boolean voiceShorter = voiceMs + epsilonMs < videoMs;

		if (!videoShorter && !voiceShorter) {
			ffmpegRunner.run(List.of(
					"-y",
					"-i", silentMaster.toString(),
					"-i", voice.toString(),
					"-map", "0:v:0",
					"-map", "1:a:0",
					"-c:v", "copy",
					"-c:a", "copy",
					out.toString()
			));
			return;
		}

		List<String> args = new ArrayList<>();
		args.add("-y");
		if (threads > 0) {
			args.add("-threads");
			args.add(String.valueOf(threads));
		}
		args.add("-i");
		args.add(silentMaster.toString());
		args.add("-i");
		args.add(voice.toString());

		if (videoShorter) {
			double padSec = (voiceMs - videoMs + slackPadMs) / 1000.0;
			padSec = Math.max(padSec, 0.04d);
			String pad = fmt(padSec);
			log.info(
					"balanced-sync mux: video shorter than narration (videoMs={} voiceMs={}); padding video tail {} s",
					videoMs,
					voiceMs,
					pad);
			args.add("-filter_complex");
			args.add("[0:v]tpad=stop_mode=clone:stop_duration=" + pad + "[v]");
			args.add("-map");
			args.add("[v]");
			args.add("-map");
			args.add("1:a:0");
			args.add("-c:v");
			args.add("libx264");
			args.add("-preset");
			args.add((preset != null && !preset.isBlank()) ? preset.trim() : "veryfast");
			args.add("-crf");
			args.add(String.valueOf(crf));
			args.add("-pix_fmt");
			args.add("yuv420p");
			args.add("-c:a");
			args.add("copy");
			args.add("-movflags");
			args.add("+faststart");
			args.add(out.toString());
			ffmpegRunner.run(args);
			return;
		}

		// Voice shorter than video: speed up full video timeline to match narration (audio copied = no cut).
		double ptsFactor = voiceMs / (double) videoMs;
		if (!Double.isFinite(ptsFactor) || ptsFactor <= 0.00001d || ptsFactor >= 1d) {
			throw new IllegalStateException("Invalid setpts factor for balanced mux (voiceMs=" + voiceMs + ", videoMs=" + videoMs + ")");
		}
		log.info(
				"balanced-sync mux: narration shorter than video (videoMs={} voiceMs={}); setpts factor {} (faster video, full audio)",
				videoMs,
				voiceMs,
				fmt(ptsFactor));
		args.add("-filter_complex");
		args.add("[0:v]setpts=" + fmt(ptsFactor) + "*PTS,setsar=1[v]");
		args.add("-map");
		args.add("[v]");
		args.add("-map");
		args.add("1:a:0");
		args.add("-c:v");
		args.add("libx264");
		args.add("-preset");
		args.add((preset != null && !preset.isBlank()) ? preset.trim() : "veryfast");
		args.add("-crf");
		args.add(String.valueOf(crf));
		args.add("-pix_fmt");
		args.add("yuv420p");
		args.add("-c:a");
		args.add("copy");
		args.add("-movflags");
		args.add("+faststart");
		args.add(out.toString());
		ffmpegRunner.run(args);
	}

	private static long firstStartMs(List<SrtParser.Cue> cues) {
		if (cues == null || cues.isEmpty()) return 0;
		long min = Long.MAX_VALUE;
		for (SrtParser.Cue c : cues) {
			if (c == null) continue;
			min = Math.min(min, Math.max(0, c.startMs()));
		}
		return min == Long.MAX_VALUE ? 0 : min;
	}

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

