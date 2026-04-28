package com.aiminions.processingservice.storage;

import com.aiminions.processingservice.media.ffmpeg.FfmpegRunner;
import com.aiminions.processingservice.storage.dto.WorkspaceExportResponse;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.core.io.ClassPathResource;

import com.google.myanmartools.ZawgyiDetector;
import com.google.myanmartools.TransliterateZ2U;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.Map;
import java.util.regex.Matcher;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;

@Service
@RequiredArgsConstructor
@Slf4j
public class WorkspaceExportService {

	private static final ZawgyiDetector ZAWGYI_DETECTOR = new ZawgyiDetector();
	private static final TransliterateZ2U Z2U = new TransliterateZ2U("z2u");
	private static final Pattern MYANMAR_CHARS = Pattern.compile("[\\u1000-\\u109F\\uAA60-\\uAA7F]");

	/**
	 * Same as frontend {@code SUBTITLE_PREVIEW_TO_BURN_FONT_FACTOR}: libass {@code FontSize} reads smaller
	 * than browser CSS px for the same geometry, especially for Myanmar shaping.
	 */
	private static final double SUBTITLE_PREVIEW_TO_BURN_FONT_FACTOR = 1.52d;

	/**
	 * ASS {@code BackColour} for black at UI opacity 0–100 (%). Format {@code &HAABBGGRR}; {@code AA} is 00=opaque, FF=transparent.
	 */
	private static String assBackColourBlackFromOpacityPercent(int opacityPercent) {
		int o = Math.max(0, Math.min(100, opacityPercent));
		int aa = (int) Math.round(255.0 * (1.0 - o / 100.0));
		aa = Math.max(0, Math.min(255, aa));
		return String.format(Locale.ROOT, "&H%02X000000", aa);
	}

	/**
	 * For {@code BorderStyle=3} (opaque box), libass/VSFilter use {@code Outline} as the box padding around the
	 * text; {@code Outline=0} often yields no visible background — matching reports of "opacity missing" on burn-in.
	 */
	private static int subtitleBoxOutlinePx(int fontSize) {
		int cap = fontSize > 120 ? 26 : 18;
		return Math.max(3, Math.min(cap, (int) Math.round(fontSize * 0.14d)));
	}

	/**
	 * FFmpeg's {@code subtitles=} {@code force_style} value is parsed as a filter option; unescaped {@code &}
	 * in {@code &H...} colours can truncate options and drop font/back styles.
	 */
	private static String escapeAmpersandsForFfmpegFilterOption(String s) {
		if (s == null || s.isBlank()) {
			return "";
		}
		return s.replace("&", "\\&");
	}

	private final ObjectStorageTransferService objectStorageTransferService;
	private final FfmpegRunner ffmpegRunner;
	private final com.aiminions.processingservice.config.ProcessingProperties processingProperties;

	public WorkspaceExportResponse exportVideo(Long userId, JsonNode payload) {
		if (payload == null || payload.isNull()) {
			throw new ResponseStatusException(BAD_REQUEST, "payload is required");
		}
		String videoUrl = ObjectStorageTransferService.stripUrlFragmentForDownload(readRequiredText(payload, "videoUrl"));
		double trimStart = readNumber(payload, "trimStart", 0d);
		double trimEnd = readNumber(payload, "trimEnd", 0d);
		double speed = readNumber(payload, "speed", 1d);
		if (speed <= 0) {
			speed = 1d;
		}

		Path workDir = null;
		try {
			workDir = Files.createTempDirectory("workspace-export-");
			Path srtFile = maybeWriteSrtFile(payload, workDir);
			Path input = objectStorageTransferService.download(videoUrl, workDir);
			Path output = workDir.resolve("export-" + System.currentTimeMillis() + ".mp4");
			runEncode(input, output, payload, trimStart, trimEnd, speed, workDir, srtFile);

			String keyHint = "video-editor/" + (userId == null ? "unknown" : userId) + "/exports/"
					+ System.currentTimeMillis() + ".mp4";
			ObjectStorageTransferService.StoredObject stored = objectStorageTransferService
					.uploadFile(output, keyHint, "video/mp4");
			String readUrl = objectStorageTransferService.presignWorkspaceRead(stored.key());
			return new WorkspaceExportResponse(stored.storageUrl(), readUrl, stored.key());
		} catch (ResponseStatusException ex) {
			throw ex;
		} catch (Exception ex) {
			log.error("Workspace export failed for userId={}", userId, ex);
			String reason = ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
			if (reason.length() > 800) {
				reason = reason.substring(0, 800) + "…";
			}
			throw new ResponseStatusException(INTERNAL_SERVER_ERROR, "Failed to export workspace video: " + reason, ex);
		} finally {
			if (workDir != null) {
				try {
					Files.walk(workDir)
							.sorted((a, b) -> b.getNameCount() - a.getNameCount())
							.forEach(path -> {
								try {
									Files.deleteIfExists(path);
								} catch (IOException ignored) {
									// best effort cleanup
								}
							});
				} catch (IOException ignored) {
					// best effort cleanup
				}
			}
		}
	}

	private void runEncode(
			Path input,
			Path output,
			JsonNode payload,
			double trimStart,
			double trimEnd,
			double speed,
			Path workDir,
			Path srtFile
	) throws IOException, InterruptedException {
		int[] size = probeVideoSize(input);
		ExportSegmentPlan plan = buildExportSegmentPlan(payload, trimStart, trimEnd, speed);
		boolean muted = readBoolean(payload.path("originalAudio"), "muted", false);
		double originalVol = readNumber(payload.path("originalAudio"), "volume", 100d) / 100d;
		boolean hasAudio = probeHasAudio(input);
		boolean audioFromGraph = plan.multi() && !muted && hasAudio;

		List<String> args = new ArrayList<>();
		args.add("-y");
		int ffmpegThreads = Math.max(0, processingProperties.getWorkspaceExportFfmpegThreads());
		log.info("FFmpeg threads: {}", ffmpegThreads);
		if (ffmpegThreads > 0) {
			args.add("-threads");
			args.add(String.valueOf(ffmpegThreads));
			args.add("-filter_threads");
			args.add(String.valueOf(ffmpegThreads));
			args.add("-filter_complex_threads");
			args.add(String.valueOf(ffmpegThreads));
		}
		if (!plan.multi()) {
			double t0 = plan.segments().get(0)[0];
			if (t0 > 0) {
				args.add("-ss");
				args.add(formatDecimal(t0));
			}
			args.add("-i");
			args.add(input.toString());
			double t1 = plan.segments().get(0)[1];
			if (t1 > t0 && t1 > 0) {
				args.add("-t");
				args.add(formatDecimal(t1 - Math.max(0d, t0)));
			}
		} else {
			args.add("-i");
			args.add(input.toString());
		}

		List<ImageInput> images = collectImageInputs(payload, workDir);
		for (ImageInput image : images) {
			if (image.loopInput()) {
				args.add("-stream_loop");
				args.add("-1");
			}
			args.add("-i");
			args.add(image.path().toString());
		}

		String filterComplex = buildFilterComplex(
				payload, plan, images, srtFile, workDir, size[0], size[1], audioFromGraph, originalVol);
		args.add("-filter_complex");
		args.add(filterComplex);
		args.add("-map");
		args.add("[vout]");

		if (muted) {
			args.add("-an");
		} else if (audioFromGraph) {
			args.add("-map");
			args.add("[aout]");
		} else if (plan.multi()) {
			// Disjoint kept spans: without a decodable audio stream we cannot align 0:a to the concat video.
			args.add("-an");
		} else {
			args.add("-map");
			args.add("0:a?");
			List<String> audioFilters = new ArrayList<>();
			if (Math.abs(speed - 1d) > 0.0001d) {
				audioFilters.add(buildAtempoFilter(speed));
			}
			if (Math.abs(originalVol - 1d) > 0.0001d) {
				audioFilters.add("volume=" + formatDecimal(Math.max(0d, originalVol)));
			}
			if (!audioFilters.isEmpty()) {
				args.add("-af");
				args.add(String.join(",", audioFilters));
			}
		}

		args.add("-c:v");
		args.add("libx264");
		args.add("-preset");
		String preset = processingProperties.getWorkspaceExportPreset();
		if (preset == null || preset.isBlank()) {
			preset = "veryfast";
		}
		args.add(preset.trim());
		args.add("-crf");
		int crf = Math.max(10, Math.min(35, processingProperties.getWorkspaceExportCrf()));
		args.add(String.valueOf(crf));
		args.add("-c:a");
		args.add("aac");
		args.add("-movflags");
		args.add("+faststart");
		args.add(output.toString());
		// Force libass to resolve fonts from our extracted fonts dir (avoids silent fallback).
		if (srtFile != null && workDir != null) {
			// Prefer bundled font dir for deterministic production output.
			String fontsDir = extractBundledFontDir(workDir);
			if (fontsDir.isBlank()) {
				fontsDir = processingProperties.getSubtitlesFontsDir() == null ? "" : processingProperties.getSubtitlesFontsDir().trim();
			}
			if (!fontsDir.isBlank()) {
				// Ask libass to be verbose about font selection so we can verify it's using our font.
				args.add(0, "debug");
				args.add(0, "-loglevel");
				Path fontConfig = writeFontConfig(workDir, fontsDir);
				Map<String, String> env = Map.of(
						"FONTCONFIG_FILE", fontConfig.toString(),
						"FONTCONFIG_PATH", fontConfig.getParent().toString(),
						"XDG_CACHE_HOME", workDir.resolve("fc-cache").toString()
				);
				var r = ffmpegRunner.runRaw(args, env, 45, java.util.concurrent.TimeUnit.MINUTES);
				logFontSelection(r.output());
				if (r.exitCode() != 0) {
					String out = r.output();
					String tail = out.length() > 4000 ? out.substring(out.length() - 4000) : out;
					throw new IllegalStateException("ffmpeg exited with " + r.exitCode() + ": " + tail);
				}
				return;
			}
		}
		ffmpegRunner.run(args);
	}

	private static final Pattern VIDEO_SIZE = Pattern.compile("(\\d{2,5})x(\\d{2,5})");

	private int[] probeVideoSize(Path input) {
		try {
			// ffmpeg prints stream info on stderr and exits non-zero without output; we still get combined output.
			var r = ffmpegRunner.runRaw(List.of("-hide_banner", "-i", input.toString()), 30, java.util.concurrent.TimeUnit.SECONDS);
			String out = r.output() == null ? "" : r.output();
			// Match the first WxH after "Video:" line.
			for (String line : out.split("\\R")) {
				if (!line.contains("Video:")) continue;
				Matcher m = VIDEO_SIZE.matcher(line);
				if (m.find()) {
					int w = Integer.parseInt(m.group(1));
					int h = Integer.parseInt(m.group(2));
					if (w > 0 && h > 0) return new int[]{w, h};
				}
			}
		} catch (Exception ignored) {
		}
		// Safe fallback for vertical shorts.
		return new int[]{1080, 1920};
	}

	private boolean probeHasAudio(Path input) {
		try {
			var r = ffmpegRunner.runRaw(List.of("-hide_banner", "-i", input.toString()), 30, java.util.concurrent.TimeUnit.SECONDS);
			String out = r.output() == null ? "" : r.output();
			for (String line : out.split("\\R")) {
				if (line.contains("Audio:")) {
					return true;
				}
			}
			return false;
		} catch (Exception ignored) {
			return false;
		}
	}

	private record ExportSegmentPlan(List<double[]> segments, double safeSpeed, double exportedDuration) {
		boolean multi() {
			return segments.size() >= 2;
		}
	}

	private ExportSegmentPlan buildExportSegmentPlan(JsonNode payload, double trimStart, double trimEnd, double speed) {
		double safeSpeed = Math.abs(speed) < 0.0001d ? 1d : speed;
		double rawDuration = readNumber(payload, "duration", 0d);
		List<double[]> segs = parseVideoTimelineKeepSegments(payload, rawDuration, trimStart, trimEnd);
		double sumLen = 0d;
		for (double[] s : segs) {
			sumLen += Math.max(0d, s[1] - s[0]);
		}
		double exportedDuration = Math.max(0.01d, sumLen / safeSpeed);
		return new ExportSegmentPlan(List.copyOf(segs), safeSpeed, exportedDuration);
	}

	private static List<double[]> mergeOverlappingKeepSegments(List<double[]> sorted) {
		List<double[]> out = new ArrayList<>();
		for (double[] s : sorted) {
			if (s[1] <= s[0] + 1e-6d) {
				continue;
			}
			if (out.isEmpty()) {
				out.add(new double[]{s[0], s[1]});
				continue;
			}
			double[] last = out.get(out.size() - 1);
			if (s[0] <= last[1] + 1e-4d) {
				last[1] = Math.max(last[1], s[1]);
			} else {
				out.add(new double[]{s[0], s[1]});
			}
		}
		return out;
	}

	private List<double[]> parseVideoTimelineKeepSegments(JsonNode payload, double duration, double trimStart, double trimEnd) {
		List<double[]> raw = new ArrayList<>();
		JsonNode arr = payload.path("videoTimelineSegments");
		if (arr != null && arr.isArray() && !arr.isEmpty()) {
			double dMax = Math.max(0d, duration);
			for (JsonNode el : arr) {
				double st = readNumber(el, "startTime", 0d);
				double en = readNumber(el, "endTime", 0d);
				if (en <= st + 1e-6d) {
					continue;
				}
				st = Math.max(0d, st);
				if (dMax > 1e-6d) {
					en = Math.min(dMax, en);
				}
				if (en > st + 1e-6d) {
					raw.add(new double[]{st, en});
				}
			}
			raw.sort(Comparator.comparingDouble(a -> a[0]));
			raw = mergeOverlappingKeepSegments(raw);
		}
		if (raw.isEmpty()) {
			double t0 = Math.max(0d, trimStart);
			double t1 = trimEnd > t0 ? trimEnd : (duration > 1e-6d ? duration : t0);
			if (t1 <= t0 + 1e-6d && duration > 1e-6d) {
				t1 = duration;
			}
			raw.add(new double[]{t0, Math.max(t0 + 1e-3d, t1)});
		}
		return raw;
	}

	private double sourceTimeToExport(ExportSegmentPlan plan, double sourceTime) {
		double t = sourceTime;
		double acc = 0d;
		for (double[] s : plan.segments()) {
			if (t <= s[0] + 1e-9d) {
				return acc;
			}
			if (t < s[1] - 1e-9d) {
				return acc + (t - s[0]) / plan.safeSpeed();
			}
			acc += (s[1] - s[0]) / plan.safeSpeed();
		}
		return acc;
	}

	private String overlayEnableFromSourceWindow(ExportSegmentPlan plan, double sourceStart, double sourceEnd, double exportedDuration) {
		List<String> terms = new ArrayList<>();
		for (double[] s : plan.segments()) {
			double cs = Math.max(sourceStart, s[0]);
			double ce = Math.min(sourceEnd, s[1]);
			if (ce <= cs + 1e-6d) {
				continue;
			}
			double os = sourceTimeToExport(plan, cs);
			double oe = sourceTimeToExport(plan, ce);
			oe = Math.min(exportedDuration, oe);
			if (oe <= os + 1e-6d) {
				continue;
			}
			terms.add("between(t," + formatDecimal(os) + "," + formatDecimal(oe) + ")");
		}
		if (terms.isEmpty()) {
			return null;
		}
		return String.join("+", terms);
	}

	private static void logFontSelection(String ffmpegOut) {
		if (ffmpegOut == null || ffmpegOut.isBlank()) return;
		// libass prints font selection lines in verbose mode.
		String[] lines = ffmpegOut.split("\\R");
		StringBuilder sb = new StringBuilder();
		for (String line : lines) {
			String l = line.trim();
			if (l.isEmpty()) continue;
			if (l.contains("fontselect")
					|| l.contains("Using font")
					|| l.contains("font provider")
					|| l.contains("libass")
					|| l.contains("Loading font file")) {
				sb.append(l).append('\n');
			}
		}
		if (!sb.isEmpty()) {
			// Trim to avoid huge logs.
			String out = sb.toString();
			if (out.length() > 4000) out = out.substring(0, 4000);
			org.slf4j.LoggerFactory.getLogger(WorkspaceExportService.class).info("FFmpeg/libass font debug:\n{}", out);
		}
	}

	private static Path writeFontConfig(Path workDir, String fontsDir) throws IOException {
		Path conf = workDir.resolve("fonts.conf");
		// Minimal fontconfig pointing at the extracted font directory.
		// This makes libass reliably pick our bundled Myanmar Unicode font instead of system fallbacks.
		String xml = """
				<?xml version="1.0"?>
				<!DOCTYPE fontconfig SYSTEM "fonts.dtd">
				<fontconfig>
				  <dir>%s</dir>
				  <cachedir>%s</cachedir>
				  <config>
				    <rescan>
				      <int>30</int>
				    </rescan>
				  </config>
				</fontconfig>
				""".formatted(escapeXml(fontsDir), escapeXml(workDir.resolve("fc-cache").toString()));
		Files.writeString(conf, xml);
		return conf;
	}

	private static String escapeXml(String v) {
		if (v == null) return "";
		return v.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
				.replace("\"", "&quot;").replace("'", "&apos;");
	}

	private String buildFilterComplex(
			JsonNode payload,
			ExportSegmentPlan plan,
			List<ImageInput> images,
			Path srtFile,
			Path workDir,
			int videoW,
			int videoH,
			boolean buildConcatAudioInGraph,
			double originalVolume
	) {
		List<String> parts = new ArrayList<>();
		double safeSpeed = plan.safeSpeed();
		double exportedDuration = plan.exportedDuration();
		double rawDuration = readNumber(payload, "duration", 0d);

		double baseScaleX = readNumber(payload.path("displayToNaturalScale"), "x", 1d);
		double baseScaleY = readNumber(payload.path("displayToNaturalScale"), "y", 1d);
		double layerScaleX = baseScaleX;
		double layerScaleY = baseScaleY;
		double layerOffsetX = 0d;
		double layerOffsetY = 0d;
		int outputVideoW = videoW;
		int outputVideoH = videoH;

		String current;
		if (plan.multi()) {
			List<String> vLabels = new ArrayList<>();
			List<double[]> segs = plan.segments();
			int n = segs.size();
			for (int i = 0; i < n; i++) {
				double a = segs.get(i)[0];
				double b = segs.get(i)[1];
				String vLab = "vx" + i;
				parts.add("[0:v]trim=start=" + formatDecimal(a) + ":end=" + formatDecimal(b)
						+ ",setpts=PTS-STARTPTS,setpts=" + formatDecimal(1d / safeSpeed) + "*PTS[" + vLab + "]");
				vLabels.add("[" + vLab + "]");
			}
			parts.add(String.join("", vLabels) + "concat=n=" + n + ":v=1:a=0[vcat]");
			current = "vcat";
			if (buildConcatAudioInGraph) {
				List<String> aLabels = new ArrayList<>();
				for (int i = 0; i < n; i++) {
					double a = segs.get(i)[0];
					double b = segs.get(i)[1];
					String aLab = "ax" + i;
					String tempoChain = buildAtempoFilter(safeSpeed);
					parts.add("[0:a]atrim=start=" + formatDecimal(a) + ":end=" + formatDecimal(b)
							+ ",asetpts=PTS-STARTPTS," + tempoChain + "[" + aLab + "]");
					aLabels.add("[" + aLab + "]");
				}
				parts.add(String.join("", aLabels) + "concat=n=" + n + ":v=0:a=1[acat]");
				parts.add("[acat]volume=" + formatDecimal(Math.max(0d, originalVolume)) + "[aout]");
			}
		} else {
			current = "v0";
			parts.add("[0:v]setpts=" + formatDecimal(1d / safeSpeed) + "*PTS[" + current + "]");
		}

		boolean protectFlip = readBoolean(payload, "protectFlip", false);
		double protectHueDeg = readNumber(payload, "protectHueDeg", 0d);
		if (protectFlip || Math.abs(protectHueDeg) > 0.0001d) {
			String next = "vp0";
			List<String> ops = new ArrayList<>();
			if (protectFlip) {
				ops.add("hflip");
			}
			if (Math.abs(protectHueDeg) > 0.0001d) {
				ops.add("hue=h=" + formatDecimal(protectHueDeg));
			}
			parts.add("[" + current + "]" + String.join(",", ops) + "[" + next + "]");
			current = next;
		}

		// Respect workspace aspect (e.g. 9:16 portrait) for export output.
		// We mimic preview's cover behavior by center-cropping the source to the selected canvas ratio.
		double sourceAspect = videoH > 0 ? (double) videoW / (double) videoH : 1d;
		double targetAspect = resolveTargetAspectRatio(payload, sourceAspect);
		if (Math.abs(sourceAspect - targetAspect) > 0.0001d && videoW > 1 && videoH > 1) {
			int cropW = videoW;
			int cropH = videoH;
			int cropX = 0;
			int cropY = 0;
			if (sourceAspect > targetAspect) {
				cropW = evenAtLeast2((int) Math.floor(videoH * targetAspect));
				cropW = Math.min(cropW, evenAtLeast2(videoW));
				cropX = evenNonNegative((videoW - cropW) / 2);
			} else {
				cropH = evenAtLeast2((int) Math.floor(videoW / targetAspect));
				cropH = Math.min(cropH, evenAtLeast2(videoH));
				cropY = evenNonNegative((videoH - cropH) / 2);
			}
			outputVideoW = cropW;
			outputVideoH = cropH;
			layerScaleX = baseScaleX * ((double) cropW / (double) videoW);
			layerScaleY = baseScaleY * ((double) cropH / (double) videoH);
			layerOffsetX = -cropX;
			layerOffsetY = -cropY;
			String next = "vaspect";
			parts.add("[" + current + "]crop=" + cropW + ":" + cropH + ":" + cropX + ":" + cropY + "[" + next + "]");
			current = next;
		}

		// Viral-style text burn-in: write text layers to ASS and burn with complex shaping.
		Path textLayersAss = maybeWriteTextLayersAss(
				payload, workDir, plan,
				layerScaleX, layerScaleY, layerOffsetX, layerOffsetY,
				outputVideoW, outputVideoH
		);
		if (textLayersAss != null) {
			String next = "vtass";
			String escapedTextAss = escapePathForFfmpegFilter(textLayersAss.toString());
			String textAssFilter = "ass='" + escapedTextAss + "':shaping=complex:original_size=" + outputVideoW + "x" + outputVideoH;
			String textFontsDir = "";
			if (workDir != null) {
				textFontsDir = extractBundledFontDir(workDir);
			}
			if (textFontsDir.isBlank()) {
				textFontsDir = processingProperties.getSubtitlesFontsDir() == null ? "" : processingProperties.getSubtitlesFontsDir().trim();
			}
			if (!textFontsDir.isBlank()) {
				textAssFilter = textAssFilter + ":fontsdir='" + escapePathForFfmpegFilter(textFontsDir) + "'";
			} else {
				log.warn("[workspace-export] text-layer ASS burn-in has no fontsdir (bundled font not extracted); libass may omit glyphs if the face is not on the system font path");
			}
			parts.add("[" + current + "]" + textAssFilter + "[" + next + "]");
			current = next;
		}

		for (int i = 0; i < images.size(); i++) {
			ImageInput imageInput = images.get(i);
			JsonNode layer = imageInput.layer();
			double srcStart = readNumber(layer, "startTime", 0d);
			double srcEnd = readNumber(layer, "endTime", rawDuration > 0 ? rawDuration : 86400d);
			String enable = overlayEnableFromSourceWindow(plan, srcStart, srcEnd, exportedDuration);
			if (enable == null) {
				continue;
			}
			int width = Math.max(2, (int) Math.round(readNumber(layer, "width", 100d) * layerScaleX));
			int height = Math.max(2, (int) Math.round(readNumber(layer, "height", 100d) * layerScaleY));
			int x = (int) Math.round(readNumber(layer, "x", 0d) * layerScaleX + layerOffsetX);
			int y = (int) Math.round(readNumber(layer, "y", 0d) * layerScaleY + layerOffsetY);
			double opacity = Math.max(0d, Math.min(1d, readNumber(layer, "opacity", 1d)));
			boolean flipX = readBoolean(layer, "flipX", false);
			boolean flipY = readBoolean(layer, "flipY", false);
			double rotation = Math.toRadians(readNumber(layer, "rotation", 0d));

			List<String> imageOps = new ArrayList<>();
			if (imageInput.loopInput()) {
				// Animated GIF/WebP-style alpha: normalize to rgba before scale/overlay.
				imageOps.add("format=rgba");
			}
			imageOps.add("scale=" + width + ":" + height);
			if (flipX) {
				imageOps.add("hflip");
			}
			if (flipY) {
				imageOps.add("vflip");
			}
			if (Math.abs(rotation) > 0.0001d) {
				imageOps.add("rotate=" + formatDecimal(rotation) + ":fillcolor=none");
			}
			if (opacity < 0.9999d) {
				imageOps.add("format=rgba");
				imageOps.add("colorchannelmixer=aa=" + formatDecimal(opacity));
			}
			String imgLabel = "img" + i;
			parts.add("[" + (i + 1) + ":v]" + String.join(",", imageOps) + "[" + imgLabel + "]");
			String next = "vi" + i;
			parts.add("[" + current + "][" + imgLabel + "]overlay=x=" + x + ":y=" + y
					+ ":enable='" + enable + "'"
					+ "[" + next + "]");
			current = next;
		}

		if (srtFile != null) {
			String next = "vsrt";
			// Convert SRT -> ASS and burn via ass filter with complex shaping for Myanmar.
			Path assFile = maybeConvertSrtToAss(srtFile, workDir, payload, outputVideoW, outputVideoH);
			Path burnFile = assFile != null ? assFile : srtFile;
			String escaped = burnFile.toString().replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'");
			// Prefer bundled font dir for deterministic production output.
			String fontsDir = "";
			if (workDir != null) {
				fontsDir = extractBundledFontDir(workDir);
			}
			if (fontsDir.isBlank()) {
				fontsDir = processingProperties.getSubtitlesFontsDir() == null ? "" : processingProperties.getSubtitlesFontsDir().trim();
			}
			String fontName = processingProperties.getSubtitlesFontName() == null ? "" : processingProperties.getSubtitlesFontName().trim();
			if (fontName.isBlank()) {
				// Bundled default (resources/fonts/NotoSerifMyanmar.ttf)
				fontName = "Noto Serif Myanmar";
			}
			int fontSize = resolveSubtitleBurnFontSize(payload, videoW, videoH);
			int marginV = Math.max(0, Math.min(300, processingProperties.getSubtitlesMarginV()));
			int bgOpacity = readInt(payload, "subtitlesBackgroundOpacity", 65);
			String backColour = assBackColourBlackFromOpacityPercent(bgOpacity);
			int boxOutline = subtitleBoxOutlinePx(fontSize);
			boolean captionBox = bgOpacity > 0;
			String primaryAss = assOpaquePrimaryFromWebHex(readText(payload, "subtitlesPrimaryColor", "#FFFFFF"));
			// Force Unicode decoding and prefer a Myanmar Unicode-capable font.
			// Note: libass uses system fontconfig; if the font isn't installed, it will fall back.
			String style = "FontName=" + escapeAss(fontName)
					+ ",FontSize=" + fontSize
					+ ",PrimaryColour=" + primaryAss;
			if (captionBox) {
				style = style
						// BorderStyle=3: box fill uses BackColour; Outline sets box padding (must be >0).
						+ ",BorderStyle=3"
						+ ",BackColour=" + backColour
						+ ",Outline=" + boxOutline
						+ ",Shadow=0"
						+ ",Alignment=2"
						+ ",MarginV=" + marginV;
			} else {
				// Plain fill only (no caption box); BorderStyle=3 + transparent BackColour still draws a padded box in libass.
				style = style
						+ ",BorderStyle=1"
						+ ",BackColour=&HFF000000"
						+ ",OutlineColour=&H00000000"
						+ ",Outline=0"
						+ ",Shadow=0"
						+ ",Alignment=2"
						+ ",MarginV=" + marginV;
			}

			String filter;
			if (burnFile.toString().toLowerCase(Locale.ROOT).endsWith(".ass")) {
				filter = "ass='" + escaped + "':shaping=complex:original_size=" + outputVideoW + "x" + outputVideoH;
			} else {
				filter = "subtitles='" + escaped + "':charenc=UTF-8:wrap_unicode=1:force_style='"
						+ escapeAmpersandsForFfmpegFilterOption(style) + "'";
			}
			if (!fontsDir.isBlank()) {
				String fd = fontsDir.replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'");
				filter = filter + ":fontsdir='" + fd + "'";
			}
			parts.add("[" + current + "]" + filter + "[" + next + "]");
			current = next;
		}

		parts.add("[" + current + "]format=yuv420p[vout]");
		return String.join(";", parts);
	}

	private Path maybeConvertSrtToAss(Path srtFile, Path workDir, JsonNode payload, int videoW, int videoH) {
		try {
			if (srtFile == null || workDir == null) return null;
			if (!srtFile.toString().toLowerCase(Locale.ROOT).endsWith(".srt")) return null;
			Path ass = workDir.resolve("burned-subtitles.ass");
			// Convert with ffmpeg so timings and formatting are handled consistently.
			ffmpegRunner.run(List.of("-y", "-hide_banner", "-loglevel", "error", "-i", srtFile.toString(), "-f", "ass", ass.toString()));

			// Rewrite ASS script + style to use our configured font + size + margins,
			// and apply optional user-position via \pos(x,y).
			String fontName = processingProperties.getSubtitlesFontName() == null ? "" : processingProperties.getSubtitlesFontName().trim();
			if (fontName.isBlank()) {
				fontName = "Noto Serif Myanmar";
			}
			int fontSize = resolveSubtitleBurnFontSize(payload, videoW, videoH);
			int marginV = Math.max(0, Math.min(300, processingProperties.getSubtitlesMarginV()));
			int bgOpacity = readInt(payload, "subtitlesBackgroundOpacity", 65);
			String backColour = assBackColourBlackFromOpacityPercent(bgOpacity);
			int boxOutline = subtitleBoxOutlinePx(fontSize);
			boolean captionBox = bgOpacity > 0;
			String primaryAss = assOpaquePrimaryFromWebHex(readText(payload, "subtitlesPrimaryColor", "#FFFFFF"));

			double posX = readNumber(payload.path("subtitlesPosition"), "x", -1d);
			double posY = readNumber(payload.path("subtitlesPosition"), "y", -1d);
			boolean hasPos = posX >= 0d && posX <= 1d && posY >= 0d && posY <= 1d && videoW > 0 && videoH > 0;
			int px = hasPos ? (int) Math.round(posX * videoW) : 0;
			int py = hasPos ? (int) Math.round(posY * videoH) : 0;

			String content = Files.readString(ass);
			String[] lines = content.split("\\R", -1);
			StringBuilder out = new StringBuilder(content.length());
			for (String line : lines) {
				if (line.startsWith("PlayResX:")) {
					line = "PlayResX: " + Math.max(2, videoW);
				} else if (line.startsWith("PlayResY:")) {
					line = "PlayResY: " + Math.max(2, videoH);
				}
				String lineNorm = line.replace("\uFEFF", "").stripLeading();
				if (lineNorm.regionMatches(true, 0, "Style: Default,", 0, "Style: Default,".length())) {
					// ASS V4+ fields: Fontname, Fontsize, Primary, Secondary, OutlineColour, BackColour, ...
					// then BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
					String[] parts = lineNorm.split(",", -1);
					if (parts.length >= 23) {
						parts[1] = escapeAssField(fontName);
						parts[2] = String.valueOf(fontSize);
						parts[3] = primaryAss;
						parts[4] = "&H00000000";
						parts[5] = "&H00000000";
						if (captionBox) {
							parts[6] = backColour;
							parts[15] = "3";
							parts[16] = String.valueOf(boxOutline);
						} else {
							parts[6] = "&HFF000000";
							parts[15] = "1";
							parts[16] = "0";
						}
						parts[17] = "0";
						parts[18] = hasPos ? "5" : "2"; // an5 + \pos vs bottom-center
						parts[21] = String.valueOf(marginV);
						line = String.join(",", parts);
					}
				}

				if (hasPos && line.startsWith("Dialogue:")) {
					int idx = line.lastIndexOf(',');
					if (idx > 0 && idx < line.length() - 1) {
						String head = line.substring(0, idx + 1);
						String txt = line.substring(idx + 1);
						String tag = "{\\\\pos(" + px + "," + py + ")\\\\an5}";
						if (!txt.startsWith("{")) {
							line = head + tag + txt;
						} else {
							// If it already has tags, prepend ours to keep it deterministic.
							line = head + tag + txt;
						}
					}
				}
				out.append(line).append('\n');
			}
			Files.writeString(ass, out.toString());
			return ass;
		} catch (Exception ex) {
			log.warn("[workspace-export] SRT→ASS rewrite failed; falling back to subtitles filter: {}", ex.toString());
			return null;
		}
	}

	private static String escapeAssField(String v) {
		if (v == null) return "";
		// Style fields are comma-separated; avoid commas.
		return v.replace(",", " ");
	}

	private Path maybeWriteTextLayersAss(
			JsonNode payload,
			Path workDir,
			ExportSegmentPlan plan,
			double scaleX,
			double scaleY,
			double offsetX,
			double offsetY,
			int videoW,
			int videoH
	) {
		try {
			if (payload == null || payload.isNull() || workDir == null) return null;
			JsonNode textLayers = payload.path("textLayers");
			if (!textLayers.isArray() || textLayers.isEmpty()) return null;

			String fontName = processingProperties.getSubtitlesFontName() == null ? "" : processingProperties.getSubtitlesFontName().trim();
			if (fontName.isBlank()) fontName = "Pyidaungsu";
			int marginV = Math.max(0, Math.min(300, processingProperties.getSubtitlesMarginV()));

			StringBuilder events = new StringBuilder();
			int emitted = 0;
			for (JsonNode layer : textLayers) {
				// Do not trim internal newlines (SRT multi-line cues); only skip truly empty cues.
				String rawContent = layer.path("content").asText("");
				String normalized;
				try {
					normalized = ensureUnicodeMyanmar(rawContent);
				} catch (Exception ex) {
					log.warn("[workspace-export] ensureUnicodeMyanmar failed for a text layer; using raw content: {}", ex.toString());
					normalized = rawContent;
				}
				// If conversion blanked mixed script, keep raw so export still shows Latin / numbers.
				String content = (normalized != null && !normalized.isBlank()) ? normalized : rawContent;
				if (content == null || content.isBlank()) {
					continue;
				}
				double layerStart = readNumber(layer, "startTime", 0d);
				double layerEnd = readNumber(layer, "endTime", readNumber(payload, "duration", 86400d));

				// Preview: TextLayer centers glyphs in [x,y,width,height] (Rnd box). ASS must use the same anchor:
				// middle-center + pos at box center, not top-left (\an7), or export position/size diverge from editor.
				double boxX = readNumber(layer, "x", 0d);
				double boxY = readNumber(layer, "y", 0d);
				double boxW = readNumber(layer, "width", 0d);
				double boxH = readNumber(layer, "height", 0d);
				double cx = (boxX + Math.max(0d, boxW) / 2d) * scaleX + offsetX;
				double cy = (boxY + Math.max(0d, boxH) / 2d) * scaleY + offsetY;
				int posX = (int) Math.round(cx);
				int posY = (int) Math.round(cy);
				double sFont = Math.sqrt(Math.max(1e-18d, scaleX * scaleY));
				int fontSize = (int) Math.round(
						Math.max(10d, readNumber(layer, "fontSize", 24d) * sFont * SUBTITLE_PREVIEW_TO_BURN_FONT_FACTOR));
				fontSize = Math.min(280, fontSize);
				double opacity = Math.max(0d, Math.min(1d, readNumber(layer, "opacity", 100d) / 100d));
				int alpha = toAssAlpha(opacity);
				String color = toAssPrimaryColor(readText(layer, "color", "#FFFFFF"));
				String tags = "{\\an5\\pos(" + posX + "," + posY + ")\\fs" + fontSize + "\\1c" + color
						+ "\\1a&H" + String.format(Locale.US, "%02X", alpha) + "&}";
				String text = escapeAssDialogueText(content);
				for (double[] s : plan.segments()) {
					double cs = Math.max(layerStart, s[0]);
					double ce = Math.min(layerEnd, s[1]);
					if (ce <= cs + 1e-6d) {
						continue;
					}
					double start = sourceTimeToExport(plan, cs);
					double end = sourceTimeToExport(plan, ce);
					end = Math.min(plan.exportedDuration(), end);
					if (end <= start + 1e-4d) {
						continue;
					}
					events.append("Dialogue: 0,")
							.append(formatAssTime(start))
							.append(",")
							.append(formatAssTime(end))
							.append(",Default,,0,0,0,,")
							.append(tags)
							.append(text)
							.append('\n');
					emitted++;
				}
			}
			if (emitted == 0) {
				log.warn("[workspace-export] textLayers array non-empty but no dialogue lines emitted (check timing vs trim, or blank content)");
				return null;
			}

			StringBuilder ass = new StringBuilder();
			// BorderStyle=3 with Outline=0 often yields invisible libass output; use outline style so burns match preview.
			ass.append("[Script Info]\n")
					.append("ScriptType: v4.00+\n")
					.append("PlayResX: ").append(Math.max(2, videoW)).append('\n')
					.append("PlayResY: ").append(Math.max(2, videoH)).append('\n')
					.append("WrapStyle: 0\n")
					.append("ScaledBorderAndShadow: yes\n\n")
					.append("[V4+ Styles]\n")
					.append("Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n")
					.append("Style: Default,").append(escapeAssField(fontName)).append(",24,&H00FFFFFF,&H000000FF,&H00000000,&H80000000,0,0,0,0,100,100,0,0,1,2,0,7,0,0,")
					.append(marginV)
					.append(",1\n\n")
					.append("[Events]\n")
					.append("Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n")
					.append(events);

			Path assFile = workDir.resolve("workspace-text-layers.ass");
			Files.writeString(assFile, ass.toString());
			return assFile;
		} catch (Exception e) {
			log.warn("[workspace-export] failed to build text-layer ASS", e);
			return null;
		}
	}

	private Path maybeWriteSrtFile(JsonNode payload, Path workDir) throws IOException {
		if (payload == null || payload.isNull()) return null;
		boolean burn = readBoolean(payload, "burnSubtitles", false);
		if (!burn) return null;
		String srtText = readText(payload, "subtitlesSrtText", "");
		if (srtText == null || srtText.trim().isEmpty()) return null;
		if (processingProperties.isSubtitlesAutoConvertZawgyi()) {
			srtText = ensureUnicodeMyanmar(srtText);
		}
		Path srt = workDir.resolve("burned-subtitles.srt");
		Files.writeString(srt, srtText);
		return srt;
	}

	private static String ensureUnicodeMyanmar(String text) {
		if (text == null || text.isBlank()) return text;
		// Detect/convert line-by-line. Running detection on the whole SRT (timestamps + English)
		// can dilute the detector score and miss mixed-encoding subtitles.
		StringBuilder out = new StringBuilder(text.length());
		String[] lines = text.split("\\R", -1);
		for (int i = 0; i < lines.length; i++) {
			String line = lines[i];
			out.append(ensureUnicodeMyanmarLine(line));
			if (i < lines.length - 1) out.append('\n');
		}
		return out.toString();
	}

	private static String ensureUnicodeMyanmarLine(String line) {
		if (line == null || line.isBlank()) return line;
		if (!MYANMAR_CHARS.matcher(line).find()) return line;

		// Score only the Myanmar-containing parts to avoid timestamp/latin dilution.
		String my = line.replaceAll("[^\\u1000-\\u109F\\uAA60-\\uAA7F]+", " ").trim();
		if (my.isBlank()) return line;

		double zawgyiProb = ZAWGYI_DETECTOR.getZawgyiProbability(my);
		// Lower threshold: better to convert than to ship broken burn-in.
		if (zawgyiProb >= 0.20d) {
			try {
				String converted = Z2U.convert(line);
				return converted != null && !converted.isBlank() ? converted : line;
			} catch (Exception ex) {
				org.slf4j.LoggerFactory.getLogger(WorkspaceExportService.class)
						.warn("[workspace-export] Zawgyi→Unicode conversion failed for a line; using original: {}", ex.toString());
				return line;
			}
		}
		return line;
	}

	private static String escapeAss(String v) {
		if (v == null) return "";
		// ASS force_style string is single-quoted in ffmpeg filter; escape quotes/backslashes.
		return v.replace("\\", "\\\\").replace("'", "\\'");
	}

	private static String escapePathForFfmpegFilter(String path) {
		if (path == null) {
			return "";
		}
		return path.replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'");
	}

	private static int evenAtLeast2(int value) {
		int v = Math.max(2, value);
		return (v & 1) == 0 ? v : v - 1;
	}

	private static int evenNonNegative(int value) {
		int v = Math.max(0, value);
		return (v & 1) == 0 ? v : v - 1;
	}

	private double resolveTargetAspectRatio(JsonNode payload, double fallbackAspect) {
		double fromCrop = readNumber(payload.path("crop"), "easyAspect", fallbackAspect);
		if (Double.isFinite(fromCrop) && fromCrop > 0.01d && fromCrop < 100d) {
			return fromCrop;
		}
		double cw = readNumber(payload.path("canvasFrame"), "width", 0d);
		double ch = readNumber(payload.path("canvasFrame"), "height", 0d);
		if (Double.isFinite(cw) && Double.isFinite(ch) && cw > 1d && ch > 1d) {
			return cw / ch;
		}
		return fallbackAspect > 0 ? fallbackAspect : 1d;
	}

	private String extractBundledFontDir(Path workDir) {
		try {
			String resPath = processingProperties.getSubtitlesFontResource() == null
					? ""
					: processingProperties.getSubtitlesFontResource().trim();
			if (resPath.isBlank()) {
				return "";
			}
			Path fonts = workDir.resolve("fonts");
			Files.createDirectories(fonts);
			copyFontResourceIfExists(resPath, fonts, "subtitle-font.ttf");
			// If the configured font is a face inside the same family, also extract common siblings.
			// libass may request a specific face (e.g. Regular) when resolving FontName.
			if (resPath.startsWith("fonts/") && resPath.endsWith(".ttf")) {
				String base = resPath.substring(0, resPath.length() - ".ttf".length());
				copyFontResourceIfExists(base + "-Regular.ttf", fonts, null);
				copyFontResourceIfExists(base + "-Bold.ttf", fonts, null);
				copyFontResourceIfExists(base + "-Italic.ttf", fonts, null);
				copyFontResourceIfExists(base + "-BoldItalic.ttf", fonts, null);
			}
			return fonts.toString();
		} catch (Exception e) {
			return "";
		}
	}

	private static void copyFontResourceIfExists(String classpathPath, Path outDir, String fallbackName) throws IOException {
		ClassPathResource res = new ClassPathResource(classpathPath);
		if (!res.exists()) {
			return;
		}
		String name = Path.of(classpathPath).getFileName().toString();
		if (name == null || name.isBlank()) {
			name = fallbackName == null ? "subtitle-font.ttf" : fallbackName;
		}
		Path out = outDir.resolve(name);
		try (InputStream in = res.getInputStream()) {
			Files.copy(in, out, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
		}
	}

	private List<ImageInput> collectImageInputs(JsonNode payload, Path workDir) throws IOException, InterruptedException {
		List<ImageInput> out = new ArrayList<>();
		JsonNode imageLayers = payload.path("imageLayers");
		if (!imageLayers.isArray()) {
			return out;
		}
		List<JsonNode> layers = new ArrayList<>();
		for (JsonNode layer : imageLayers) {
			layers.add(layer);
		}
		if (layers.isEmpty()) return out;

		int workers = Math.max(1, processingProperties.getWorkspaceExportImagePrepThreads());
		workers = Math.min(workers, Math.max(1, layers.size()));
		ExecutorService pool = Executors.newFixedThreadPool(workers);
		try {
			List<CompletableFuture<ImageInput>> futures = new ArrayList<>();
			for (int i = 0; i < layers.size(); i++) {
				final int idx = i;
				final JsonNode layer = layers.get(i);
				futures.add(CompletableFuture.supplyAsync(() -> prepareImageInput(layer, idx, workDir), pool));
			}
			for (CompletableFuture<ImageInput> f : futures) {
				try {
					ImageInput input = f.get();
					if (input != null) {
						out.add(input);
					}
				} catch (Exception e) {
					// Keep export resilient: image overlay failures should not fail the entire export.
					log.warn("Skipping one image overlay after async preparation failure", e);
				}
			}
		} finally {
			pool.shutdown();
			if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
				pool.shutdownNow();
			}
		}
		return out;
	}

	private ImageInput prepareImageInput(JsonNode layer, int index, Path workDir) {
		String src = ObjectStorageTransferService.stripUrlFragmentForDownload(readText(layer, "src", ""));
		if (src == null || src.isBlank()) {
			return null;
		}
		try {
			Path downloaded = objectStorageTransferService.download(src, workDir);
			boolean animatedGif = isLikelyAnimatedGif(src, downloaded);
			if (animatedGif) {
				Path normalized = workDir.resolve("layer-image-" + index + ".gif");
				Files.copy(downloaded, normalized, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
				return new ImageInput(layer, normalized, true);
			}
			Path normalized = workDir.resolve("layer-image-" + index + ".png");
			ffmpegRunner.run(List.of("-y", "-i", downloaded.toString(), "-frames:v", "1", normalized.toString()));
			return new ImageInput(layer, normalized, false);
		} catch (Exception e) {
			// Many external hosts (e.g. Pinterest) block non-browser fetches. Skip overlay instead of failing export.
			log.warn("Skipping image overlay; could not download or decode: {}", src, e);
			return null;
		}
	}

	/**
	 * If we flatten these to a single PNG, export loses animation; keep as GIF and loop in filtergraph.
	 */
	private static boolean isLikelyAnimatedGif(String srcUrl, Path downloadedFile) {
		if (srcUrl != null && srcUrl.toLowerCase(Locale.ROOT).contains(".gif")) {
			return true;
		}
		String name = downloadedFile.getFileName().toString().toLowerCase(Locale.ROOT);
		return name.endsWith(".gif");
	}

	private String readRequiredText(JsonNode payload, String field) {
		JsonNode node = payload.get(field);
		if (node == null || !node.isTextual() || node.asText().isBlank()) {
			throw new ResponseStatusException(BAD_REQUEST, field + " is required");
		}
		return node.asText().trim();
	}

	private String readText(JsonNode payload, String field, String defaultValue) {
		JsonNode node = payload.get(field);
		if (node == null || node.isNull()) {
			return defaultValue;
		}
		if (node.isTextual()) {
			String text = node.asText().trim();
			return text.isEmpty() ? defaultValue : text;
		}
		return defaultValue;
	}

	private boolean readBoolean(JsonNode payload, String field, boolean defaultValue) {
		JsonNode node = payload.get(field);
		if (node == null || node.isNull()) {
			return defaultValue;
		}
		if (node.isBoolean()) {
			return node.asBoolean();
		}
		if (node.isTextual()) {
			String raw = node.asText().trim();
			if ("true".equalsIgnoreCase(raw)) {
				return true;
			}
			if ("false".equalsIgnoreCase(raw)) {
				return false;
			}
		}
		return defaultValue;
	}

	private double readNumber(JsonNode payload, String field, double defaultValue) {
		JsonNode node = payload.get(field);
		if (node == null || node.isNull()) {
			return defaultValue;
		}
		if (node.isNumber()) {
			return node.asDouble();
		}
		if (node.isTextual()) {
			try {
				return Double.parseDouble(node.asText().trim());
			} catch (NumberFormatException ignored) {
				return defaultValue;
			}
		}
		return defaultValue;
	}

	private int readInt(JsonNode payload, String field, int defaultValue) {
		JsonNode node = payload.get(field);
		if (node == null || node.isNull()) {
			return defaultValue;
		}
		if (node.isInt() || node.isLong() || node.isNumber()) {
			return node.asInt(defaultValue);
		}
		if (node.isTextual()) {
			try {
				return Integer.parseInt(node.asText().trim());
			} catch (NumberFormatException ignored) {
				return defaultValue;
			}
		}
		return defaultValue;
	}

	/**
	 * When the client sends preview canvas metrics, map CSS preview font size to libass {@code FontSize}
	 * for the probed output frame:
	 * {@code round(max(10, previewFontPx * max(outW/canvasW, outH/canvasH) * SUBTITLE_PREVIEW_TO_BURN_FONT_FACTOR))}.
	 * Otherwise use legacy {@code subtitlesFontSize} (clamped 14–96).
	 */
	private int resolveSubtitleBurnFontSize(JsonNode payload, int outputVideoW, int outputVideoH) {
		double previewFontPx = readNumber(payload, "subtitlesPreviewFontPx", -1d);
		int canvasW = readInt(payload, "subtitlesPreviewCanvasW", -1);
		int canvasH = readInt(payload, "subtitlesPreviewCanvasH", -1);
		if (previewFontPx > 0d && canvasW > 0 && canvasH > 0 && outputVideoW > 0 && outputVideoH > 0) {
			double scaleX = outputVideoW / (double) canvasW;
			double scaleY = outputVideoH / (double) canvasH;
			double m = Math.max(scaleX, scaleY);
			int mapped = (int) Math.round(Math.max(10d, previewFontPx * m * SUBTITLE_PREVIEW_TO_BURN_FONT_FACTOR));
			return Math.min(280, Math.max(10, mapped));
		}
		int legacy = readInt(payload, "subtitlesFontSize", processingProperties.getSubtitlesFontSize());
		return Math.max(14, Math.min(96, legacy));
	}

	private String buildAtempoFilter(double speed) {
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
		chain.add("atempo=" + formatDecimal(remaining));
		return String.join(",", chain);
	}

	/**
	 * ASS {@code PrimaryColour} for opaque glyphs from CSS-style {@code #RRGGBB}. Format {@code &HAABBGGRR}
	 * with {@code AA=00} opaque (libass / VSFilter).
	 */
	private static String assOpaquePrimaryFromWebHex(String raw) {
		String hex = raw == null ? "FFFFFF" : raw.trim();
		if (hex.startsWith("#")) {
			hex = hex.substring(1);
		}
		if (!hex.matches("(?i)[0-9a-f]{6}")) {
			hex = "FFFFFF";
		}
		String rr = hex.substring(0, 2);
		String gg = hex.substring(2, 4);
		String bb = hex.substring(4, 6);
		return "&H00" + bb + gg + rr;
	}

	private static String toAssPrimaryColor(String raw) {
		return assOpaquePrimaryFromWebHex(raw) + "&";
	}

	private static int toAssAlpha(double opacity) {
		double clamped = Math.max(0d, Math.min(1d, opacity));
		return (int) Math.round((1d - clamped) * 255d);
	}

	private static String normalizeAssDialogueNewlines(String value) {
		if (value == null || value.isEmpty()) {
			return "";
		}
		// Unicode line/paragraph separators (common in pasted SRT) and all CR variants → LF before ASS \N.
		return value
				.replace("\u2028", "\n")
				.replace("\u2029", "\n")
				.replace("\r\n", "\n")
				.replace('\r', '\n');
	}

	private static String escapeAssDialogueText(String value) {
		String t = normalizeAssDialogueNewlines(value);
		return t
				.replace("\\", "\\\\")
				.replace("{", "\\{")
				.replace("}", "\\}")
				.replace("\n", "\\N");
	}

	private static String formatAssTime(double seconds) {
		double s = Math.max(0d, Double.isFinite(seconds) ? seconds : 0d);
		int hours = (int) Math.floor(s / 3600d);
		s -= hours * 3600d;
		int minutes = (int) Math.floor(s / 60d);
		s -= minutes * 60d;
		int secs = (int) Math.floor(s);
		int centis = (int) Math.round((s - secs) * 100d);
		if (centis >= 100) {
			centis = 0;
			secs++;
		}
		if (secs >= 60) {
			secs = 0;
			minutes++;
		}
		if (minutes >= 60) {
			minutes = 0;
			hours++;
		}
		return String.format(Locale.US, "%d:%02d:%02d.%02d", hours, minutes, secs, centis);
	}

	private String formatDecimal(double value) {
		return String.format(Locale.US, "%.4f", value);
	}

	private record ImageInput(
			JsonNode layer,
			Path path,
			/** When true, pass {@code -stream_loop -1} before this input so GIF loops for the overlay window. */
			boolean loopInput
	) {
	}
}
