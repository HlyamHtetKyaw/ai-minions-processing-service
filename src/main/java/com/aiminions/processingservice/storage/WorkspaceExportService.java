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
import java.util.List;
import java.util.Locale;
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
		List<String> args = new ArrayList<>();
		args.add("-y");
		if (trimStart > 0) {
			args.add("-ss");
			args.add(formatDecimal(trimStart));
		}
		args.add("-i");
		args.add(input.toString());
		if (trimEnd > trimStart && trimEnd > 0) {
			args.add("-t");
			args.add(formatDecimal(trimEnd - Math.max(0d, trimStart)));
		}

		List<ImageInput> images = collectImageInputs(payload, workDir);
		for (ImageInput image : images) {
			args.add("-i");
			args.add(image.path().toString());
		}

		String filterComplex = buildFilterComplex(payload, trimStart, trimEnd, speed, images, srtFile, workDir, size[0], size[1]);
		args.add("-filter_complex");
		args.add(filterComplex);
		args.add("-map");
		args.add("[vout]");

		boolean muted = readBoolean(payload.path("originalAudio"), "muted", false);
		double originalVol = readNumber(payload.path("originalAudio"), "volume", 100d) / 100d;
		if (muted) {
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
		args.add("veryfast");
		args.add("-crf");
		args.add("23");
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
			double trimStart,
			double trimEnd,
			double speed,
			List<ImageInput> images,
			Path srtFile,
			Path workDir,
			int videoW,
			int videoH
	) {
		List<String> parts = new ArrayList<>();
		double safeSpeed = Math.abs(speed) < 0.0001d ? 1d : speed;
		double rawDuration = readNumber(payload, "duration", 0d);
		double selectedDuration = trimEnd > trimStart ? trimEnd - trimStart : Math.max(0d, rawDuration - trimStart);
		double exportedDuration = Math.max(0.01d, selectedDuration / safeSpeed);

		double scaleX = readNumber(payload.path("displayToNaturalScale"), "x", 1d);
		double scaleY = readNumber(payload.path("displayToNaturalScale"), "y", 1d);

		String current = "v0";
		parts.add("[0:v]setpts=" + formatDecimal(1d / safeSpeed) + "*PTS[" + current + "]");

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

		JsonNode textLayers = payload.path("textLayers");
		if (textLayers.isArray()) {
			int idx = 0;
			for (JsonNode layer : textLayers) {
				String text = layer.path("content").asText("").trim();
				if (text.isEmpty()) {
					continue;
				}
				double start = toExportTime(readNumber(layer, "startTime", 0d), trimStart, safeSpeed);
				double end = toExportTime(readNumber(layer, "endTime", exportedDuration), trimStart, safeSpeed);
				end = Math.min(exportedDuration, end);
				if (end <= start) {
					continue;
				}
				int x = (int) Math.round(readNumber(layer, "x", 0d) * scaleX);
				int y = (int) Math.round(readNumber(layer, "y", 0d) * scaleY);
				int fontSize = (int) Math.round(Math.max(10d, readNumber(layer, "fontSize", 24d) * Math.max(scaleX, scaleY)));
				double opacity = Math.max(0d, Math.min(1d, readNumber(layer, "opacity", 100d) / 100d));
				String color = toFfmpegColor(readText(layer, "color", "#FFFFFF"), opacity);
				String next = "vt" + idx;
				parts.add("[" + current + "]drawtext=text='" + escapeText(text) + "':x=" + x + ":y=" + y
						+ ":fontsize=" + fontSize + ":fontcolor=" + color
						+ ":enable='between(t," + formatDecimal(start) + "," + formatDecimal(end) + ")'"
						+ "[" + next + "]");
				current = next;
				idx++;
			}
		}

		for (int i = 0; i < images.size(); i++) {
			ImageInput imageInput = images.get(i);
			JsonNode layer = imageInput.layer();
			double start = toExportTime(readNumber(layer, "startTime", 0d), trimStart, safeSpeed);
			double end = toExportTime(readNumber(layer, "endTime", exportedDuration), trimStart, safeSpeed);
			end = Math.min(exportedDuration, end);
			if (end <= start) {
				continue;
			}
			int width = Math.max(2, (int) Math.round(readNumber(layer, "width", 100d) * scaleX));
			int height = Math.max(2, (int) Math.round(readNumber(layer, "height", 100d) * scaleY));
			int x = (int) Math.round(readNumber(layer, "x", 0d) * scaleX);
			int y = (int) Math.round(readNumber(layer, "y", 0d) * scaleY);
			double opacity = Math.max(0d, Math.min(1d, readNumber(layer, "opacity", 1d)));
			boolean flipX = readBoolean(layer, "flipX", false);
			boolean flipY = readBoolean(layer, "flipY", false);
			double rotation = Math.toRadians(readNumber(layer, "rotation", 0d));

			List<String> imageOps = new ArrayList<>();
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
					+ ":enable='between(t," + formatDecimal(start) + "," + formatDecimal(end) + ")'"
					+ "[" + next + "]");
			current = next;
		}

		if (srtFile != null) {
			String next = "vsrt";
			// Convert SRT -> ASS and burn via ass filter with complex shaping for Myanmar.
			Path assFile = maybeConvertSrtToAss(srtFile, workDir, payload, videoW, videoH);
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
			int fontSize = readInt(payload, "subtitlesFontSize", processingProperties.getSubtitlesFontSize());
			fontSize = Math.max(14, Math.min(60, fontSize));
			int marginV = Math.max(0, Math.min(300, processingProperties.getSubtitlesMarginV()));
			// Force Unicode decoding and prefer a Myanmar Unicode-capable font.
			// Note: libass uses system fontconfig; if the font isn't installed, it will fall back.
			String style = "FontName=" + escapeAss(fontName)
					+ ",FontSize=" + fontSize
					+ ",PrimaryColour=&H00FFFFFF"
					// Outlines can make Myanmar clusters look broken in some libass render paths.
					// Prefer a background box for readability instead of stroke outline.
					+ ",BorderStyle=3"
					+ ",BackColour=&H80000000"
					+ ",Outline=0"
					+ ",Shadow=0"
					+ ",Alignment=2"
					+ ",MarginV=" + marginV;

			String filter;
			if (burnFile.toString().toLowerCase(Locale.ROOT).endsWith(".ass")) {
				filter = "ass='" + escaped + "':shaping=complex:original_size=" + videoW + "x" + videoH;
			} else {
				filter = "subtitles='" + escaped + "':charenc=UTF-8:wrap_unicode=1:force_style='" + style + "'";
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
			if (fontName.isBlank()) fontName = "Pyidaungsu";
			int fontSize = readInt(payload, "subtitlesFontSize", processingProperties.getSubtitlesFontSize());
			fontSize = Math.max(14, Math.min(60, fontSize));
			int marginV = Math.max(0, Math.min(300, processingProperties.getSubtitlesMarginV()));

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
				if (line.startsWith("Style: Default,")) {
					// ASS style format from ffmpeg: Style: Default,Arial,16,&Hffffff,...
					// We'll keep colors but force font + size + alignment + margins and disable outline.
					String[] parts = line.split(",", -1);
					if (parts.length >= 23) {
						parts[1] = escapeAssField(fontName);
						parts[2] = String.valueOf(fontSize);
						// BorderStyle=3 + BackColour provides readability without outline.
						parts[16] = "3";           // BorderStyle
						parts[17] = "0";           // Outline
						parts[18] = "0";           // Shadow
						parts[19] = hasPos ? "5" : "2"; // Alignment center (with \pos) or bottom-center
						parts[22] = String.valueOf(marginV); // MarginV
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
		} catch (Exception ignored) {
			return null;
		}
	}

	private static String escapeAssField(String v) {
		if (v == null) return "";
		// Style fields are comma-separated; avoid commas.
		return v.replace(",", " ");
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
			return Z2U.convert(line);
		}
		return line;
	}

	private static String escapeAss(String v) {
		if (v == null) return "";
		// ASS force_style string is single-quoted in ffmpeg filter; escape quotes/backslashes.
		return v.replace("\\", "\\\\").replace("'", "\\'");
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
		for (JsonNode layer : imageLayers) {
			String src = ObjectStorageTransferService.stripUrlFragmentForDownload(readText(layer, "src", ""));
			if (src == null || src.isBlank()) {
				continue;
			}
			try {
				Path downloaded = objectStorageTransferService.download(src, workDir);
				Path normalized = workDir.resolve("layer-image-" + out.size() + ".png");
				ffmpegRunner.run(
						List.of("-y", "-i", downloaded.toString(), "-frames:v", "1", normalized.toString()));
				out.add(new ImageInput(layer, normalized));
			} catch (Exception e) {
				// Many external hosts (e.g. Pinterest) block non-browser fetches. Skip overlay instead of failing export.
				log.warn("Skipping image overlay; could not download or decode: {}", src, e);
			}
		}
		return out;
	}

	private double toExportTime(double sourceTime, double trimStart, double speed) {
		double shifted = Math.max(0d, sourceTime - trimStart);
		return shifted / Math.max(0.0001d, speed);
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

	private String toFfmpegColor(String raw, double alpha) {
		String c = raw == null ? "#FFFFFF" : raw.trim();
		if (c.isEmpty()) {
			c = "#FFFFFF";
		}
		if (c.startsWith("#")) {
			return c + "@" + formatDecimal(alpha);
		}
		return c + "@" + formatDecimal(alpha);
	}

	private String escapeText(String value) {
		String t = value == null ? "" : value;
		return t
				.replace("\\", "\\\\")
				.replace(":", "\\:")
				.replace("'", "\\'")
				.replace("%", "\\%")
				.replace("\n", "\\n")
				.replace("\r", "");
	}

	private String formatDecimal(double value) {
		return String.format(Locale.US, "%.4f", value);
	}

	private record ImageInput(
			JsonNode layer,
			Path path
	) {
	}
}
