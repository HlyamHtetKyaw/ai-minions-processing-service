package com.aiminions.processingservice.storage;

import com.aiminions.processingservice.media.ffmpeg.FfmpegRunner;
import com.aiminions.processingservice.storage.dto.WorkspaceExportResponse;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;

@Service
@RequiredArgsConstructor
public class WorkspaceExportService {

	private final ObjectStorageTransferService objectStorageTransferService;
	private final FfmpegRunner ffmpegRunner;

	public WorkspaceExportResponse exportVideo(Long userId, JsonNode payload) {
		if (payload == null || payload.isNull()) {
			throw new ResponseStatusException(BAD_REQUEST, "payload is required");
		}
		String videoUrl = readRequiredText(payload, "videoUrl");
		double trimStart = readNumber(payload, "trimStart", 0d);
		double trimEnd = readNumber(payload, "trimEnd", 0d);
		double speed = readNumber(payload, "speed", 1d);
		if (speed <= 0) {
			speed = 1d;
		}

		Path workDir = null;
		try {
			workDir = Files.createTempDirectory("workspace-export-");
			Path input = objectStorageTransferService.download(videoUrl, workDir);
			Path output = workDir.resolve("export-" + System.currentTimeMillis() + ".mp4");
			runEncode(input, output, payload, trimStart, trimEnd, speed, workDir);

			String keyHint = "video-editor/" + (userId == null ? "unknown" : userId) + "/exports/"
					+ System.currentTimeMillis() + ".mp4";
			ObjectStorageTransferService.StoredObject stored = objectStorageTransferService
					.uploadFile(output, keyHint, "video/mp4");
			String readUrl = objectStorageTransferService.presignWorkspaceRead(stored.key());
			return new WorkspaceExportResponse(stored.storageUrl(), readUrl, stored.key());
		} catch (ResponseStatusException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new ResponseStatusException(INTERNAL_SERVER_ERROR, "Failed to export workspace video", ex);
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
			Path workDir
	) throws IOException, InterruptedException {
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

		String filterComplex = buildFilterComplex(payload, trimStart, trimEnd, speed, images);
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
		ffmpegRunner.run(args);
	}

	private String buildFilterComplex(
			JsonNode payload,
			double trimStart,
			double trimEnd,
			double speed,
			List<ImageInput> images
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

		parts.add("[" + current + "]format=yuv420p[vout]");
		return String.join(";", parts);
	}

	private List<ImageInput> collectImageInputs(JsonNode payload, Path workDir) throws IOException, InterruptedException {
		List<ImageInput> out = new ArrayList<>();
		JsonNode imageLayers = payload.path("imageLayers");
		if (!imageLayers.isArray()) {
			return out;
		}
		int idx = 0;
		for (JsonNode layer : imageLayers) {
			String src = readText(layer, "src", "");
			if (src.isBlank()) {
				continue;
			}
			Path downloaded = objectStorageTransferService.download(src, workDir);
			Path normalized = workDir.resolve("layer-image-" + idx + ".png");
			ffmpegRunner.run(List.of("-y", "-i", downloaded.toString(), "-frames:v", "1", normalized.toString()));
			out.add(new ImageInput(layer, normalized));
			idx++;
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
