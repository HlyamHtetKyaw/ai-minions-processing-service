package com.aiminions.processingservice.jobs.subtitles;

import java.util.List;

public final class SrtFormatter {

	private SrtFormatter() {}

	public record Cue(long startMs, long endMs, String text) {}

	public static String toSrt(List<Cue> cues) {
		StringBuilder sb = new StringBuilder(Math.max(256, cues.size() * 64));
		int idx = 1;
		for (Cue c : cues) {
			if (c == null) continue;
			String text = c.text() == null ? "" : c.text().trim();
			if (text.isBlank()) continue;
			long start = Math.max(0, c.startMs());
			long end = Math.max(start + 1, c.endMs());
			sb.append(idx++).append('\n');
			sb.append(formatTime(start)).append(" --> ").append(formatTime(end)).append('\n');
			sb.append(text).append('\n').append('\n');
		}
		return sb.toString();
	}

	public static String formatTime(long ms) {
		long totalSeconds = ms / 1000;
		long milli = Math.floorMod(ms, 1000);
		long seconds = totalSeconds % 60;
		long totalMinutes = totalSeconds / 60;
		long minutes = totalMinutes % 60;
		long hours = totalMinutes / 60;
		return pad2(hours) + ":" + pad2(minutes) + ":" + pad2(seconds) + "," + pad3(milli);
	}

	private static String pad2(long v) {
		if (v < 10) return "0" + v;
		return String.valueOf(v);
	}

	private static String pad3(long v) {
		if (v < 10) return "00" + v;
		if (v < 100) return "0" + v;
		return String.valueOf(v);
	}
}

