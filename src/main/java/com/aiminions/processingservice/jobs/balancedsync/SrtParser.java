package com.aiminions.processingservice.jobs.balancedsync;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal SRT parser for cues with start/end timestamps and text.
 * Ignores cue indices and preserves only time ranges + trimmed text.
 */
public final class SrtParser {

	private SrtParser() {}

	public record Cue(long startMs, long endMs, String text) {
		public long durationMs() {
			return Math.max(0, endMs - startMs);
		}
	}

	private static final Pattern TIMELINE = Pattern.compile(
			"(\\d{2}):(\\d{2}):(\\d{2})[,\\.](\\d{3})\\s*-->\\s*(\\d{2}):(\\d{2}):(\\d{2})[,\\.](\\d{3})");

	public static List<Cue> parse(String srtText) {
		if (srtText == null || srtText.isBlank()) return List.of();

		String normalized = srtText.replace("\r\n", "\n").replace('\r', '\n');
		String[] blocks = normalized.split("\\n\\s*\\n");
		List<Cue> cues = new ArrayList<>();

		for (String b : blocks) {
			if (b == null) continue;
			String block = b.trim();
			if (block.isEmpty()) continue;

			String[] lines = block.split("\\n");
			int timeLineIdx = -1;
			Matcher m = null;
			for (int i = 0; i < lines.length; i++) {
				Matcher mm = TIMELINE.matcher(lines[i].trim());
				if (mm.find()) {
					timeLineIdx = i;
					m = mm;
					break;
				}
			}
			if (timeLineIdx < 0 || m == null) continue;

			long start = toMs(m.group(1), m.group(2), m.group(3), m.group(4));
			long end = toMs(m.group(5), m.group(6), m.group(7), m.group(8));
			if (start < 0) start = 0;
			if (end < start + 1) end = start + 1;

			StringBuilder text = new StringBuilder();
			for (int i = timeLineIdx + 1; i < lines.length; i++) {
				String t = lines[i] == null ? "" : lines[i].trim();
				if (t.isEmpty()) continue;
				if (!text.isEmpty()) text.append(' ');
				text.append(t);
			}
			String t = text.toString().trim();
			if (t.isEmpty()) continue;
			cues.add(new Cue(start, end, t));
		}

		if (cues.isEmpty()) return List.of();
		cues.sort(Comparator.comparingLong(Cue::startMs).thenComparingLong(Cue::endMs));
		return cues;
	}

	private static long toMs(String hh, String mm, String ss, String ms) {
		try {
			long h = Long.parseLong(hh);
			long m = Long.parseLong(mm);
			long s = Long.parseLong(ss);
			long milli = Long.parseLong(ms);
			return (h * 3600L + m * 60L + s) * 1000L + milli;
		} catch (Exception e) {
			return -1;
		}
	}
}

