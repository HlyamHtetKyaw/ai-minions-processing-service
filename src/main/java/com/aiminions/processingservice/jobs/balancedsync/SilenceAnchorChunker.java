package com.aiminions.processingservice.jobs.balancedsync;

import java.util.ArrayList;
import java.util.List;

public final class SilenceAnchorChunker {

	private SilenceAnchorChunker() {}

	public record SceneChunk(int index, long startMs, long endMs, List<SrtParser.Cue> cues) {
		public long durationMs() {
			return Math.max(0, endMs - startMs);
		}
	}

	public static List<SceneChunk> chunk(List<SrtParser.Cue> cues, long anchorGapMs) {
		if (cues == null || cues.isEmpty()) return List.of();
		long gap = Math.max(0, anchorGapMs);

		List<SceneChunk> out = new ArrayList<>();
		List<SrtParser.Cue> bucket = new ArrayList<>();
		long chunkStart = -1;
		long chunkEnd = -1;
		int idx = 0;

		for (int i = 0; i < cues.size(); i++) {
			SrtParser.Cue c = cues.get(i);
			if (c == null) continue;
			if (bucket.isEmpty()) {
				chunkStart = c.startMs();
				chunkEnd = c.endMs();
				bucket.add(c);
				continue;
			}

			SrtParser.Cue prev = bucket.get(bucket.size() - 1);
			long gapMs = c.startMs() - prev.endMs();
			if (gapMs > gap) {
				out.add(new SceneChunk(idx++, chunkStart, chunkEnd, List.copyOf(bucket)));
				bucket.clear();
				chunkStart = c.startMs();
				chunkEnd = c.endMs();
				bucket.add(c);
			} else {
				bucket.add(c);
				chunkEnd = Math.max(chunkEnd, c.endMs());
			}
		}

		if (!bucket.isEmpty()) {
			out.add(new SceneChunk(idx, chunkStart, chunkEnd, List.copyOf(bucket)));
		}

		return out;
	}
}

