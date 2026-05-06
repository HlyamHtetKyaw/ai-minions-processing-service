package com.aiminions.processingservice.jobs.balancedsync;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SrtParserTest {

	@Test
	void parsesBasicSrtCue() {
		String srt = """
				1
				00:00:01,000 --> 00:00:02,500
				Hello world

				""";
		List<SrtParser.Cue> cues = SrtParser.parse(srt);
		assertEquals(1, cues.size());
		assertEquals(1000, cues.get(0).startMs());
		assertEquals(2500, cues.get(0).endMs());
		assertEquals("Hello world", cues.get(0).text());
	}

	@Test
	void toleratesDotMillisecondsAndMultipleLines() {
		String srt = """
				00:00:00.000 --> 00:00:01.000
				line1
				line2

				""";
		List<SrtParser.Cue> cues = SrtParser.parse(srt);
		assertEquals(1, cues.size());
		assertEquals("line1 line2", cues.get(0).text());
	}
}

