package com.aiminions.processingservice.media.ffmpeg;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Component;

import com.aiminions.processingservice.config.ProcessingProperties;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class FfmpegRunner {

	private final ProcessingProperties processingProperties;

	public void run(List<String> args) throws IOException, InterruptedException {
		List<String> cmd = new ArrayList<>();
		cmd.add(processingProperties.getFfmpegBinary());
		cmd.addAll(args);
		log.debug("ffmpeg {}", String.join(" ", cmd));
		ProcessBuilder pb = new ProcessBuilder(cmd);
		pb.redirectErrorStream(true);
		Process p = pb.start();
		String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		boolean finished = p.waitFor(45, TimeUnit.MINUTES);
		if (!finished) {
			p.destroyForcibly();
			throw new IllegalStateException("ffmpeg timed out");
		}
		int code = p.exitValue();
		if (code != 0) {
			String tail = out.length() > 4000 ? out.substring(out.length() - 4000) : out;
			throw new IllegalStateException("ffmpeg exited with " + code + ": " + tail);
		}
	}
}
