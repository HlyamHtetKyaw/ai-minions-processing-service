package com.aiminions.processingservice.media.ffmpeg;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Component;

import com.aiminions.processingservice.config.ProcessingProperties;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class FfmpegRunner {

	private final ProcessingProperties processingProperties;

	/**
	 * Fail fast in logs if ffmpeg cannot start (common on Windows when App Control blocks PATH resolution).
	 * Does not stop the JVM so other endpoints still start; video features will error until fixed.
	 */
	@PostConstruct
	void logFfmpegAvailability() {
		String bin = processingProperties.getFfmpegBinary();
		try {
			RunResult r = runRaw(List.of("-hide_banner", "-version"), 20, TimeUnit.SECONDS);
			if (r.exitCode() == 0) {
				String first = r.output() == null ? "" : r.output().lines().findFirst().orElse("").trim();
				log.info("ffmpeg is available: binary={} — {}", bin, first);
			} else {
				log.warn("ffmpeg binary={} exited with code {}: {}", bin, r.exitCode(), truncate(r.output(), 600));
			}
		} catch (Exception e) {
			log.error(
					"""
					================================================================================
					FFMPEG NOT STARTABLE — video workspace export and other ffmpeg jobs will fail.
					Configured binary: {}
					Reason: {}
					General: install ffmpeg and set FFMPEG_BINARY to the full path, or put it on PATH.
					Windows (error 4551 / Application Control): use a full path to an allowed ffmpeg.exe
					(e.g. extract https://www.gyan.dev/ffmpeg/builds/ to C:\\\\ffmpeg, then
					FFMPEG_BINARY=C:\\\\ffmpeg\\\\bin\\\\ffmpeg.exe).
					IntelliJ: Run → Edit Configurations → Environment variables → FFMPEG_BINARY=...
					Also: right-click ffmpeg.exe → Properties → Unblock; or IT allowlist for that path.
					================================================================================""",
					bin,
					e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
		}
	}

	private static String truncate(String s, int max) {
		if (s == null || s.length() <= max) {
			return s == null ? "" : s;
		}
		return s.substring(0, max) + "…";
	}

	public void run(List<String> args) throws IOException, InterruptedException {
		RunResult r = runRaw(args, 45, TimeUnit.MINUTES);
		if (r.exitCode() != 0) {
			String out = r.output();
			String tail = out.length() > 4000 ? out.substring(out.length() - 4000) : out;
			throw new IllegalStateException("ffmpeg exited with " + r.exitCode() + ": " + tail);
		}
	}

	public void run(List<String> args, Map<String, String> env) throws IOException, InterruptedException {
		RunResult r = runRaw(args, env, 45, TimeUnit.MINUTES);
		if (r.exitCode() != 0) {
			String out = r.output();
			String tail = out.length() > 4000 ? out.substring(out.length() - 4000) : out;
			throw new IllegalStateException("ffmpeg exited with " + r.exitCode() + ": " + tail);
		}
	}

	/** Runs ffmpeg and returns stdout+stderr (even when exit code != 0). */
	public RunResult runRaw(List<String> args, long timeout, TimeUnit unit) throws IOException, InterruptedException {
		return runRaw(args, Map.of(), timeout, unit);
	}

	/** Runs ffmpeg with extra environment and returns stdout+stderr (even when exit code != 0). */
	public RunResult runRaw(List<String> args, Map<String, String> env, long timeout, TimeUnit unit) throws IOException, InterruptedException {
		List<String> cmd = new ArrayList<>();
		cmd.add(processingProperties.getFfmpegBinary());
		cmd.addAll(args);
		log.debug("ffmpeg {}", String.join(" ", cmd));
		ProcessBuilder pb = new ProcessBuilder(cmd);
		if (env != null && !env.isEmpty()) {
			pb.environment().putAll(env);
		}
		pb.redirectErrorStream(true);
		final Process p;
		try {
			p = pb.start();
		} catch (IOException e) {
			String m = e.getMessage() == null ? "" : e.getMessage();
			if (m.contains("4551") || m.contains("Application Control") || m.contains("blocked this file")) {
				String bin = processingProperties.getFfmpegBinary();
				throw new IOException(
						"Cannot start ffmpeg: Windows blocked execution (Application Control / AppLocker / WDAC). "
								+ "Set FFMPEG_BINARY or app.processing.ffmpeg-binary to the full path of an allowed ffmpeg.exe "
								+ "(e.g. from https://www.gyan.dev/ffmpeg/builds/), add it to your allowlist, or unblock the file in Properties. "
								+ "Currently configured: "
								+ bin,
						e);
			}
			throw e;
		}
		String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		boolean finished = p.waitFor(timeout, unit);
		if (!finished) {
			p.destroyForcibly();
			throw new IllegalStateException("ffmpeg timed out");
		}
		return new RunResult(p.exitValue(), out);
	}

	public record RunResult(int exitCode, String output) {
	}
}
