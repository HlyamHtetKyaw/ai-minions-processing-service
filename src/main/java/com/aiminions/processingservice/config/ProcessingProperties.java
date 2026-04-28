package com.aiminions.processingservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties(prefix = "app.processing")
public class ProcessingProperties {

	private String redisJobChannel = "ai-minions:jobs:transcribe";

	private String redisBalancedSyncJobChannel = "ai-minions:jobs:balanced-sync";
	private String redisSubtitlesJobChannel = "ai-minions:jobs:subtitles";

	private String generationStatusChannelPrefix = "ai-minions:generation:status:";

	private String mainServiceBaseUrl = "http://localhost:8081";

	private String workerToken = "";

	private String ffmpegBinary = "ffmpeg";

	/**
	 * Number of ffmpeg worker threads per export process.
	 * 0 lets ffmpeg auto-select based on the host.
	 */
	private int workspaceExportFfmpegThreads = 0;

	/** x264 preset for workspace export. Faster presets reduce encode time with lower compression efficiency. */
	private String workspaceExportPreset = "veryfast";

	/** x264 CRF for workspace export quality/size. Lower is higher quality and slower. */
	private int workspaceExportCrf = 23;

	/**
	 * Parallel workers used while preparing image overlays (download + normalize).
	 * Keep this modest to avoid saturating CPU/network under concurrent exports.
	 */
	private int workspaceExportImagePrepThreads = 4;

	/**
	 * Optional directory containing fonts for FFmpeg libass subtitles rendering.
	 * Example: "/usr/share/fonts" in many Linux images.
	 */
	private String subtitlesFontsDir = "";

	/**
	 * Preferred font name for burning subtitles (Unicode Myanmar capable),
	 * e.g. "Noto Sans Myanmar" or "Pyidaungsu".
	 */
	private String subtitlesFontName = "Noto Sans Myanmar";

	/**
	 * Optional classpath font resource to extract for FFmpeg, e.g. "fonts/Pyidaungsu.ttf".
	 * Useful when running in a container without system Myanmar fonts.
	 */
	private String subtitlesFontResource = "";

	/** Default subtitle burn-in font size (libass). */
	private int subtitlesFontSize = 22;

	/** Bottom margin for burned subtitles (pixels). */
	private int subtitlesMarginV = 64;

	/**
	 * If true, attempt to detect and convert Zawgyi-encoded subtitle text to Unicode
	 * right before burning subtitles. Keep false by default to avoid altering already-correct Unicode.
	 */
	private boolean subtitlesAutoConvertZawgyi = false;

	private double silenceStopDurationSeconds = 0.5;

	private String silenceStopThreshold = "-50dB";

	private String aiServiceBaseUrl = "http://localhost:8080";

	public String generationStatusChannel(long jobId) {
		return generationStatusChannelPrefix + jobId;
	}
}
