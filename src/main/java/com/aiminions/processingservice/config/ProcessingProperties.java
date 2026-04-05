package com.aiminions.processingservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties(prefix = "app.processing")
public class ProcessingProperties {

	private String redisJobChannel = "ai-minions:jobs:transcribe";

	private String generationStatusChannelPrefix = "ai-minions:generation:status:";

	private String mainServiceBaseUrl = "http://localhost:8081";

	private String workerToken = "";

	private String ffmpegBinary = "ffmpeg";

	private double silenceStopDurationSeconds = 0.5;

	private String silenceStopThreshold = "-50dB";

	private String aiServiceBaseUrl = "http://localhost:8080";

	public String generationStatusChannel(long jobId) {
		return generationStatusChannelPrefix + jobId;
	}
}
