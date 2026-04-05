package com.aiminions.processingservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties(prefix = "app.storage")
public class WorkerStorageProperties {

	public enum Provider {
		AWS,
		GCP,
		DIGITALOCEAN
	}

	private Provider provider = Provider.AWS;
	private String bucket = "";
	private String region = "us-east-1";
	private String endpoint = "";
	private Boolean pathStyleAccess;
	private String gcpCredentialsPath = "";
	private String awsAccessKeyId = "";
	private String awsSecretAccessKey = "";

	public boolean isPathStyleAccessEffective() {
		if (pathStyleAccess != null) {
			return pathStyleAccess;
		}
		return provider == Provider.DIGITALOCEAN;
	}
}
