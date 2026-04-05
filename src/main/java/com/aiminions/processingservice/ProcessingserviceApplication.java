package com.aiminions.processingservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.aiminions.processingservice.config.ProcessingProperties;
import com.aiminions.processingservice.config.WorkerStorageProperties;

@SpringBootApplication
@EnableConfigurationProperties({ ProcessingProperties.class, WorkerStorageProperties.class })
public class ProcessingserviceApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProcessingserviceApplication.class, args);
	}
}
