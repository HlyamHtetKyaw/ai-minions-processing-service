package com.aiminions.processingservice.config;

import java.util.concurrent.Executor;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class AsyncProcessingConfig {

	@Bean(name = "transcribeExecutor")
	Executor transcribeExecutor() {
		ThreadPoolTaskExecutor ex = new ThreadPoolTaskExecutor();
		ex.setThreadNamePrefix("transcribe-");
		ex.setCorePoolSize(2);
		ex.setMaxPoolSize(8);
		ex.setQueueCapacity(100);
		ex.initialize();
		return ex;
	}
}
