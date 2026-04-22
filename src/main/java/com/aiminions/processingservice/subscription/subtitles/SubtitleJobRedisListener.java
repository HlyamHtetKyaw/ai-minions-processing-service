package com.aiminions.processingservice.subscription.subtitles;

import com.aiminions.processingservice.jobs.subtitles.SubtitleJobMessage;
import com.aiminions.processingservice.jobs.subtitles.SubtitlePipeline;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executor;

@Component
@Slf4j
public class SubtitleJobRedisListener implements MessageListener {

	private final ObjectMapper objectMapper;
	private final SubtitlePipeline subtitlePipeline;
	private final Executor executor;

	public SubtitleJobRedisListener(
			ObjectMapper objectMapper,
			SubtitlePipeline subtitlePipeline,
			@Qualifier("transcribeExecutor") Executor executor
	) {
		this.objectMapper = objectMapper;
		this.subtitlePipeline = subtitlePipeline;
		this.executor = executor;
	}

	@Override
	public void onMessage(Message message, byte[] pattern) {
		String body = new String(message.getBody(), StandardCharsets.UTF_8);
		executor.execute(() -> {
			try {
				SubtitleJobMessage msg = objectMapper.readValue(body, SubtitleJobMessage.class);
				log.info("Received subtitle job message jobId={} sourceType={}", msg.jobId(), msg.sourceType());
				subtitlePipeline.run(msg);
			} catch (Exception e) {
				log.error("Failed to handle subtitle job message: {}", body, e);
			}
		});
	}
}

