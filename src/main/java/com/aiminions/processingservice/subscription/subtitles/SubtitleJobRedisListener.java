package com.aiminions.processingservice.subscription.subtitles;

import com.aiminions.processingservice.jobs.subtitles.SubtitleJobMessage;
import com.aiminions.processingservice.jobs.subtitles.SubtitlePipeline;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executor;

@Component
@RequiredArgsConstructor
@Slf4j
public class SubtitleJobRedisListener implements MessageListener {

	private final ObjectMapper objectMapper;
	private final SubtitlePipeline subtitlePipeline;

	@Qualifier("transcribeExecutor")
	private final Executor executor;

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

