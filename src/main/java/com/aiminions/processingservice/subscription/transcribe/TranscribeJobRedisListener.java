package com.aiminions.processingservice.subscription.transcribe;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executor;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

import com.aiminions.processingservice.jobs.transcribe.TranscribeJobMessage;
import com.aiminions.processingservice.jobs.transcribe.TranscribePipeline;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class TranscribeJobRedisListener implements MessageListener {

	private final ObjectMapper objectMapper;
	private final TranscribePipeline transcribePipeline;
	@Qualifier("transcribeExecutor")
	private final Executor transcribeExecutor;

	@Override
	public void onMessage(Message message, byte[] pattern) {
		String body = new String(message.getBody(), StandardCharsets.UTF_8);
		transcribeExecutor.execute(() -> {
			try {
				TranscribeJobMessage msg = objectMapper.readValue(body, TranscribeJobMessage.class);
				log.info("Received transcribe job message jobId={} sourceType={}", msg.jobId(), msg.sourceType());
				transcribePipeline.run(msg);
			} catch (Exception e) {
				log.error("Failed to handle transcribe job message: {}", body, e);
			}
		});
	}
}
