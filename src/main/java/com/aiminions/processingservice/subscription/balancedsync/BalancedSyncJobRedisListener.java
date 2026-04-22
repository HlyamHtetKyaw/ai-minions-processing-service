package com.aiminions.processingservice.subscription.balancedsync;

import com.aiminions.processingservice.jobs.balancedsync.BalancedSyncJobMessage;
import com.aiminions.processingservice.jobs.balancedsync.BalancedSyncPipeline;
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
public class BalancedSyncJobRedisListener implements MessageListener {

	private final ObjectMapper objectMapper;
	private final BalancedSyncPipeline balancedSyncPipeline;
	@Qualifier("balancedSyncExecutor")
	private final Executor balancedSyncExecutor;

	@Override
	public void onMessage(Message message, byte[] pattern) {
		String body = new String(message.getBody(), StandardCharsets.UTF_8);
		balancedSyncExecutor.execute(() -> {
			try {
				BalancedSyncJobMessage msg = objectMapper.readValue(body, BalancedSyncJobMessage.class);
				log.info("Received balanced-sync job message jobId={} videoRate={} voiceRate={}",
						msg.jobId(), msg.videoRate(), msg.voiceRate());
				balancedSyncPipeline.run(msg);
			} catch (Exception e) {
				log.error("Failed to handle balanced-sync job message: {}", body, e);
			}
		});
	}
}

