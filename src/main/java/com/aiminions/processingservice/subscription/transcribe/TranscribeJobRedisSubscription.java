package com.aiminions.processingservice.subscription.transcribe;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;

import com.aiminions.processingservice.config.ProcessingProperties;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@ConditionalOnBean(RedisMessageListenerContainer.class)
@RequiredArgsConstructor
@Slf4j
public class TranscribeJobRedisSubscription {

	private final RedisMessageListenerContainer redisMessageListenerContainer;
	private final TranscribeJobRedisListener transcribeJobRedisListener;
	private final ProcessingProperties processingProperties;

	@PostConstruct
	void subscribe() {
		String channel = processingProperties.getRedisJobChannel();
		redisMessageListenerContainer.addMessageListener(transcribeJobRedisListener, new ChannelTopic(channel));
		log.info("Subscribed to Redis channel {}", channel);
	}
}
