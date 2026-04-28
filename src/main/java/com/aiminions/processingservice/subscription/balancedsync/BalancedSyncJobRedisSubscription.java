package com.aiminions.processingservice.subscription.balancedsync;

import com.aiminions.processingservice.config.ProcessingProperties;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnBean(RedisMessageListenerContainer.class)
@ConditionalOnProperty(name = "app.processing.queue-mode", havingValue = "pubsub")
@RequiredArgsConstructor
@Slf4j
public class BalancedSyncJobRedisSubscription {

	private final RedisMessageListenerContainer redisMessageListenerContainer;
	private final BalancedSyncJobRedisListener balancedSyncJobRedisListener;
	private final ProcessingProperties processingProperties;

	@PostConstruct
	void subscribe() {
		String channel = processingProperties.getRedisBalancedSyncJobChannel();
		redisMessageListenerContainer.addMessageListener(balancedSyncJobRedisListener, new ChannelTopic(channel));
		log.info("Subscribed to Redis channel {}", channel);
	}
}

