package com.aiminions.processingservice.subscription.subtitles;

import com.aiminions.processingservice.config.ProcessingProperties;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnBean(RedisMessageListenerContainer.class)
@RequiredArgsConstructor
@Slf4j
public class SubtitleJobRedisSubscription {

	private final RedisMessageListenerContainer redisMessageListenerContainer;
	private final SubtitleJobRedisListener subtitleJobRedisListener;
	private final ProcessingProperties processingProperties;

	@PostConstruct
	void subscribe() {
		String channel = processingProperties.getRedisSubtitlesJobChannel();
		redisMessageListenerContainer.addMessageListener(subtitleJobRedisListener, new ChannelTopic(channel));
		log.info("Subscribed to Redis channel {}", channel);
	}
}

