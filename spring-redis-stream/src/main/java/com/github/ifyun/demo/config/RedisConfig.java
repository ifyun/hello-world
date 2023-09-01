package com.github.ifyun.demo.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Slf4j
@Configuration
public class RedisConfig {

    public static final String STREAM_KEY = "stream0";

    public static final String GROUP_NAME = "grp0";

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
        var redisTemplate = new RedisTemplate<String, Object>();
        redisTemplate.setConnectionFactory(connectionFactory);
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new StringRedisSerializer());
        redisTemplate.setHashKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashValueSerializer(RedisSerializer.string());
        redisTemplate.afterPropertiesSet();

        if (Boolean.FALSE.equals(redisTemplate.hasKey(STREAM_KEY))) {
            // 不存在 STREAM_KEY，创建消费组，自动创建 Stream
            redisTemplate.opsForStream().createGroup(STREAM_KEY, GROUP_NAME);
        } else {
            var groups = redisTemplate.opsForStream().groups(STREAM_KEY);
            // 存在 STREAM_KEY，检查是否已存在消费组
            if (groups.stream().noneMatch(g -> g.groupName().equals(GROUP_NAME))) {
                redisTemplate.opsForStream().createGroup(STREAM_KEY, GROUP_NAME);
            }
        }

        return redisTemplate;
    }
}
