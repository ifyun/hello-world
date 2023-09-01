package com.github.ifyun.demo.component;

import com.github.ifyun.demo.entity.Message;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;

import java.util.concurrent.TimeUnit;

import static com.github.ifyun.demo.config.RedisConfig.GROUP_NAME;
import static com.github.ifyun.demo.config.RedisConfig.STREAM_KEY;

@Slf4j
@RequiredArgsConstructor
public class QueueListener implements StreamListener<String, ObjectRecord<String, Message>> {

    private final RedisTemplate<String, Object> redisTemplate;

    @Override
    @SneakyThrows
    public void onMessage(ObjectRecord<String, Message> message) {
        var id = message.getId();
        var value = message.getValue();

        TimeUnit.SECONDS.sleep(1);

        // 随机产生未 ACK 消息
        if (RandomUtils.nextInt(1, 5) % 2 == 0) {
            redisTemplate.opsForStream().acknowledge(STREAM_KEY, GROUP_NAME, id);
            redisTemplate.opsForStream().delete(STREAM_KEY, id);
            log.info("已确认: {}, {}", id, value);
        } else {
            log.warn("未确认: {}", id);
        }
    }
}
