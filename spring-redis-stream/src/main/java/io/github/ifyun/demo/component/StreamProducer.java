package io.github.ifyun.demo.component;

import io.github.ifyun.demo.entity.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.github.ifyun.demo.config.RedisConfig.STREAM_KEY;

/**
 * 消息生产者
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StreamProducer implements ApplicationRunner {
    private final RedisTemplate<String, Object> redisTemplate;

    @Override
    public void run(ApplicationArguments args) {
        // 每隔 3s 产生一个消息
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(this::sendRecord, 0, 3, TimeUnit.SECONDS);
    }

    private void sendRecord() {
        var msg = Message.create();
        var record = StreamRecords.newRecord()
                .in(STREAM_KEY)
                .ofObject(msg)
                .withId(RecordId.autoGenerate());

        redisTemplate.opsForStream().add(record);
    }
}
