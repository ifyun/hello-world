package com.github.ifyun.demo.component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.github.ifyun.demo.config.RedisConfig.GROUP_NAME;
import static com.github.ifyun.demo.config.RedisConfig.STREAM_KEY;

/**
 * 未 ACK 消息处理器
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PendingHandler {

    private final RedisTemplate<String, Object> redisTemplate;

    @Scheduled(fixedDelay = 5, timeUnit = TimeUnit.SECONDS)
    public void start() {
        try {
            execute();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void execute() {
        // 获取 Pending 消息（已分发给消费者但是未 ACK 的消息）
        var pending = redisTemplate.opsForStream().pending(STREAM_KEY, GROUP_NAME, Range.unbounded(), 20);
        pending.stream()
                // 只取分发超过一定时间没有 ACK 的消息
                .filter(e -> e.getElapsedTimeSinceLastDelivery().getSeconds() > 10)
                // 这里的写法是每次获取一条消息，
                // 也可以只通过一次 range 操作把所有的消息都拿过来，需要找出 RecordId 的最小值和最大值
                .map(e -> redisTemplate.opsForStream().range(STREAM_KEY, Range.just(e.getId().getValue())))
                .filter(v -> v != null && !v.isEmpty())
                .flatMap(Collection::stream)
                .forEach(record -> {
                    // 作为新的消息加入
                    var newRecord = StreamRecords.newRecord()
                            .ofObject(record.getValue())
                            .withStreamKey(STREAM_KEY);
                    redisTemplate.opsForStream().add(newRecord);
                    // 将旧消息 ACK 并删除
                    redisTemplate.opsForStream().acknowledge(STREAM_KEY, GROUP_NAME, record.getId());
                    redisTemplate.opsForStream().delete(STREAM_KEY, record.getId());
                    log.warn("已重发: {}", record.getId());
                });
    }
}
