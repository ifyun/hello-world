package com.github.ifyun.demo.config;

import com.github.ifyun.demo.component.QueueListener;
import com.github.ifyun.demo.entity.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.hash.ObjectHashMapper;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.util.ErrorHandler;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.concurrent.Executors;

import static com.github.ifyun.demo.config.RedisConfig.GROUP_NAME;
import static com.github.ifyun.demo.config.RedisConfig.STREAM_KEY;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class RedisStreamConfig {

    @Value("${spring.application.name}")
    private String appName;

    private final RedisConnectionFactory redisConnectionFactory;

    private final RedisTemplate<String, Object> redisTemplate;

    private static class StreamErrorHandler implements ErrorHandler {
        @Override
        public void handleError(Throwable t) {
            var stream = new ByteArrayOutputStream();
            t.printStackTrace(new PrintStream(stream));
            log.error(stream.toString());
        }
    }

    /**
     * 必须设置初始方法和销毁方法，否则不会开始消费消息
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    public StreamMessageListenerContainer<String, ObjectRecord<String, Message>> streamMessageListenerContainer() {
        // 注意：单个消费者不支持多个线程消费消息
        var executor = Executors.newSingleThreadExecutor();
        var options = StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                .builder()
                .batchSize(1)
                .executor(executor)
                .keySerializer(RedisSerializer.string())
                .hashKeySerializer(RedisSerializer.string())
                .hashValueSerializer(RedisSerializer.string())
                .errorHandler(new StreamErrorHandler())
                .pollTimeout(Duration.ZERO)
                .objectMapper(new ObjectHashMapper())
                .targetType(Message.class)
                .build();

        var streamListenerContainer = StreamMessageListenerContainer.create(redisConnectionFactory, options);
        // 注册监听器，手动 ACK
        // 从上一次消费的 RecordId 之后开始接收
        streamListenerContainer.receive(
                Consumer.from(GROUP_NAME, appName),
                StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed()),
                new QueueListener(redisTemplate)
        );

        return streamListenerContainer;
    }
}
