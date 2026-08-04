package com.tzw.mq;

import io.lettuce.core.RedisURI;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Redis Stream MQ 适配器集成测试。
 *
 * <p>需要本地 Redis（默认 127.0.0.1:6379，可用 REDIS_URL 覆盖）。
 * Redis 不可用时测试自动跳过，不影响常规构建。
 */
class RedisStreamMqAdapterTest {

    private String redisUrl;
    private RedisStreamMqAdapter adapter;

    @BeforeEach
    void setUp() {
        redisUrl = redisUrl();
        Assumptions.assumeTrue(redisReachable(redisUrl), "Redis not available, skip integration test");
        adapter = new RedisStreamMqAdapter(redisUrl);
    }

    @AfterEach
    void tearDown() {
        if (adapter != null) {
            adapter.close();
        }
    }

    @Test
    void producerConsumerAcrossConnections() throws Exception {
        RedisStreamMqAdapter producer = new RedisStreamMqAdapter(redisUrl);
        try {
            CountDownLatch latch = new CountDownLatch(2);
            List<MatchReadyEvent> received = new ArrayList<>();

            adapter.subscribeTyped("match.ready", MatchReadyEvent.class, event -> {
                synchronized (received) {
                    received.add(event);
                }
                latch.countDown();
            });

            // 等待消费者线程就绪（组创建 + 阻塞读取）
            Thread.sleep(500);

            producer.send("match.ready", new MatchReadyEvent(101L, 7L, "127.0.0.1", 10086, "tok1"));
            producer.send("match.ready", new MatchReadyEvent(102L, 8L, "127.0.0.1", 10086, "tok2"));

            assertTrue(latch.await(10, TimeUnit.SECONDS), "messages not delivered within 10s");
            assertEquals(2, received.size());
            MatchReadyEvent first = received.get(0);
            assertEquals(101L, first.roomId());
            assertEquals(7L, first.playerId());
            assertEquals("tok1", first.token());
        } finally {
            producer.close();
        }
    }

    private static String redisUrl() {
        String url = System.getenv("REDIS_URL");
        if (url == null || url.isBlank()) {
            url = System.getProperty("redis.url");
        }
        if (url == null || url.isBlank()) {
            url = "redis://127.0.0.1:6379";
        }
        return url;
    }

    private static boolean redisReachable(String url) {
        try {
            RedisURI uri = RedisURI.create(url);
            String host = uri.getHost() == null ? "127.0.0.1" : uri.getHost();
            int port = uri.getPort() == 0 ? 6379 : uri.getPort();
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 500);
                return true;
            }
        } catch (Exception e) {
            return false;
        }
    }
}
