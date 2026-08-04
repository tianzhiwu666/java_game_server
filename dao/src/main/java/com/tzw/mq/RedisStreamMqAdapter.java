package com.tzw.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Redis Stream MQ 适配器 —— 基于 Lettuce 的跨进程消息队列。
 *
 * <p>使用 Redis Stream + 消费者组实现发布订阅，替代进程内 {@link InMemoryMqAdapter}：
 * <ul>
 *   <li>发送：XADD 写入流 {@code mq:{topic}}</li>
 *   <li>接收：XREADGROUP 从消费者组阻塞读取，处理成功后 XACK</li>
 *   <li>消息体：JSON 序列化的 {@link MqMessage} 信封（含类型 + payload）</li>
 * </ul>
 *
 * <p>相比 Redis Pub/Sub，Stream + 消费者组具备消息持久化和 ACK 机制，
 * 消费者短暂离线期间的消息不会丢失（重连后从 PEL/新消息继续消费）。
 *
 * <h3>线程模型</h3>
 * <ul>
 *   <li>发送：共享连接（XADD 非阻塞，Lettuce 同步命令线程安全）</li>
 *   <li>消费：每个 topic 一个守护线程 + 独立连接（XREADGROUP BLOCK 会独占连接）</li>
 * </ul>
 */
public class RedisStreamMqAdapter implements MqProducer, MqConsumer, TypedMqConsumer {

    private static final Logger log = LoggerFactory.getLogger(RedisStreamMqAdapter.class);

    /** 流键前缀 */
    private static final String STREAM_PREFIX = "mq:";
    /** 消费者组名称（每个流一个组，本进程内单实例消费） */
    private static final String GROUP = "default";
    /** 阻塞读取超时 */
    private static final Duration BLOCK_TIMEOUT = Duration.ofSeconds(2);
    /** 每次批量读取条数 */
    private static final int BATCH_SIZE = 16;
    /** 处理器失败最大重试次数 */
    private static final int MAX_RETRIES = 3;
    /** 断线重连间隔 */
    private static final long RECONNECT_INTERVAL_MS = 1000;

    private final RedisClient client;
    private final RedisURI redisUri;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String consumerId = "consumer-" + UUID.randomUUID().toString().substring(0, 8);
    private final RetryTemplate sendRetry = new RetryTemplate(3, 500);

    /** topic → 处理器（每个 topic 单处理器） */
    private final Map<String, TypedHandler<?>> handlers = new ConcurrentHashMap<>();
    /** topic → 消费者线程 */
    private final Map<String, Thread> consumerThreads = new ConcurrentHashMap<>();

    /** 发送共享连接（懒加载，Redis 掉线后自动重建） */
    private volatile StatefulRedisConnection<String, String> sendConn;

    private record TypedHandler<T>(Class<T> type, Consumer<T> consumer) {}

    public RedisStreamMqAdapter(String redisUri) {
        this(RedisURI.create(redisUri));
    }

    public RedisStreamMqAdapter(RedisURI redisUri) {
        this.redisUri = redisUri;
        this.client = RedisClient.create(redisUri);
    }

    /**
     * 从环境创建适配器：优先环境变量 {@code REDIS_URL}，其次系统属性 {@code redis.url}，默认本地 6379。
     */
    public static RedisStreamMqAdapter fromEnv() {
        String url = System.getenv("REDIS_URL");
        if (url == null || url.isBlank()) {
            url = System.getProperty("redis.url");
        }
        if (url == null || url.isBlank()) {
            url = "redis://127.0.0.1:6379";
        }
        return new RedisStreamMqAdapter(url);
    }

    /** 当前连接的 Redis 地址（用于日志） */
    public String redisUri() {
        return redisUri.toString();
    }

    // ==================== MqProducer ====================

    @Override
    public void send(String topic, Object message) {
        try {
            String json = objectMapper.writeValueAsString(new MqMessage(message));
            sendRetry.execute(() -> {
                sendCommands().xadd(streamKey(topic), Map.of("data", json));
                return null;
            });
            log.debug("[RedisMQ] send: topic={}, type={}", topic, message.getClass().getSimpleName());
        } catch (Exception e) {
            log.error("[RedisMQ] send error: topic={}, error={}", topic, e.getMessage());
        }
    }

    @Override
    public void sendAsync(String topic, Object message, Runnable callback) {
        send(topic, message);
        if (callback != null) {
            callback.run();
        }
    }

    // ==================== MqConsumer / TypedMqConsumer ====================

    @Override
    public void subscribe(String topic, Consumer<Object> handler) {
        subscribeTyped(topic, Object.class, handler);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void subscribeTyped(String topic, Class<T> type, Consumer<T> handler) {
        handlers.put(topic, new TypedHandler<>(type, (Consumer<T>) handler));
        Thread thread = new Thread(() -> consumeLoop(topic), "mq-consumer-" + topic);
        thread.setDaemon(true);
        consumerThreads.put(topic, thread);
        thread.start();
        log.info("[RedisMQ] subscribe: topic={}, type={}", topic, type.getSimpleName());
    }

    @Override
    public void unsubscribe(String topic) {
        handlers.remove(topic);
        Thread thread = consumerThreads.remove(topic);
        if (thread != null) {
            thread.interrupt();
        }
        log.info("[RedisMQ] unsubscribe: topic={}", topic);
    }

    // ==================== 消费循环 ====================

    /**
     * 消费循环：连接 Redis → 确保消费者组存在 → XREADGROUP 阻塞读取。
     * 连接失败或 Redis 重启时，等待 {@link #RECONNECT_INTERVAL_MS} 后重连。
     */
    private void consumeLoop(String topic) {
        while (!Thread.currentThread().isInterrupted()) {
            StatefulRedisConnection<String, String> conn = null;
            try {
                conn = client.connect();
                RedisCommands<String, String> commands = conn.sync();
                ensureConsumerGroup(commands, topic);
                log.info("[RedisMQ] consumer ready: topic={}, group={}", topic, GROUP);

                while (!Thread.currentThread().isInterrupted()) {
                    List<StreamMessage<String, String>> records = commands.xreadgroup(
                            io.lettuce.core.Consumer.from(GROUP, consumerId),
                            XReadArgs.Builder.count(BATCH_SIZE).block(BLOCK_TIMEOUT),
                            XReadArgs.StreamOffset.lastConsumed(streamKey(topic)));
                    if (records == null || records.isEmpty()) {
                        continue;
                    }
                    for (StreamMessage<String, String> record : records) {
                        processMessage(topic, record, commands);
                    }
                }
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                log.error("[RedisMQ] consume error: topic={}, error={}", topic, e.getMessage());
                try {
                    Thread.sleep(RECONNECT_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } finally {
                if (conn != null) {
                    conn.close();
                }
            }
        }
        log.info("[RedisMQ] consumer stopped: topic={}", topic);
    }

    /**
     * 确保消费者组存在（MKSTREAM：流不存在时自动创建）。
     * 组已存在时 Redis 返回 BUSYGROUP，忽略即可。
     */
    private void ensureConsumerGroup(RedisCommands<String, String> commands, String topic) {
        try {
            commands.xgroupCreate(
                    XReadArgs.StreamOffset.from(streamKey(topic), "0-0"),
                    GROUP,
                    XGroupCreateArgs.Builder.mkstream(true));
        } catch (Exception e) {
            log.debug("[RedisMQ] consumer group already exists: topic={}, msg={}", topic, e.getMessage());
        }
    }

    /**
     * 处理单条消息：反序列化 → 调用处理器 → 成功后 XACK。
     * 处理器失败时指数退避重试 {@link #MAX_RETRIES} 次，仍失败则 ACK 丢弃并记录错误。
     */
    @SuppressWarnings("unchecked")
    private void processMessage(String topic, StreamMessage<String, String> record,
                                RedisCommands<String, String> commands) {
        TypedHandler<?> handler = handlers.get(topic);
        String json = record.getBody().get("data");
        if (handler == null || json == null) {
            commands.xack(streamKey(topic), GROUP, record.getId());
            return;
        }

        int attempt = 0;
        while (attempt <= MAX_RETRIES) {
            try {
                MqMessage envelope = objectMapper.readValue(json, MqMessage.class);
                Object typed = convertPayload(envelope.getPayload(), handler.type);
                ((Consumer<Object>) handler.consumer).accept(typed);
                commands.xack(streamKey(topic), GROUP, record.getId());
                return;
            } catch (Exception e) {
                attempt++;
                if (attempt > MAX_RETRIES) {
                    log.error("[RedisMQ] handler failed after {} retries: topic={}, error={}",
                            MAX_RETRIES, topic, e.getMessage());
                    commands.xack(streamKey(topic), GROUP, record.getId());
                    return;
                }
                log.warn("[RedisMQ] handler retry {}/{}: topic={}, error={}",
                        attempt, MAX_RETRIES, topic, e.getMessage());
                try {
                    Thread.sleep(1000L * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /**
     * 将 payload 转换为处理器期望的类型。
     * Jackson 反序列化 MqMessage 后 payload 为 Map，需要 convertValue 到目标 record 类型。
     */
    private Object convertPayload(Object payload, Class<?> type) {
        if (type == Object.class || type.isInstance(payload)) {
            return payload;
        }
        return objectMapper.convertValue(payload, type);
    }

    // ==================== 连接管理 ====================

    private RedisCommands<String, String> sendCommands() {
        StatefulRedisConnection<String, String> conn = sendConn;
        if (conn == null || !conn.isOpen()) {
            synchronized (this) {
                conn = sendConn;
                if (conn == null || !conn.isOpen()) {
                    conn = client.connect();
                    sendConn = conn;
                }
            }
        }
        return conn.sync();
    }

    private static String streamKey(String topic) {
        return STREAM_PREFIX + topic;
    }

    /**
     * 关闭适配器：停止消费者线程、关闭连接、释放 RedisClient。
     * 由服务入口在 stop() 时调用。
     */
    public void close() {
        for (Thread thread : consumerThreads.values()) {
            thread.interrupt();
        }
        consumerThreads.clear();
        handlers.clear();
        StatefulRedisConnection<String, String> conn = sendConn;
        if (conn != null) {
            conn.close();
        }
        client.shutdown();
        log.info("[RedisMQ] adapter closed");
    }
}
