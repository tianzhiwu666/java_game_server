package com.tzw.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * 内存 MQ 适配器 —— Phase 3 临时实现。
 *
 * <p>基于内存的消息队列，模拟 MQ 的发布-订阅语义。
 * 支持多订阅者、异步分发。
 *
 * <h3>替换为真实 MQ</h3>
 * <p>后期替换为 RocketMQ/Kafka 时，只需实现 MqProducer/MqConsumer 接口，
 * 修改配置中的 Bean 创建即可。
 *
 * <h3>线程安全</h3>
 * <p>使用 ConcurrentHashMap + CopyOnWriteArrayList 保证并发安全。
 */
public class InMemoryMqAdapter implements MqProducer, MqConsumer, TypedMqConsumer {

    private static final Logger log = LoggerFactory.getLogger(InMemoryMqAdapter.class);

    /** Topic → 订阅者列表 */
    private final Map<String, List<Consumer<Object>>> subscribers = new ConcurrentHashMap<>();

    /** 异步分发线程池 */
    private final ExecutorService dispatcher = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(),
            r -> new Thread(r, "mq-dispatcher"));

    @Override
    public void send(String topic, Object message) {
        dispatch(topic, message);
    }

    @Override
    public void sendAsync(String topic, Object message, Runnable callback) {
        dispatcher.submit(() -> {
            try {
                dispatch(topic, message);
                if (callback != null) {
                    callback.run();
                }
            } catch (Exception e) {
                log.error("[MQ] async send error: topic={}, error={}", topic, e.getMessage());
            }
        });
    }

    @Override
    public void subscribe(String topic, Consumer<Object> handler) {
        subscribers.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(handler);
        log.info("[MQ] subscribe: {}", topic);
    }

    @Override
    public <T> void subscribeTyped(String topic, Class<T> type, Consumer<T> handler) {
        // 内存实现：类型信息仅作记录，分发时按 Object 处理
        subscribe(topic, (Consumer<Object>) handler);
        log.info("[MQ] subscribeTyped: topic={}, type={}", topic, type.getSimpleName());
    }

    @Override
    public void unsubscribe(String topic) {
        subscribers.remove(topic);
        log.info("[MQ] unsubscribe: {}", topic);
    }

    /**
     * 分发消息到所有订阅者
     */
    private void dispatch(String topic, Object message) {
        List<Consumer<Object>> handlers = subscribers.get(topic);
        if (handlers == null || handlers.isEmpty()) {
            log.warn("[MQ] no subscriber for topic: {}", topic);
            return;
        }

        for (Consumer<Object> handler : handlers) {
            try {
                handler.accept(message);
            } catch (Exception e) {
                log.error("[MQ] handler error: topic={}, error={}", topic, e.getMessage(), e);
            }
        }
    }

    /**
     * 获取 Topic 的订阅者数量（用于监控）
     */
    public int subscriberCount(String topic) {
        List<Consumer<Object>> handlers = subscribers.get(topic);
        return handlers == null ? 0 : handlers.size();
    }
}
