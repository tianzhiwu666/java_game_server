package com.tzw.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * 进程内事件总线 —— 模块间异步通信的桥梁。
 *
 * <p>Phase 1 使用进程内事件总线，Phase 2 可替换为 Kafka/RocketMQ 等真实 MQ。
 * 设计为可替换的接口，切换时只需修改实现，订阅方代码不变。
 *
 * <h3>设计模式</h3>
 * <ul>
 *   <li>发布-订阅模式：发布者发布事件，订阅者异步接收</li>
 *   <li>类型安全：按事件类型注册订阅者，编译期类型检查</li>
 *   <li>同步分发：事件在发布线程同步分发给所有订阅者</li>
 * </ul>
 *
 * <h3>线程安全</h3>
 * <p>使用 {@link ConcurrentHashMap} + {@link CopyOnWriteArrayList} 保证并发安全。
 * 订阅者列表使用 CopyOnWriteArrayList，避免遍历时的 ConcurrentModificationException。
 *
 * <h3>使用示例</h3>
 * <pre>
 * // 订阅
 * eventBus.subscribe(MatchResultEvent.class, event -> {
 *     // 处理匹配结果
 * });
 *
 * // 发布
 * eventBus.publish(new MatchResultEvent(roomId, results));
 * </pre>
 */
public class EventBus {

    private static final Logger log = LoggerFactory.getLogger(EventBus.class);

    /** 事件类型 → 订阅者列表 */
    private final Map<Class<?>, List<Consumer<?>>> subscribers = new ConcurrentHashMap<>();

    /**
     * 订阅事件
     *
     * @param eventType 事件类型
     * @param handler   事件处理器
     * @param <T>       事件类型
     */
    @SuppressWarnings("unchecked")
    public <T> void subscribe(Class<T> eventType, Consumer<T> handler) {
        subscribers.computeIfAbsent(eventType, k -> new CopyOnWriteArrayList<>())
                   .add(handler);
        log.debug("[EventBus] subscribe: {}", eventType.getSimpleName());
    }

    /**
     * 发布事件
     *
     * <p>同步分发给所有订阅者。如果订阅者抛出异常，记录日志但不影响其他订阅者。
     *
     * @param event 事件对象
     * @param <T>   事件类型
     */
    @SuppressWarnings("unchecked")
    public <T> void publish(T event) {
        List<Consumer<?>> handlers = subscribers.get(event.getClass());
        if (handlers == null || handlers.isEmpty()) {
            log.debug("[EventBus] no subscriber for: {}", event.getClass().getSimpleName());
            return;
        }

        for (Consumer<?> handler : handlers) {
            try {
                ((Consumer<T>) handler).accept(event);
            } catch (Exception e) {
                log.error("[EventBus] handler error for: {}, error: {}",
                        event.getClass().getSimpleName(), e.getMessage(), e);
            }
        }
    }

    /**
     * 获取指定事件类型的订阅者数量（用于测试和监控）
     *
     * @param eventType 事件类型
     * @return 订阅者数量
     */
    public int subscriberCount(Class<?> eventType) {
        List<Consumer<?>> handlers = subscribers.get(eventType);
        return handlers == null ? 0 : handlers.size();
    }

    /**
     * 清除所有订阅（主要用于测试）
     */
    public void clear() {
        subscribers.clear();
    }
}
