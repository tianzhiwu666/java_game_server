package com.tzw.mq;

import java.util.function.Consumer;

/**
 * MQ 消费者接口 —— 订阅消息队列中的消息。
 *
 * <p>抽象接口，支持多种 MQ 实现（RocketMQ、Kafka 等）。
 */
public interface MqConsumer {

    /**
     * 订阅指定 Topic 的消息
     *
     * @param topic   主题
     * @param handler 消息处理器
     */
    void subscribe(String topic, Consumer<Object> handler);

    /**
     * 取消订阅
     *
     * @param topic 主题
     */
    void unsubscribe(String topic);
}
