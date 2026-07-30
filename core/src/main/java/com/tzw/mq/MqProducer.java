package com.tzw.mq;

/**
 * MQ 生产者接口 —— 发送消息到消息队列。
 *
 * <p>抽象接口，支持多种 MQ 实现（RocketMQ、Kafka 等）。
 * Phase 3 先使用内存实现，后期可替换为真实 MQ。
 */
public interface MqProducer {

    /**
     * 发送消息到指定 Topic
     *
     * @param topic   主题（如 match.create, match.result）
     * @param message 消息对象（需可序列化）
     */
    void send(String topic, Object message);

    /**
     * 异步发送消息（回调通知结果）
     *
     * @param topic   主题
     * @param message 消息对象
     * @param callback 发送结果回调
     */
    void sendAsync(String topic, Object message, Runnable callback);
}
