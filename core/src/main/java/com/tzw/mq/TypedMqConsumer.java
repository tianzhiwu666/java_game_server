package com.tzw.mq;

import java.util.function.Consumer;

/**
 * 类型安全 MQ 消费者接口。
 *
 * <p>支持反序列化为指定类型的消息订阅。
 */
public interface TypedMqConsumer {

    /**
     * 订阅指定 Topic 的消息（类型安全）
     *
     * @param topic   主题
     * @param type    消息类型 Class
     * @param handler 类型安全的消息处理器
     * @param <T>     消息类型
     */
    <T> void subscribeTyped(String topic, Class<T> type, Consumer<T> handler);
}
