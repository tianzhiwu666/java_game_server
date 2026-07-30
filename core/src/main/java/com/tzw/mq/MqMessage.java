package com.tzw.mq;

import java.io.Serializable;

/**
 * MQ 消息信封 —— 包含类型信息的消息包装。
 *
 * <p>解决 Redis Pub/Sub 消息反序列化时的类型问题。
 */
public class MqMessage implements Serializable {

    /** 消息类型的全限定类名 */
    private String type;

    /** 消息 payload */
    private Object payload;

    public MqMessage() {}

    public MqMessage(Object payload) {
        this.type = payload.getClass().getName();
        this.payload = payload;
    }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public Object getPayload() { return payload; }
    public void setPayload(Object payload) { this.payload = payload; }

    @Override
    public String toString() {
        return String.format("MqMessage{type='%s'}", type);
    }
}
