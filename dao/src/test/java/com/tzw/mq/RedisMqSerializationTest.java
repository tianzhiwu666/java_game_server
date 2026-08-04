package com.tzw.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * MQ 消息信封 JSON 序列化往返测试。
 *
 * <p>验证 record 事件经 Jackson 序列化/反序列化后字段不丢失，
 * 这是 Redis Stream MQ 适配器消息传递正确性的基础。
 */
class RedisMqSerializationTest {

    @Test
    void roundTripMatchCreateEvent() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        MatchCreateEvent original = new MatchCreateEvent(42L, 1);

        MqMessage envelope = new MqMessage(original);
        String json = mapper.writeValueAsString(envelope);
        MqMessage decoded = mapper.readValue(json, MqMessage.class);

        assertEquals("com.tzw.mq.MatchCreateEvent", decoded.getType());
        MatchCreateEvent payload = mapper.convertValue(decoded.getPayload(), MatchCreateEvent.class);
        assertEquals(42L, payload.playerId());
        assertEquals(1, payload.mode());
    }

    @Test
    void roundTripMatchReadyEvent() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        MatchReadyEvent original = new MatchReadyEvent(1001L, 7L, "127.0.0.1", 10086, "token-abc");

        MqMessage envelope = new MqMessage(original);
        String json = mapper.writeValueAsString(envelope);
        MqMessage decoded = mapper.readValue(json, MqMessage.class);

        MatchReadyEvent payload = mapper.convertValue(decoded.getPayload(), MatchReadyEvent.class);
        assertEquals(1001L, payload.roomId());
        assertEquals(7L, payload.playerId());
        assertEquals("127.0.0.1", payload.roomHost());
        assertEquals(10086, payload.roomPort());
        assertEquals("token-abc", payload.token());
    }

    @Test
    void roundTripMatchResultEvent() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        MatchResultEvent original = new MatchResultEvent(1001L, 7L, true, 100, 50, 3);

        MqMessage envelope = new MqMessage(original);
        String json = mapper.writeValueAsString(envelope);
        MqMessage decoded = mapper.readValue(json, MqMessage.class);

        MatchResultEvent payload = mapper.convertValue(decoded.getPayload(), MatchResultEvent.class);
        assertEquals(1001L, payload.roomId());
        assertEquals(7L, payload.playerId());
        assertEquals(true, payload.win());
        assertEquals(100, payload.expGain());
        assertEquals(50, payload.goldGain());
    }
}
