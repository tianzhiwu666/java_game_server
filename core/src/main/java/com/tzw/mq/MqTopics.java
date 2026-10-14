package com.tzw.mq;

/**
 * MQ Topic 常量 —— 养成服与战斗服之间的事件主题约定。
 *
 * <p>值为编译期常量字符串，可直接用作 switch 标签。
 * wire 格式契约（Redis Stream 的 stream key / InMemoryMq 的 topic 名）：
 * 替换实现或重构时这些字符串不得变化，否则跨服匹配链路断裂。
 */
public final class MqTopics {

    /** 养成服 → 战斗服：请求创建匹配房间 */
    public static final String MATCH_CREATE = "match.create";

    /** 战斗服 → 养成服：房间就绪，通知玩家进入战斗 */
    public static final String MATCH_READY = "match.ready";

    /** 战斗服 → 养成服：对战结算，发放奖励 */
    public static final String MATCH_RESULT = "match.result";

    private MqTopics() {
    }
}
