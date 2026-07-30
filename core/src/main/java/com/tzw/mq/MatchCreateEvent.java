package com.tzw.mq;

/**
 * 匹配创建事件
 *
 * <p>养成服务发布此事件，请求房间服务创建对战房间。
 */
public record MatchCreateEvent(
        long playerId,      // 请求匹配的玩家 ID
        int mode            // 匹配模式（1=单人匹配）
) {}
