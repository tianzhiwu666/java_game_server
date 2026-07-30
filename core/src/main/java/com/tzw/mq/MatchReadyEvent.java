package com.tzw.mq;

/**
 * 匹配就绪事件
 *
 * <p>房间服务发布此事件，通知养成服务房间已创建，玩家可以进入。
 */
public record MatchReadyEvent(
        long roomId,        // 房间 ID
        long playerId,      // 目标玩家 ID
        String roomHost,    // 房间服务地址
        int roomPort,       // 房间服务端口
        String token        // 进入房间的令牌
) {}
