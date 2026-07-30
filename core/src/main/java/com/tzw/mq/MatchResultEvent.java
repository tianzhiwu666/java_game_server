package com.tzw.mq;

/**
 * 匹配结果事件
 *
 * <p>房间服务发布此事件，通知养成服务对战结束，发放奖励。
 */
public record MatchResultEvent(
        long roomId,                    // 房间 ID
        long playerId,                  // 玩家 ID
        boolean win,                    // 是否获胜
        long expGain,                   // 获得经验
        long goldGain,                  // 获得金币
        int newLevel                    // 结算后的等级
) {}
