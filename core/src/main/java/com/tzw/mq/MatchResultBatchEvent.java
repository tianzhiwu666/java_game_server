package com.tzw.mq;

import java.util.List;

/**
 * 批量匹配结果事件（一局游戏结束后）
 *
 * <p>包含所有参与玩家的结果。
 */
public record MatchResultBatchEvent(
        long roomId,                    // 房间 ID
        List<MatchResultEvent> results  // 所有玩家的结果列表
) {}
