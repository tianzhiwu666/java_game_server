package com.tzw.server;

import com.tzw.logic.RoomManager;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 帧同步服务器 —— 战斗服的顶层组件。
 *
 * <p>持有 {@link RoomManager} 和全局连接计数，是网络层与逻辑层的桥梁。
 * Router 通过此类访问 RoomManager 和连接计数。
 *
 * <p>无 Spring，由 {@link com.tzw.battle.BattleServer} 手动装配。
 */
public class LockStepServer {

    /** 房间管理器，管理所有活跃房间 */
    private final RoomManager roomManager;

    /** 全局连接计数（仅用于监控） */
    private final AtomicLong totalConn;

    public LockStepServer(RoomManager roomManager, AtomicLong totalConn) {
        this.roomManager = roomManager;
        this.totalConn = totalConn;
    }

    public RoomManager roomManager() {
        return roomManager;
    }

    /**
     * 增加全局连接计数
     *
     * @return 增加后的计数值
     */
    public long incrementTotalConn() {
        return totalConn.incrementAndGet();
    }

    /**
     * 减少全局连接计数
     *
     * @return 减少后的计数值
     */
    public long decrementTotalConn() {
        return totalConn.decrementAndGet();
    }
}
