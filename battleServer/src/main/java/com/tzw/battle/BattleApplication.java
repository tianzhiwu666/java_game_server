package com.tzw.battle;

/**
 * 战斗服启动入口（空骨架）。
 *
 * <p>后续迁入：LockStepServer / RoomManager / Router / MatchService 等组件。
 * 战斗系统通过 KCP 可靠 UDP 实现 30Hz 帧同步对战。
 */
public final class BattleApplication {
    private BattleApplication() {}

    public static void main(String[] args) {
        System.out.println("[battleServer] started (skeleton, Java " + Runtime.version() + ")");
        // TODO: 初始化 KCP 服务器、RoomManager、MatchService
    }
}
