package com.tzw.growth;

/**
 * 养成逻辑服启动入口（空骨架）。
 *
 * <p>后续迁入：GrowthServer / GrowthRouter / GrowthActor / GrowthSessionManager 等组件。
 * 养成系统通过 TCP 长连接处理升级、装备、抽卡、匹配等业务。
 */
public final class GrowthApplication {
    private GrowthApplication() {}

    public static void main(String[] args) {
        System.out.println("[growthServer] started (skeleton, Java " + Runtime.version() + ")");
        // TODO: 初始化 Netty TCP 服务器、GrowthRouter、GrowthSessionManager
    }
}
