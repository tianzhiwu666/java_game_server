package com.tzw.logic.game;

/**
 * 游戏事件回调接口 —— Game 与 Room 之间的解耦桥梁。
 *
 * <p>镜像 Go 参考实现中的 {@code game.gameListener} 接口。
 * 定义了游戏逻辑向房间层通知事件的回调方法。
 *
 * <h3>设计目的</h3>
 * <p>该接口实现了<b>回调模式</b>，将 {@link Game}（游戏逻辑）与 {@link com.tzw.logic.room.Room}（房间容器）解耦：
 * <ul>
 *   <li>{@link Game} 只依赖接口，不依赖 {@link com.tzw.logic.room.Room} 的具体实现</li>
 *   <li>{@link com.tzw.logic.room.Room} 实现该接口，接收游戏事件</li>
 *   <li>便于单元测试：可以用 mock 实现测试 Game</li>
 * </ul>
 *
 * <h3>回调事件</h3>
 * <ul>
 *   <li>{@link #onJoinGame} — 玩家加入游戏</li>
 *   <li>{@link #onGameStart} — 游戏开始</li>
 *   <li>{@link #onLeaveGame} — 玩家离开游戏</li>
 *   <li>{@link #onGameOver} — 游戏结束</li>
 * </ul>
 *
 * <h3>线程安全</h3>
 * <p>所有回调方法在房间单线程中执行（由 {@link Game} 调用），
 * 因此实现方无需额外同步。
 */
public interface GameListener {

    /**
     * 玩家加入游戏时调用。
     *
     * <p>在 {@link Game#joinGame} 成功后触发。
     * 可用于通知匹配系统、更新房间元数据等。
     *
     * @param roomId 房间 ID
     * @param playerId 加入的玩家 ID
     */
    void onJoinGame(long roomId, long playerId);

    /**
     * 游戏开始时调用。
     *
     * <p>在 {@link Game#doStart} 中触发，所有玩家已准备或超时强制开始。
     * 可用于记录游戏开始日志、触发观战系统等。
     *
     * @param roomId 房间 ID
     */
    void onGameStart(long roomId);

    /**
     * 玩家离开游戏时调用。
     *
     * <p>在 {@link Game#leaveGame} 中触发。
     * 可用于通知匹配系统玩家已离开。
     *
     * @param roomId 房间 ID
     * @param playerId 离开的玩家 ID
     */
    void onLeaveGame(long roomId, long playerId);

    /**
     * 游戏结束时调用。
     *
     * <p>在 {@link Game#doGameOver} 中触发。
     * 房间层据此设置 {@link com.tzw.logic.room.Room#closed} 标志，阻止新连接加入。
     *
     * @param roomId 房间 ID
     */
    void onGameOver(long roomId);
}
