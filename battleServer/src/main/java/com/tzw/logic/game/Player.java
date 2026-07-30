package com.tzw.logic.game;

import com.tzw.network.Conn;
import com.tzw.network.Packet;

/**
 * 每玩家状态 —— 房间内单个玩家的所有状态信息。
 *
 * <p>镜像 Go 参考实现中的 {@code logic/game/player.go}。
 * 每个玩家对象代表房间内一个参与者的完整状态。
 *
 * <h3>状态字段</h3>
 * <ul>
 *   <li><b>id</b>：玩家唯一标识</li>
 *   <li><b>idx</b>：座位索引（1~N），用于客户端显示和协议中的 roomseatid</li>
 *   <li><b>isReady</b>：是否已准备（客户端加载完成后发送 Ready）</li>
 *   <li><b>isOnline</b>：是否在线（连接是否活跃）</li>
 *   <li><b>loadingProgress</b>：加载进度（0~100），客户端报告资源加载进度</li>
 *   <li><b>lastHeartbeatTime</b>：上次心跳时间（秒），用于检测网络状况</li>
 *   <li><b>sendFrameCount</b>：已发送给该玩家的帧游标，用于增量帧广播</li>
 *   <li><b>client</b>：网络连接对象</li>
 * </ul>
 *
 * <h3>sendFrameCount 游标</h3>
 * <p>这是帧广播的关键优化。每个玩家独立维护一个游标，记录已发送到的帧号。
 * 下次广播时只发送新帧，避免重复发送。
 *
 * <p>游标在以下情况更新：
 * <ul>
 *   <li>每次成功发送帧数据后（{@link Game#broadcastFrameData}）</li>
 *   <li>断线重连后（{@link Game#doReconnect}）</li>
 * </ul>
 *
 * <h3>线程安全</h3>
 * <p><b>此类只在房间单线程中访问，无需额外同步。</b>
 * 所有方法都由 {@link Game} 调用，而 {@link Game} 在房间单线程中运行。
 */
public class Player {

    /** 玩家唯一标识 */
    private final long id;

    /** 座位索引（1~N），用于客户端显示和协议中的 roomseatid */
    private final int idx;

    /** 是否已准备（客户端加载完成后发送 Ready） */
    private boolean isReady;

    /** 是否在线（连接是否活跃） */
    private boolean isOnline;

    /** 加载进度（0~100），客户端报告资源加载进度 */
    private int loadingProgress;

    /** 上次心跳时间（秒），用于检测网络状况 */
    private long lastHeartbeatTime;

    /**
     * 已发送给该玩家的帧游标。
     *
     * <p>用于增量帧广播：每次只发送 {@code sendFrameCount} 之后的新帧。
     * 这是帧广播的关键优化，避免重复发送已确认的帧。
     */
    private long sendFrameCount;

    /** 网络连接对象 */
    private Conn client;

    public Player(long id, int idx) {
        this.id = id;
        this.idx = idx;
    }

    /**
     * 连接建立。
     *
     * <p>在玩家加入游戏时调用（{@link Game#joinGame}）。
     * 重置玩家状态：在线、未准备、刷新心跳时间。
     *
     * @param conn 网络连接
     */
    public void connect(Conn conn) {
        this.client = conn;
        this.isOnline = true;
        this.isReady = false;
        this.lastHeartbeatTime = System.currentTimeMillis() / 1000;
    }

    /**
     * 检查玩家是否在线。
     *
     * <p>需要同时满足：连接对象非空且 isOnline 标志为 true。
     *
     * @return true 如果玩家在线
     */
    public boolean isOnline() {
        return client != null && isOnline;
    }

    /**
     * 刷新心跳时间。
     *
     * <p>在收到心跳消息时调用（{@link Game#handleHeartbeat}）。
     * 用于检测玩家网络状况：如果长时间未收到心跳，视为网络不佳。
     */
    public void refreshHeartbeatTime() {
        this.lastHeartbeatTime = System.currentTimeMillis() / 1000;
    }

    public long getLastHeartbeatTime() {
        return lastHeartbeatTime;
    }

    public void setSendFrameCount(long count) {
        this.sendFrameCount = count;
    }

    public long getSendFrameCount() {
        return sendFrameCount;
    }

    /**
     * 发送消息（异步写入，失败则关闭连接）。
     *
     * <p>通过 {@link Conn#asyncWrite} 异步写入，不阻塞房间线程。
     * 如果写入失败（通常是连接已关闭），主动关闭连接。
     *
     * @param msg 要发送的数据包
     */
    public void sendMessage(Packet msg) {
        if (!isOnline()) {
            return;
        }
        if (!client.asyncWrite(msg)) {
            // 写入失败，连接可能已关闭
            client.close();
        }
    }

    /**
     * 清理（断开连接时调用）。
     *
     * <p>关闭网络连接并重置状态。由 {@link Game#leaveGame} 调用。
     */
    public void cleanup() {
        if (client != null) {
            client.close();
        }
        client = null;
        isReady = false;
        isOnline = false;
    }

    // ==================== Getters ====================

    public long getId() { return id; }
    public int getIdx() { return idx; }
    public boolean isReady() { return isReady; }
    public void setReady(boolean ready) { isReady = ready; }
    public int getLoadingProgress() { return loadingProgress; }
    public void setLoadingProgress(int progress) { this.loadingProgress = progress; }
    public Conn getClient() { return client; }
}
