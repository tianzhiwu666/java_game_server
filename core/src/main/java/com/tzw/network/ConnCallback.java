package com.tzw.network;

/**
 * ============================================================
 *  连接回调接口 — 网络事件的通知机制
 * ============================================================
 *
 * 【作用】
 * 当网络层发生事件时（连接建立、收到数据、连接关闭），通过此接口通知上层业务逻辑。
 * 这是观察者模式的应用：网络层定义事件，业务层实现处理。
 *
 * 【实现者】
 * - {@link com.tzw.server.Router}：第一层分发，处理 Connect 和 Heartbeat
 * - {@link com.tzw.logic.room.Room}：第二层分发，处理游戏内消息
 *
 * 【回调链】
 * 1. 新连接进来 → Router.onConnect() → 计数
 * 2. 收到数据 → Router.onMessage() → 判断消息类型：
 *    - MSG_Connect / MSG_Heartbeat → Router 自己处理
 *    - 其他 → 返回 false → Conn 关闭回调链 → Room 接管
 * 3. Room.onConnect() 设置回调为 Room 自身
 * 4. 后续数据 → Room.onMessage() → 放入 msgQ → 房间线程处理
 *
 * 【镜像 Go】
 * Go 的 {@code network.ConnCallback}：OnConnect / OnMessage / OnClose
 */
public interface ConnCallback {

    /**
     * 连接建立时调用
     *
     * @param conn 新建立的连接
     * @return true 继续处理，false 关闭连接
     */
    boolean onConnect(Conn conn);

    /**
     * 收到数据包时调用
     *
     * @param conn 连接
     * @param packet 收到的数据包
     * @return true 继续处理，false 关闭连接
     */
    boolean onMessage(Conn conn, Packet packet);

    /**
     * 连接关闭时调用
     *
     * @param conn 已关闭的连接
     */
    void onClose(Conn conn);
}
