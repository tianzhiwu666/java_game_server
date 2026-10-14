package com.tzw.logic.growth;

import com.tzw.network.Conn;
import com.tzw.packet.MsgPacket;
import com.tzw.pb.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 发送职责 —— 持有玩家当前 TCP 连接，统一向客户端发包与数据推送。
 *
 * <p>从 {@link GrowthActor} 拆出（逐行搬移，行为不变）：
 * Actor 线程消费 inChan/outChan 时调用 {@link #bind}/{@link #unbind}，
 * 各 Handler 通过 {@link #send} 回包。
 *
 * <p>{@code volatile conn} 保持原 GrowthActor 的可见性语义。
 */
public class GrowthMessenger {

    private static final Logger log = LoggerFactory.getLogger(GrowthMessenger.class);

    /** 玩家 ID（仅用于日志前缀，与拆分前保持一致） */
    private final long playerId;

    /** 玩家养成状态（推送 PlayerData/Inventory 时读取） */
    private final PlayerGrowthState state;

    /** 当前 TCP 连接（可能为空：玩家可能暂时断线） */
    private volatile Conn conn;

    public GrowthMessenger(long playerId, PlayerGrowthState state) {
        this.playerId = playerId;
        this.state = state;
    }

    /** 绑定新连接（Actor 线程消费 inChan 时调用） */
    public void bind(Conn in) {
        this.conn = in;
    }

    /** 解绑连接：仅当传入连接是当前绑定的连接时才置空 */
    public void unbind(Conn out) {
        if (this.conn == out) {
            this.conn = null;
        }
    }

    /**
     * 推送玩家数据给客户端
     */
    public void pushPlayerData() {
        send(MsgPacket.newInstance((byte) Message.ID.MSG_PlayerData_VALUE,
                state.toProto().build()));
    }

    /**
     * 推送背包数据给客户端
     */
    public void pushInventory() {
        Message.S2C_InventoryMsg inventory = Message.S2C_InventoryMsg.newBuilder()
                .addAllItems(state.getInventory().values())
                .build();
        send(MsgPacket.newInstance((byte) Message.ID.MSG_Inventory_VALUE, inventory));
    }

    /**
     * 发送消息给客户端
     *
     * @param msg 消息包
     */
    public void send(MsgPacket msg) {
        if (conn != null && !conn.isClosed()) {
            conn.asyncWrite(msg);
        }
    }
}
