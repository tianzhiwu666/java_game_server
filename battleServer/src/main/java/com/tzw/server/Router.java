package com.tzw.server;

import com.tzw.network.Conn;
import com.tzw.network.ConnCallback;
import com.tzw.network.Packet;
import com.tzw.packet.MsgPacket;
import com.tzw.pb.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消息路由器 —— 第一层消息分发。
 *
 * <p>镜像 Go 参考实现中的 {@code server/router.go}。
 * 实现 {@link ConnCallback} 接口，是网络层与逻辑层之间的桥梁。
 *
 * <h3>两层路由架构</h3>
 * <p>帧同步服务器采用两层路由设计：
 * <ol>
 *   <li><b>Router（本类）</b>：运行在 Netty 线程中，处理与房间无关的消息：
 *       <ul>
 *         <li>{@code MSG_Connect} — 连接握手，校验房间存在性、状态、玩家归属和 token</li>
 *         <li>{@code MSG_Heartbeat} — 心跳，原样回复</li>
 *       </ul>
 *       对于其他消息，返回 false 表示"不处理"，由上层（Room 事件循环）接管。</li>
 *   <li><b>Room 事件循环</b>：运行在房间单线程中，处理所有游戏逻辑消息：
 *       {@code MSG_JoinRoom}、{@code MSG_Progress}、{@code MSG_Ready}、
 *       {@code MSG_Input}、{@code MSG_Result}。</li>
 * </ol>
 *
 * <h3>为什么分两层</h3>
 * <p>Connect 握手必须在房间事件循环外处理，因为此时玩家尚未加入房间。
 * 而游戏逻辑消息必须在房间单线程内处理，以保证帧同步的确定性。
 * 这种分层确保了线程安全：Netty 线程只读房间元数据（房间是否存在、是否结束），
 * 不修改游戏状态。
 *
 * <h3>Connect 握手流程</h3>
 * <ol>
 *   <li>解析 protobuf 消息获取 playerID、roomID、token</li>
 *   <li>检查房间是否存在（{@link RoomManager#getRoom}）</li>
 *   <li>检查房间是否已结束（{@link com.tzw.logic.room.Room#isOver}）</li>
 *   <li>检查玩家是否属于该房间（{@link com.tzw.logic.room.Room#hasPlayer}）</li>
 *   <li>验证 token（当前为桩函数）</li>
 *   <li>将 playerID 存入连接的附加数据（{@link Conn#putExtraData}）</li>
 *   <li>委托给 {@link com.tzw.logic.room.Room#onConnect} 进入房间事件队列</li>
 * </ol>
 *
 * <h3>线程安全</h3>
 * <p>本类运行在 Netty 线程中，不直接修改 {@link com.tzw.logic.game.Game} 状态。
 * 所有游戏状态修改都通过 {@code Room.onConnect} 间接完成，
 * 最终由房间单线程处理。
 */
public class Router implements ConnCallback {

    private static final Logger log = LoggerFactory.getLogger(Router.class);

    /** 顶层服务器引用，用于访问 RoomManager 和连接计数 */
    private final LockStepServer server;

    public Router(LockStepServer server) {
        this.server = server;
    }

    /**
     * TODO 桩函数，直接返回输入（身份验证尚未实现）。
     *
     * <p>在 Go 参考实现中，{@code verifyToken} 也是桩函数。
     * 生产环境应替换为 JWT 验证或其他身份认证机制。
     *
     * @param secret 原始 token
     * @return 验证后的 token（当前直接返回输入）
     */
    private static String verifyToken(String secret) {
        return secret;
    }

    /**
     * 连接建立回调。
     *
     * <p>增加全局连接计数并记录日志。返回 true 表示接受连接。
     *
     * @param conn 新建立的连接
     * @return true 接受连接
     */
    @Override
    public boolean onConnect(Conn conn) {
        long count = server.incrementTotalConn();
        log.debug("[router] OnConnect [{}] totalConn={}", conn.remoteAddress(), count);
        return true;
    }

    /**
     * 消息到达回调 —— 第一层路由分发。
     *
     * <p>根据消息 ID 分发到不同处理器：
     * <ul>
     *   <li>{@code MSG_Connect} → {@link #handleConnect}</li>
     *   <li>{@code MSG_Heartbeat} → {@link #handleHeartbeat}</li>
     *   <li>其他 → 返回 false，由 Room 事件循环处理</li>
     * </ul>
     *
     * @param conn 来源连接
     * @param packet 消息数据包
     * @return true 已处理，false 未处理（交给上层）
     */
    @Override
    public boolean onMessage(Conn conn, Packet packet) {
        MsgPacket msg = (MsgPacket) packet;
        Message.ID msgId = Message.ID.forNumber(msg.getMessageID());

        log.info("[router] OnMessage [{}] msg=[{}] len=[{}]",
                conn.remoteAddress(), msgId, msg.getData() != null ? msg.getData().length : 0);

        if (msgId == null) {
            log.warn("[router] unknown message id={}", msg.getMessageID());
            return false;
        }

        switch (msgId) {
            case MSG_Connect:
                return handleConnect(conn, msg);
            case MSG_Heartbeat:
                handleHeartbeat(conn);
                return true;
            default:
                // 其他消息在 Room 的游戏循环内处理，返回 false 表示"不处理"
                return false;
        }
    }

    /**
     * 连接关闭回调。
     *
     * <p>减少全局连接计数并记录日志。
     * 注意：实际的玩家离开逻辑在 Room 事件循环中处理（通过 outChan）。
     *
     * @param conn 关闭的连接
     */
    @Override
    public void onClose(Conn conn) {
        long count = server.decrementTotalConn();
        log.info("[router] OnClose: total={}", count);
    }

    /**
     * 处理 Connect 握手。
     *
     * <p>这是客户端连接后的第一个消息，用于校验玩家身份和房间状态。
     * 校验通过后，将 playerID 存入连接附加数据，并委托给 Room 处理。
     *
     * <p><b>注意</b>：Connect 成功响应不在此处发送，而是由 Game 在 JoinGame 时发送。
     * 这样设计是因为 Connect 只是"进入房间队列"，真正的加入在游戏循环中完成。
     *
     * @param conn 来源连接
     * @param msg Connect 消息
     * @return true 已处理（无论成功失败）
     */
    private boolean handleConnect(Conn conn, MsgPacket msg) {
        Message.C2S_ConnectMsg connect;
        try {
            connect = msg.unmarshal(Message.C2S_ConnectMsg.getDefaultInstance());
        } catch (Exception e) {
            log.error("[router] msg.Unmarshal error={}", e.getMessage());
            return false;
        }

        long playerID = connect.getPlayerID();
        long roomID = connect.getBattleID();
        String token = connect.getToken();

        Message.S2C_ConnectMsg.Builder ret = Message.S2C_ConnectMsg.newBuilder()
                .setErrorCode(Message.ERRORCODE.ERR_Ok);

        // 1. 检查房间是否存在
        com.tzw.logic.room.Room room = server.roomManager().getRoom(roomID);
        if (room == null) {
            ret.setErrorCode(Message.ERRORCODE.ERR_NoRoom);
            conn.asyncWrite(MsgPacket.newInstance((byte) Message.ID.MSG_Connect_VALUE, ret.build()));
            log.error("[router] no room player={} room={} token={}", playerID, roomID, token);
            return true;
        }

        // 2. 检查房间是否已结束
        if (room.isOver()) {
            ret.setErrorCode(Message.ERRORCODE.ERR_RoomState);
            conn.asyncWrite(MsgPacket.newInstance((byte) Message.ID.MSG_Connect_VALUE, ret.build()));
            log.error("[router] room is over player={} room={} token={}", playerID, roomID, token);
            return true;
        }

        // 3. 检查玩家是否属于该房间
        if (!room.hasPlayer(playerID)) {
            ret.setErrorCode(Message.ERRORCODE.ERR_NoPlayer);
            conn.asyncWrite(MsgPacket.newInstance((byte) Message.ID.MSG_Connect_VALUE, ret.build()));
            log.error("[router] !room.hasPlayer(playerID) player={} room={} token={}", playerID, roomID, token);
            return true;
        }

        // 4. 验证 token（当前为桩函数，生产环境应替换）
        if (!token.equals(verifyToken(token))) {
            ret.setErrorCode(Message.ERRORCODE.ERR_Token);
            conn.asyncWrite(MsgPacket.newInstance((byte) Message.ID.MSG_Connect_VALUE, ret.build()));
            log.error("[router] verifyToken failed player={} room={} token={}", playerID, roomID, token);
            return true;
        }

        // 5. 将 playerID 存入连接附加数据，供后续消息路由使用
        conn.putExtraData(playerID);

        // 6. 委托给 Room 处理（将连接放入 inChan，由房间单线程消费）
        // 注意：这里不返回 Connect 成功，由 Game 在 JoinGame 时返回
        return room.onConnect(conn);
    }

    /**
     * 处理心跳：原样回复。
     *
     * <p>心跳用于检测连接存活和测量 RTT。
     * 在帧同步系统中，客户端以固定频率（通常 30Hz）发送心跳，
     * 服务器原样回复即可。
     *
     * @param conn 来源连接
     */
    private void handleHeartbeat(Conn conn) {
        conn.asyncWrite(MsgPacket.newInstance((byte) Message.ID.MSG_Heartbeat_VALUE, (Object) null));
    }
}
