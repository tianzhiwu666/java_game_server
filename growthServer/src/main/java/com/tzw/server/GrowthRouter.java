package com.tzw.server;

import com.tzw.logic.growth.GrowthSessionManager;
import com.tzw.mq.EventBus;
import com.tzw.mq.MatchReadyEvent;
import com.tzw.mq.MatchResultEvent;
import com.tzw.mq.MqProducer;
import com.tzw.mq.TypedMqConsumer;
import com.tzw.network.Conn;
import com.tzw.network.ConnCallback;
import com.tzw.network.Packet;
import com.tzw.packet.MsgPacket;
import com.tzw.pb.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 养成系统消息路由器 —— 养成服务的第一层消息分发。
 *
 * <p>镜像 {@link Router}（战斗系统的路由器），实现 {@link ConnCallback} 接口。
 * 运行在 Netty workerGroup 线程中。
 *
 * <h3>两层路由架构</h3>
 * <p>与战斗系统一样，养成系统采用两层路由：
 * <ol>
 *   <li><b>GrowthRouter（本类）</b>：处理鉴权（MSG_GrowthAuth）和心跳（MSG_Heartbeat），
 *       鉴权成功后回调切换为 GrowthActor。</li>
 *   <li><b>GrowthActor</b>：运行在玩家单线程中，处理所有养成业务消息。</li>
 * </ol>
 *
 * <h3>鉴权流程</h3>
 * <pre>
 * 1. 客户端发送 MSG_GrowthAuth（此时 conn.callback = GrowthRouter）
 * 2. GrowthRouter.handleAuth() 校验 token → 获取 playerId
 * 3. 调用 GrowthSessionManager.getOrCreateActor(playerId)
 * 4. GrowthActor.onConnect(conn) → conn.setCallback(this) ← 回调切换为 GrowthActor
 * 5. 后续消息 → GrowthActor.onMessage() → 放入 msgQ → 返回 true → 连接保持
 * </pre>
 *
 * <h3>线程安全</h3>
 * <p>本类运行在 Netty 线程中，不直接修改 {@link com.tzw.logic.growth.GrowthActor} 状态。
 * 所有游戏状态修改都通过 {@code GrowthActor.tell()} 间接完成，
 * 最终由玩家单线程处理。
 */
public class GrowthRouter implements ConnCallback {

    private static final Logger log = LoggerFactory.getLogger(GrowthRouter.class);

    /** 养成会话管理器，用于获取/创建玩家 Actor */
    private final GrowthSessionManager sessionManager;

    /** 事件总线（进程内） */
    private final EventBus eventBus;

    /** MQ 消费者（跨进程） */
    private final TypedMqConsumer mqConsumer;

    /** MQ 生产者（跨进程） */
    private final MqProducer mqProducer;

    /** 全局连接计数（仅用于监控） */
    private long totalConn = 0;

    public GrowthRouter(GrowthSessionManager sessionManager, EventBus eventBus,
                        TypedMqConsumer mqConsumer, MqProducer mqProducer) {
        this.sessionManager = sessionManager;
        this.eventBus = eventBus;
        this.mqConsumer = mqConsumer;
        this.mqProducer = mqProducer;

        // 订阅匹配相关事件
        subscribeMatchEvents();
    }

    /**
     * 订阅匹配相关事件
     *
     * <p>通过 MQ 订阅战斗服务发布的 match.ready 和 match.result 事件。
     */
    private void subscribeMatchEvents() {
        // 订阅匹配成功事件（战斗服务 → 养成服务）
        mqConsumer.subscribeTyped("match.ready", MatchReadyEvent.class, event -> {
            log.info("[GrowthRouter] match.ready: playerId={}, roomId={}", event.playerId(), event.roomId());
            // 通知客户端房间就绪
            sessionManager.dispatch(event.playerId(), new GrowthEvent("match.ready", event));
        });

        // 订阅对战结果事件（战斗服务 → 养成服务）
        mqConsumer.subscribeTyped("match.result", MatchResultEvent.class, event -> {
            log.info("[GrowthRouter] match.result: playerId={}, win={}", event.playerId(), event.win());
            // 路由到 GrowthActor 处理奖励
            sessionManager.dispatch(event.playerId(), new GrowthEvent("match.result", event));
        });

        log.info("[GrowthRouter] subscribed to match.ready and match.result via MQ");
    }

    /**
     * 连接建立回调
     *
     * <p>增加全局连接计数并记录日志。返回 true 表示接受连接。
     *
     * @param conn 新建立的连接
     * @return true 接受连接
     */
    @Override
    public boolean onConnect(Conn conn) {
        totalConn++;
        log.debug("[GrowthRouter] onConnect [{}] totalConn={}", conn.remoteAddress(), totalConn);
        return true;
    }

    /**
     * 消息到达回调 —— 第一层路由分发
     *
     * <p>根据消息 ID 分发到不同处理器：
     * <ul>
     *   <li>{@code MSG_GrowthAuth} → {@link #handleAuth}</li>
     *   <li>{@code MSG_Heartbeat} → 原样回复</li>
     *   <li>其他 → 返回 false，关闭连接（养成消息需要鉴权后才能处理）</li>
     * </ul>
     *
     * @param conn 来源连接
     * @param packet 消息数据包
     * @return true 已处理，false 未处理（关闭连接）
     */
    @Override
    public boolean onMessage(Conn conn, Packet packet) {
        MsgPacket msg = (MsgPacket) packet;
        Message.ID msgId = Message.ID.forNumber(msg.getMessageID());

        log.info("[GrowthRouter] onMessage [{}] msg=[{}]",
                conn.remoteAddress(), msgId);

        if (msgId == null) {
            log.warn("[GrowthRouter] unknown message id={}", msg.getMessageID());
            return false;
        }

        // 未鉴权时只允许 GrowthAuth 消息
        if (conn.getExtraData() == null) {
            if (msgId == Message.ID.MSG_GrowthAuth) {
                return handleAuth(conn, msg);
            }
            log.warn("[GrowthRouter] unauthenticated message id={}, close connection", msgId);
            return false;
        }

        // 已鉴权但回调仍是 Router（异常情况）
        log.warn("[GrowthRouter] authenticated but callback is still Router, msg={}", msgId);
        return false;
    }

    /**
     * 连接关闭回调
     *
     * <p>减少全局连接计数并记录日志。
     *
     * @param conn 关闭的连接
     */
    @Override
    public void onClose(Conn conn) {
        totalConn--;
        log.info("[GrowthRouter] onClose: total={}", totalConn);

        // 通知对应的 GrowthActor 连接断开
        Object id = conn.getExtraData();
        if (id instanceof Long) {
            sessionManager.dispatch((Long) id, new GrowthEvent("conn.close", null));
        }
    }

    /**
     * 处理养成系统鉴权
     *
     * <p>这是客户端 TCP 连接后的第一个消息，用于校验玩家身份。
     * 校验通过后，获取/创建 GrowthActor 并切换回调。
     *
     * @param conn 来源连接
     * @param msg 鉴权消息
     * @return true 已处理
     */
    private boolean handleAuth(Conn conn, MsgPacket msg) {
        Message.C2S_GrowthAuthMsg auth;
        try {
            auth = msg.unmarshal(Message.C2S_GrowthAuthMsg.getDefaultInstance());
        } catch (Exception e) {
            log.error("[GrowthRouter] msg.Unmarshal error={}", e.getMessage());
            return false;
        }

        long playerId = auth.getPlayerID();
        String token = auth.getToken();

        // TODO 桩函数，生产环境应替换为 JWT 等认证机制
        if (!verifyToken(token)) {
            log.error("[GrowthRouter] verifyToken failed player={}", playerId);
            return false;
        }

        // 将 playerID 存入连接附加数据
        conn.putExtraData(playerId);

        // 获取/创建 GrowthActor
        sessionManager.getOrCreateActor(playerId, conn);

        log.info("[GrowthRouter] auth success player={}", playerId);
        return true;
    }

    /**
     * TODO 桩函数，直接返回输入（身份验证尚未实现）。
     */
    private static boolean verifyToken(String token) {
        return token != null && !token.isEmpty();
    }

    /**
     * 内部类：Growth 事件封装
     *
     * <p>用于通过 GrowthSessionManager.dispatch() 向 GrowthActor 传递事件。
     */
    public record GrowthEvent(String type, Object payload) {}
}
