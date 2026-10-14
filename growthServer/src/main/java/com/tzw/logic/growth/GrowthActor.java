package com.tzw.logic.growth;

import com.tzw.logic.growth.handler.EquipHandler;
import com.tzw.logic.growth.handler.GachaHandler;
import com.tzw.logic.growth.handler.MatchHandler;
import com.tzw.logic.growth.handler.UpgradeHandler;
import com.tzw.mq.MatchReadyEvent;
import com.tzw.mq.MatchResultEvent;
import com.tzw.mq.MqProducer;
import com.tzw.network.Conn;
import com.tzw.network.ConnCallback;
import com.tzw.network.Packet;
import com.tzw.packet.MsgPacket;
import com.tzw.pb.Message;
import com.tzw.server.GrowthRouter.GrowthEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 玩家养成 Actor —— 每玩家单线程事件循环。
 *
 * <p>镜像 {@link com.tzw.logic.room.Room} 的 Actor 模式：
 * <ul>
 *   <li>每个玩家一个 Actor，拥有私有状态 {@link PlayerGrowthState}</li>
 *   <li>外部通过消息（{@link BlockingQueue}）与 Actor 通信，不直接调用方法</li>
 *   <li>Actor 线程串行处理消息，无需锁即可保证状态一致性</li>
 * </ul>
 *
 * <h3>与 Room Actor 的区别</h3>
 * <table border="1">
 *   <tr><th>维度</th><th>Room</th><th>GrowthActor（本类）</th></tr>
 *   <tr><td>触发机制</td><td>30Hz tick + 消息</td><td>纯消息驱动（无 tick）</td></tr>
 *   <tr><td>队列容量</td><td>msgQ=2048</td><td>msgQ=512（养成消息频率低）</td></tr>
 *   <tr><td>生命周期</td><td>房间存在期间</td><td>玩家在线期间</td></tr>
 *   <tr><td>持久化</td><td>无需持久化</td><td>需要 Redis + MySQL 持久化</td></tr>
 * </table>
 *
 * <h3>线程安全</h3>
 * <ul>
 *   <li>{@link #closed} 使用 {@link AtomicBoolean}，因为 {@link com.tzw.server.GrowthRouter} 在 Netty 线程读取它</li>
 *   <li>{@link #running} 使用 {@code volatile} 保证可见性</li>
 *   <li>其他字段只在 Actor 线程中访问，无需同步</li>
 * </ul>
 */
public class GrowthActor implements Runnable, ConnCallback {

    private static final Logger log = LoggerFactory.getLogger(GrowthActor.class);

    /** 玩家 ID */
    private final long playerId;

    /** 入站消息队列（容量 512） */
    private final BlockingQueue<MsgPacket> msgQ = new LinkedBlockingQueue<>(512);

    /** 连接加入队列（容量 4） */
    private final BlockingQueue<Conn> inChan = new LinkedBlockingQueue<>(4);

    /** 连接离开队列（容量 4） */
    private final BlockingQueue<Conn> outChan = new LinkedBlockingQueue<>(4);

    /** 系统事件队列（匹配结果等） */
    private final BlockingQueue<GrowthEvent> eventQ = new LinkedBlockingQueue<>(64);

    /** 玩家养成状态 */
    private final PlayerGrowthState state;

    /** 发送职责：持有当前 TCP 连接（可能为空：玩家可能暂时断线） */
    private final GrowthMessenger messenger;

    /** 升级业务处理器 */
    private final UpgradeHandler upgradeHandler;

    /** 装备业务处理器 */
    private final EquipHandler equipHandler;

    /** 事件循环线程引用 */
    private volatile Thread loopThread;

    /** 事件循环运行标志 */
    private volatile boolean running = false;

    /** Actor 是否已关闭（AtomicBoolean 因为被 GrowthRouter 跨线程读取） */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** 抽卡业务处理器 */
    private final GachaHandler gachaHandler;

    /** 匹配业务处理器 */
    private final MatchHandler matchHandler;

    public GrowthActor(long playerId, MqProducer mqProducer) {
        this.playerId = playerId;
        this.state = new PlayerGrowthState(playerId);
        this.messenger = new GrowthMessenger(playerId, state);
        this.upgradeHandler = new UpgradeHandler(playerId, state, messenger);
        this.equipHandler = new EquipHandler(playerId, state, messenger);
        this.gachaHandler = new GachaHandler(playerId, state, messenger);
        this.matchHandler = new MatchHandler(playerId, state, messenger, mqProducer);
    }

    // ==================== Getter ====================

    public long getPlayerId() { return playerId; }
    public PlayerGrowthState getState() { return state; }
    public boolean isRunning() { return running && !closed.get(); }
    public boolean isClosed() { return closed.get(); }

    // ==================== 消息入口（Netty 线程调用） ====================

    /**
     * 投递消息到 Actor 队列
     *
     * <p>由 Netty 线程调用，非阻塞。
     *
     * @param packet 消息包
     * @return true 投递成功，false 队列满
     */
    public boolean tell(MsgPacket packet) {
        if (closed.get()) {
            return false;
        }
        return msgQ.offer(packet);
    }

    /**
     * 投递系统事件到 Actor 队列
     *
     * <p>由 EventBus 回调调用，非阻塞。
     *
     * @param event 系统事件
     * @return true 投递成功，false 队列满
     */
    public boolean tellEvent(GrowthEvent event) {
        if (closed.get()) {
            return false;
        }
        return eventQ.offer(event);
    }

    // ==================== ConnCallback（回调切换后） ====================

    /**
     * 连接建立回调
     *
     * <p>由 {@link com.tzw.server.GrowthRouter} 调用，鉴权成功后切换回调。
     * 将连接放入 inChan，由 Actor 线程消费。
     *
     * @param conn 新建立的连接
     * @return true 接受连接
     */
    @Override
    public boolean onConnect(Conn conn) {
        conn.setCallback(this);
        inChan.offer(conn);
        log.info("[GrowthActor-{}] onConnect {}", playerId, conn.remoteAddress());
        return true;
    }

    /**
     * 消息到达回调
     *
     * <p>由 Netty 线程调用。将消息放入 msgQ，由 Actor 线程消费。
     *
     * @param conn 来源连接
     * @param packet 消息数据包
     * @return true 消息已接受（放入队列）
     */
    @Override
    public boolean onMessage(Conn conn, Packet packet) {
        Object id = conn.getExtraData();
        if (!(id instanceof Long) || (Long) id != playerId) {
            log.error("[GrowthActor-{}] OnMessage error conn don't have correct id", playerId);
            return false;
        }
        MsgPacket msg = (MsgPacket) packet;
        if (!msgQ.offer(msg)) {
            log.warn("[GrowthActor-{}] msgQ full, drop msg={}", playerId, msg.getMessageID());
            return false;
        }
        return true;
    }

    /**
     * 连接关闭回调
     *
     * <p>由 Netty 线程调用。将连接放入 outChan，由 Actor 线程消费。
     *
     * @param conn 关闭的连接
     */
    @Override
    public void onClose(Conn conn) {
        outChan.offer(conn);
        log.info("[GrowthActor-{}] onClose {}", playerId, conn.remoteAddress());
    }

    // ==================== 主事件循环 ====================

    /**
     * 主事件循环 —— Actor 的核心。
     *
     * <p>在单线程中运行，纯消息驱动（无需 tick）。
     * 循环处理：
     * <ol>
     *   <li>从 msgQ 取出消息并处理（带超时以便检查其他队列）</li>
     *   <li>从 inChan 取出新连接</li>
     *   <li>从 outChan 取出断开连接</li>
     *   <li>从 eventQ 取出系统事件</li>
     * </ol>
     */
    @Override
    public void run() {
        loopThread = Thread.currentThread();
        running = true;

        log.info("[GrowthActor-{}] running...", playerId);

        while (running) {
            try {
                // 1. 处理消息（带超时以便检查其他队列）
                MsgPacket msg = msgQ.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (msg != null) {
                    processMsg(msg);
                }

                // 2. 处理新连接
                Conn in = inChan.poll();
                if (in != null) {
                    log.info("[GrowthActor-{}] player connected", playerId);
                    messenger.bind(in);
                    // 推送当前数据给客户端
                    messenger.pushPlayerData();
                }

                // 3. 处理连接离开
                Conn out = outChan.poll();
                if (out != null) {
                    log.info("[GrowthActor-{}] player disconnected", playerId);
                    messenger.unbind(out);
                }

                // 4. 处理系统事件
                GrowthEvent event = eventQ.poll();
                if (event != null) {
                    processEvent(event);
                }

            } catch (InterruptedException e) {
                // 被 stop() 中断，退出循环
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("[GrowthActor-{}] error in event loop: {}", playerId, e.getMessage(), e);
            }
        }

        // 清理：保存数据
        saveData();
        closed.set(true);
        log.info("[GrowthActor-{}] stopped", playerId);
    }

    // ==================== 消息处理 ====================

    /**
     * 处理消息 —— 消息分发核心
     *
     * @param msg 消息包
     */
    private void processMsg(MsgPacket msg) {
        // 消息 ID 在 wire 上是无符号单字节（养成段 200~242 会溢出为负），须 & 0xFF 还原
        Message.ID msgId = Message.ID.forNumber(msg.getMessageID() & 0xFF);
        if (msgId == null) {
            return;
        }

        log.info("[GrowthActor-{}] processMsg msg=[{}]", playerId, msgId);

        switch (msgId) {
            case MSG_UpgradeLevel -> upgradeHandler.handle(msg);
            case MSG_EquipItem -> equipHandler.handleEquip(msg);
            case MSG_UnequipItem -> equipHandler.handleUnequip(msg);
            case MSG_Gacha -> gachaHandler.handle(msg);
            case MSG_PlayerData -> messenger.pushPlayerData();
            case MSG_Inventory -> messenger.pushInventory();
            case MSG_EnterMatch -> matchHandler.handleEnterMatch();
            case MSG_Heartbeat -> handleHeartbeat();
            default -> log.warn("[GrowthActor-{}] unknown message id[{}]", playerId, msgId);
        }
    }

    /**
     * 处理系统事件
     *
     * @param event 系统事件
     */
    private void processEvent(GrowthEvent event) {
        log.info("[GrowthActor-{}] processEvent type={}", playerId, event.type());

        switch (event.type()) {
            case "match.ready" -> {
                if (event.payload() instanceof MatchReadyEvent e) {
                    matchHandler.handleMatchReady(e);
                }
            }
            case "match.result" -> {
                if (event.payload() instanceof MatchResultEvent e) {
                    matchHandler.handleMatchResult(e);
                }
            }
            case "conn.close" -> {
                // 连接已断开，无需额外处理
            }
            default -> log.warn("[GrowthActor-{}] unknown event type: {}", playerId, event.type());
        }
    }

    // ==================== 业务处理 ====================

    /**
     * 处理心跳
     */
    private void handleHeartbeat() {
        messenger.send(MsgPacket.newInstance((byte) Message.ID.MSG_Heartbeat_VALUE, (Object) null));
    }

    // ==================== 数据持久化 ====================

    /**
     * 保存玩家数据
     *
     * <p>在 Actor 停止时调用，将数据写入 Redis。
     * MySQL 写入由 {@link com.tzw.dao.PersistScheduler} 定期批量处理。
     */
    private void saveData() {
        if (!state.isDirty()) {
            return;
        }
        // TODO 写入 Redis
        log.info("[GrowthActor-{}] save data: level={}, gold={}", playerId, state.getLevel(), state.getGold());
        state.clearDirty();
    }

    /**
     * 强制停止 Actor
     *
     * <p>由 {@link GrowthSessionManager} 调用。
     * 设置 running = false 并中断线程，触发 run() 循环退出。
     */
    public void stop() {
        running = false;
        closed.set(true);
        if (loopThread != null) {
            loopThread.interrupt();
        }
    }
}
