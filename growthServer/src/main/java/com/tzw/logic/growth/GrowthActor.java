package com.tzw.logic.growth;

import com.tzw.mq.MatchCreateEvent;
import com.tzw.mq.MatchReadyEvent;
import com.tzw.mq.MatchResultEvent;
import com.tzw.mq.MqProducer;
import com.tzw.network.Conn;
import com.tzw.network.ConnCallback;
import com.tzw.network.Packet;
import com.tzw.packet.MsgPacket;
import com.tzw.pb.Message;
import com.tzw.pb.Message.Item;
import com.tzw.server.GrowthRouter.GrowthEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
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

    /** 当前 TCP 连接（可能为空：玩家可能暂时断线） */
    private volatile Conn conn;

    /** 事件循环线程引用 */
    private volatile Thread loopThread;

    /** 事件循环运行标志 */
    private volatile boolean running = false;

    /** Actor 是否已关闭（AtomicBoolean 因为被 GrowthRouter 跨线程读取） */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** 随机数生成器（用于抽卡） */
    private final Random random = new Random();

    /** MQ 发布者（实例变量，由构造函数注入） */
    private final MqProducer mqProducer;

    public GrowthActor(long playerId, MqProducer mqProducer) {
        this.playerId = playerId;
        this.mqProducer = mqProducer;
        this.state = new PlayerGrowthState(playerId);
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
                    this.conn = in;
                    log.info("[GrowthActor-{}] player connected", playerId);
                    // 推送当前数据给客户端
                    pushPlayerData();
                }

                // 3. 处理连接离开
                Conn out = outChan.poll();
                if (out != null) {
                    log.info("[GrowthActor-{}] player disconnected", playerId);
                    if (this.conn == out) {
                        this.conn = null;
                    }
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
        Message.ID msgId = Message.ID.forNumber(msg.getMessageID());
        if (msgId == null) {
            return;
        }

        log.info("[GrowthActor-{}] processMsg msg=[{}]", playerId, msgId);

        switch (msgId) {
            case MSG_UpgradeLevel -> handleUpgrade(msg);
            case MSG_EquipItem -> handleEquip(msg);
            case MSG_UnequipItem -> handleUnequip(msg);
            case MSG_Gacha -> handleGacha(msg);
            case MSG_PlayerData -> pushPlayerData();
            case MSG_Inventory -> pushInventory();
            case MSG_EnterMatch -> handleEnterMatch(msg);
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
                    handleMatchReady(e);
                }
            }
            case "match.result" -> {
                if (event.payload() instanceof MatchResultEvent e) {
                    handleMatchResult(e);
                }
            }
            case "conn.close" -> {
                // 连接已断开，无需额外处理
            }
            default -> log.warn("[GrowthActor-{}] unknown event type: {}", playerId, event.type());
        }
    }

    /**
     * 处理匹配成功事件 — 通知客户端进入战斗
     *
     * @param event 匹配就绪事件
     */
    private void handleMatchReady(MatchReadyEvent event) {
        log.info("[GrowthActor-{}] match ready: roomId={}, token={}", playerId, event.roomId(), event.token());

        // 推送匹配成功消息给客户端
        Message.S2C_MatchReadyMsg matchReady = Message.S2C_MatchReadyMsg.newBuilder()
                .setRoomHost(event.roomHost())
                .setRoomPort(event.roomPort())
                .setToken(event.token())
                .setRoomID(event.roomId())
                .build();
        send(MsgPacket.newInstance((byte) Message.ID.MSG_MatchReady_VALUE, matchReady));
    }

    // ==================== 业务处理 ====================

    /**
     * 处理升级请求
     */
    private void handleUpgrade(MsgPacket msg) {
        try {
            Message.C2S_UpgradeLevelMsg req = msg.unmarshal(
                    Message.C2S_UpgradeLevelMsg.getDefaultInstance());
            int targetLevel = req.getTargetLevel();

            // 校验目标等级
            if (targetLevel <= state.getLevel() || targetLevel > state.getLevel() + 1) {
                sendUpgradeResult(false, state.getLevel(), state.getGold(), state.getTotalAttack(),
                        Message.ERRORCODE.ERR_RoomState);
                return;
            }

            // 计算升级费用
            long cost = PlayerGrowthState.getRequiredExp(state.getLevel());

            // 消费金币
            if (!state.spendGold(cost)) {
                sendUpgradeResult(false, state.getLevel(), state.getGold(), state.getTotalAttack(),
                        Message.ERRORCODE.ERR_RoomState);
                return;
            }

            // 执行升级
            state.setLevel(state.getLevel() + 1);
            state.setBaseAttack(state.getBaseAttack() + 5);

            log.info("[GrowthActor-{}] upgrade to level={}, cost={}", playerId, state.getLevel(), cost);

            sendUpgradeResult(true, state.getLevel(), state.getGold(), state.getTotalAttack(),
                    Message.ERRORCODE.ERR_Ok);

        } catch (Exception e) {
            log.error("[GrowthActor-{}] handleUpgrade error: {}", playerId, e.getMessage());
        }
    }

    private void sendUpgradeResult(boolean success, int newLevel, long remainGold,
                                    int newAttack, Message.ERRORCODE errorCode) {
        Message.S2C_UpgradeLevelMsg response = Message.S2C_UpgradeLevelMsg.newBuilder()
                .setSuccess(success)
                .setNewLevel(newLevel)
                .setRemainGold(remainGold)
                .setNewAttack(newAttack)
                .setErrorCode(errorCode)
                .build();
        send(MsgPacket.newInstance((byte) Message.ID.MSG_UpgradeLevel_VALUE, response));
    }

    /**
     * 处理装备请求
     */
    private void handleEquip(MsgPacket msg) {
        try {
            Message.C2S_EquipItemMsg req = msg.unmarshal(
                    Message.C2S_EquipItemMsg.getDefaultInstance());
            long itemId = req.getItemID();

            if (!state.hasItem(itemId, 1)) {
                sendEquipResult(false, itemId, state.getTotalAttack(),
                        Message.ERRORCODE.ERR_RoomState);
                return;
            }

            state.equipItem(itemId);
            log.info("[GrowthActor-{}] equip item={}", playerId, itemId);

            sendEquipResult(true, itemId, state.getTotalAttack(), Message.ERRORCODE.ERR_Ok);

        } catch (Exception e) {
            log.error("[GrowthActor-{}] handleEquip error: {}", playerId, e.getMessage());
        }
    }

    private void sendEquipResult(boolean success, long itemId, int newAttack,
                                  Message.ERRORCODE errorCode) {
        Message.S2C_EquipItemMsg response = Message.S2C_EquipItemMsg.newBuilder()
                .setSuccess(success)
                .setItemID(itemId)
                .setNewAttack(newAttack)
                .setErrorCode(errorCode)
                .build();
        send(MsgPacket.newInstance((byte) Message.ID.MSG_EquipItem_VALUE, response));
    }

    /**
     * 处理卸下装备请求
     */
    private void handleUnequip(MsgPacket msg) {
        try {
            Message.C2S_UnequipItemMsg req = msg.unmarshal(
                    Message.C2S_UnequipItemMsg.getDefaultInstance());
            long itemId = req.getItemID();

            if (state.getEquippedItemId() != itemId) {
                sendUnequipResult(false, state.getTotalAttack(),
                        Message.ERRORCODE.ERR_RoomState);
                return;
            }

            state.unequipItem();
            log.info("[GrowthActor-{}] unequip item={}", playerId, itemId);

            sendUnequipResult(true, state.getTotalAttack(), Message.ERRORCODE.ERR_Ok);

        } catch (Exception e) {
            log.error("[GrowthActor-{}] handleUnequip error: {}", playerId, e.getMessage());
        }
    }

    private void sendUnequipResult(boolean success, int newAttack, Message.ERRORCODE errorCode) {
        Message.S2C_UnequipItemMsg response = Message.S2C_UnequipItemMsg.newBuilder()
                .setSuccess(success)
                .setNewAttack(newAttack)
                .setErrorCode(errorCode)
                .build();
        send(MsgPacket.newInstance((byte) Message.ID.MSG_UnequipItem_VALUE, response));
    }

    /**
     * 处理抽卡请求
     */
    private void handleGacha(MsgPacket msg) {
        try {
            Message.C2S_GachaMsg req = msg.unmarshal(
                    Message.C2S_GachaMsg.getDefaultInstance());
            int times = req.getTimes();

            // 只支持单抽和十连
            if (times != 1 && times != 10) {
                sendGachaResult(java.util.Collections.emptyList(), state.getGold(),
                        Message.ERRORCODE.ERR_RoomState);
                return;
            }

            // 计算费用
            long cost = times == 1 ? 100 : 900;

            if (!state.spendGold(cost)) {
                sendGachaResult(java.util.Collections.emptyList(), state.getGold(),
                        Message.ERRORCODE.ERR_RoomState);
                return;
            }

            // 抽卡逻辑
            java.util.List<Item> items = new java.util.ArrayList<>();
            for (int i = 0; i < times; i++) {
                Item item = rollGacha();
                state.addItem(item);
                items.add(item);
            }

            log.info("[GrowthActor-{}] gacha times={}, cost={}", playerId, times, cost);

            sendGachaResult(items, state.getGold(), Message.ERRORCODE.ERR_Ok);

        } catch (Exception e) {
            log.error("[GrowthActor-{}] handleGacha error: {}", playerId, e.getMessage());
        }
    }

    /**
     * 抽卡随机逻辑
     *
     * @return 抽到的物品
     */
    private Item rollGacha() {
        int rarity = random.nextInt(100);
        long itemId;
        String name;
        int attackBonus;

        if (rarity < 5) {
            // 5% 传说
            itemId = 1001;
            name = "传说武器";
            attackBonus = 50;
        } else if (rarity < 20) {
            // 15% 史诗
            itemId = 1002;
            name = "史诗武器";
            attackBonus = 30;
        } else if (rarity < 50) {
            // 30% 稀有
            itemId = 1003;
            name = "稀有武器";
            attackBonus = 15;
        } else {
            // 50% 普通
            itemId = 1004;
            name = "普通武器";
            attackBonus = 5;
        }

        return Item.newBuilder()
                .setItemID(itemId)
                .setName(name)
                .setCount(1)
                .setAttackBonus(attackBonus)
                .build();
    }

    private void sendGachaResult(java.util.List<Item> items, long remainGold,
                                  Message.ERRORCODE errorCode) {
        Message.S2C_GachaMsg response = Message.S2C_GachaMsg.newBuilder()
                .addAllItems(items)
                .setRemainGold(remainGold)
                .setErrorCode(errorCode)
                .build();
        send(MsgPacket.newInstance((byte) Message.ID.MSG_Gacha_VALUE, response));
    }

    /**
     * 处理进入匹配请求
     *
     * <p>发布 MatchCreateEvent 到 MQ，战斗服务订阅后创建房间。
     */
    private void handleEnterMatch(MsgPacket msg) {
        log.info("[GrowthActor-{}] enter match", playerId);

        // 发布匹配请求到 MQ
        if (mqProducer != null) {
            MatchCreateEvent event = new MatchCreateEvent(playerId, 1);
            mqProducer.send("match.create", event);
            log.info("[GrowthActor-{}] published match.create event", playerId);
        } else {
            log.warn("[GrowthActor-{}] mqProducer not available", playerId);
        }
    }

    /**
     * 处理心跳
     */
    private void handleHeartbeat() {
        send(MsgPacket.newInstance((byte) Message.ID.MSG_Heartbeat_VALUE, (Object) null));
    }

    /**
     * 处理对战结果 —— 发放奖励
     *
     * @param event 对战结果事件
     */
    private void handleMatchResult(MatchResultEvent event) {
        log.info("[GrowthActor-{}] match result: win={}, exp={}, gold={}",
                playerId, event.win(), event.expGain(), event.goldGain());

        // 发放奖励
        state.addExp(event.expGain());
        state.addGold(event.goldGain());

        // 检查升级
        while (state.tryLevelUp()) {
            log.info("[GrowthActor-{}] auto level up to {}", playerId, state.getLevel());
        }

        // 推送结算给客户端
        Message.S2C_MatchResultMsg result = Message.S2C_MatchResultMsg.newBuilder()
                .setRoomID(event.roomId())
                .setWin(event.win())
                .setExpGain(event.expGain())
                .setGoldGain(event.goldGain())
                .setNewLevel(state.getLevel())
                .build();
        send(MsgPacket.newInstance((byte) Message.ID.MSG_MatchResult_VALUE, result));
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

    // ==================== 辅助方法 ====================

    /**
     * 推送玩家数据给客户端
     */
    private void pushPlayerData() {
        send(MsgPacket.newInstance((byte) Message.ID.MSG_PlayerData_VALUE,
                state.toProto().build()));
    }

    /**
     * 推送背包数据给客户端
     */
    private void pushInventory() {
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
    private void send(MsgPacket msg) {
        if (conn != null && !conn.isClosed()) {
            conn.asyncWrite(msg);
        }
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
