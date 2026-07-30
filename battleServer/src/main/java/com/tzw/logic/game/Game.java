package com.tzw.logic.game;

import com.tzw.config.LockstepProperties;
import com.tzw.network.Conn;
import com.tzw.packet.MsgPacket;
import com.tzw.pb.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 一局游戏 —— 帧同步服务器的游戏逻辑核心。
 *
 * <p><b>这是系统中第二重要的类。</b>镜像 Go 参考实现中的 {@code logic/game/game.go}。
 * 实现游戏状态机，处理所有游戏逻辑消息，驱动帧广播。
 *
 * <h3>状态机（READY → GAMING → OVER → STOP）</h3>
 * <pre>
 * READY:  等待所有玩家准备或超时
 *   ↓ (所有玩家准备 或 超时且有在线玩家)
 * GAMING: 战斗中，每帧收集输入并广播
 *   ↓ (所有在线玩家提交结果 或 帧数超限)
 * OVER:   结束，通知房间
 *   ↓
 * STOP:   停止，退出事件循环
 * </pre>
 *
 * <h3>Tick 驱动的状态转换</h3>
 * <p>游戏状态由 {@link #tick} 方法驱动，每 33ms（30Hz）调用一次：
 * <ul>
 *   <li><b>READY → tickReady</b>：检查是否所有玩家已准备，或超时强制开始</li>
 *   <li><b>GAMING → tickGaming</b>：推进帧计数器，广播帧数据</li>
 *   <li><b>OVER → doGameOver</b>：通知监听器，转换到 STOP</li>
 *   <li><b>STOP</b>：返回 false，触发房间退出</li>
 * </ul>
 *
 * <h3>消息处理</h3>
 * <p>通过 {@link #processMsg} 分发消息：
 * <ul>
 *   <li><b>MSG_JoinRoom</b>：回复座位信息、其他玩家列表、随机种子</li>
 *   <li><b>MSG_Progress</b>：更新加载进度，广播给其他玩家（发送者除外）</li>
 *   <li><b>MSG_Ready</b>：标记玩家已准备，如果在 GAMING 状态则触发重连</li>
 *   <li><b>MSG_Input</b>：收集玩家输入到帧缓冲区，设置 dirty 标志</li>
 *   <li><b>MSG_Result</b>：记录玩家提交的结果</li>
 *   <li><b>MSG_Heartbeat</b>：回复心跳，刷新心跳时间</li>
 * </ul>
 *
 * <h3>帧广播逻辑</h3>
 * <p>{@link #broadcastFrameData} 是热路径，每帧执行一次：
 * <ol>
 *   <li>检查是否需要广播：dirty 标志或帧差超过 {@code broadcastOffsetFrames}</li>
 *   <li>遍历每个在线且已准备的玩家</li>
 *   <li>跳过网络不佳的玩家（心跳超时）</li>
 *   <li>从玩家的 {@code sendFrameCount} 游标开始，发送新帧</li>
 *   <li>每 {@code maxFrameDataPerMsg} 帧打包一个消息</li>
 *   <li>更新玩家的 {@code sendFrameCount} 游标</li>
 * </ol>
 *
 * <h3>断线重连</h3>
 * <p>{@link #doReconnect} 处理玩家重连：
 * <ol>
 *   <li>发送 Start 消息（含时间戳）</li>
 *   <li>批量重放所有历史帧（从帧 0 到当前帧）</li>
 *   <li>每 {@code maxFrameDataPerMsg} 帧打包一个消息</li>
 *   <li>更新玩家的 {@code sendFrameCount} 到当前帧</li>
 * </ol>
 *
 * <h3>线程安全</h3>
 * <p><b>此类所有方法只在房间的单线程中调用，无需额外同步。</b>
 * 这是帧同步确定性的根本保证。
 */
public class Game {

    private static final Logger log = LoggerFactory.getLogger(Game.class);

    /**
     * 游戏状态枚举。
     *
     * <p>镜像 Go 的 GameState，状态流转为：
     * READY → GAMING → OVER → STOP
     */
    public enum State {
        /** 准备阶段：等待所有玩家准备 */
        READY,
        /** 战斗中：收集输入并广播帧 */
        GAMING,
        /** 结束：所有玩家已提交结果或超时 */
        OVER,
        /** 停止：退出事件循环 */
        STOP
    }

    /** 房间 ID */
    private final long id;

    /** 游戏开始时间（秒），在 doStart 中设置 */
    private long startTime;

    /** 随机种子（用于确定性计算） */
    private final int randomSeed;

    /** 当前游戏状态 */
    private State state;

    /** 玩家 ID → Player 对象的映射 */
    private final Map<Long, Player> players = new HashMap<>();

    /** 帧缓冲区（确定性回放日志） */
    private final Lockstep lockstep = new Lockstep();

    /**
     * 客户端帧计数（已广播到的帧号）。
     *
     * <p>用于判断是否需要广播：当 {@code lockstep.frameCount - clientFrameCount >= broadcastOffsetFrames}
     * 时触发广播。这是帧广播的节流机制。
     */
    private long clientFrameCount;

    /** 玩家提交的结果：玩家 ID → 获胜者 ID */
    private final Map<Long, Long> result = new HashMap<>();

    /** 游戏事件回调（指向 Room） */
    private final GameListener listener;

    /**
     * 脏标志：强制下一帧广播。
     *
     * <p>当收到玩家输入时设置为 true，确保输入能尽快广播给其他玩家。
     * 这是帧广播的"即时触发"机制，避免等待 {@code broadcastOffsetFrames} 帧。
     */
    private boolean dirty;

    /** 配置对象 */
    private final LockstepProperties properties;

    public Game(long id, List<Long> playerIds, int randomSeed, GameListener listener, LockstepProperties properties) {
        this.id = id;
        this.randomSeed = randomSeed;
        this.listener = listener;
        this.properties = properties;
        this.state = State.READY;
        this.startTime = System.currentTimeMillis() / 1000;

        // 为每个玩家创建 Player 对象，分配座位索引（1~N）
        int idx = 1;
        for (Long pid : playerIds) {
            players.put(pid, new Player(pid, idx++));
        }
    }

    // ==================== 核心方法 ====================

    /**
     * 加入游戏。
     *
     * <p>处理玩家加入逻辑：
     * <ol>
     *   <li>检查玩家是否属于该房间</li>
     *   <li>检查游戏状态（不能在 OVER/STOP 状态加入）</li>
     *   <li>如果已有连接，顶掉旧连接（断线重连场景）</li>
     *   <li>建立新连接，发送 Connect 成功响应</li>
     *   <li>触发 onJoinGame 回调</li>
     * </ol>
     *
     * @param id 玩家 ID
     * @param conn 网络连接
     * @return true 成功加入
     */
    public boolean joinGame(long id, Conn conn) {
        Message.S2C_ConnectMsg.Builder msg = Message.S2C_ConnectMsg.newBuilder()
                .setErrorCode(Message.ERRORCODE.ERR_Ok);

        Player p = players.get(id);
        if (p == null) {
            log.error("[game({})] player[{}] join room failed", id, id);
            return false;
        }

        // 游戏已结束，拒绝加入
        if (state != State.READY && state != State.GAMING) {
            msg.setErrorCode(Message.ERRORCODE.ERR_RoomState);
            p.sendMessage(MsgPacket.newInstance((byte) Message.ID.MSG_Connect_VALUE, msg.build()));
            log.error("[game({})] player[{}] game is over", this.id, id);
            return true;
        }

        // 把现有连接顶掉（断线重连场景）
        if (p.getClient() != null) {
            p.getClient().putExtraData(null);
            log.error("[game({})] player[{}] replace", this.id, id);
        }

        p.connect(conn);
        p.sendMessage(MsgPacket.newInstance((byte) Message.ID.MSG_Connect_VALUE, msg.build()));

        listener.onJoinGame(this.id, id);
        return true;
    }

    /**
     * 离开游戏。
     *
     * <p>清理玩家状态并触发 onLeaveGame 回调。
     *
     * @param id 玩家 ID
     * @return true 成功离开
     */
    public boolean leaveGame(long id) {
        Player p = players.get(id);
        if (p == null) {
            return false;
        }
        p.cleanup();
        listener.onLeaveGame(this.id, id);
        return true;
    }

    /**
     * 处理消息 —— 消息分发核心。
     *
     * <p>根据消息 ID 分发到不同的处理器。
     * 所有处理器都在房间单线程中执行。
     *
     * @param id 发送者玩家 ID
     * @param msg 消息数据包
     */
    public void processMsg(long id, MsgPacket msg) {
        Player player = players.get(id);
        if (player == null) {
            log.error("[game({})] processMsg player[{}] msg=[{}]", this.id, id, msg.getMessageID());
            return;
        }

        Message.ID msgId = Message.ID.forNumber(msg.getMessageID());
        if (msgId == null) {
            return;
        }

        log.info("[game({})] processMsg player[{}] msg=[{}]", this.id, id, msgId);

        switch (msgId) {
            case MSG_JoinRoom:
                handleJoinRoom(player);
                break;
            case MSG_Progress:
                handleProgress(player, msg);
                break;
            case MSG_Heartbeat:
                handleHeartbeat(player);
                break;
            case MSG_Ready:
                handleReady(player);
                break;
            case MSG_Input:
                handleInput(player, msg);
                break;
            case MSG_Result:
                handleResult(player, msg);
                break;
            default:
                log.warn("[game({})] processMsg unknown message id[{}]", this.id, msgId);
        }
    }

    /**
     * 主逻辑 tick —— 驱动状态机。
     *
     * <p>由 {@link com.tzw.logic.room.Room#run} 每 33ms 调用一次。
     * 根据当前状态调用对应的 tick 方法。
     *
     * @param now 当前时间戳（秒）
     * @return true 继续运行，false 游戏结束（退出事件循环）
     */
    public boolean tick(long now) {
        switch (state) {
            case READY:
                return tickReady(now);
            case GAMING:
                return tickGaming();
            case OVER:
                doGameOver();
                state = State.STOP;
                log.info("[game({})] do game over", id);
                return true;
            case STOP:
                return false;
        }
        return false;
    }

    /**
     * 关闭游戏：广播 MSG_Close。
     *
     * <p>通知所有客户端房间已关闭，客户端必须强制退出。
     */
    public void close() {
        broadcast(MsgPacket.newInstance((byte) Message.ID.MSG_Close_VALUE, (Object) null));
    }

    /**
     * 清理所有玩家连接。
     *
     * <p>在房间退出时调用（{@link com.tzw.logic.room.Room#run} 的 finally 块）。
     */
    public void cleanup() {
        for (Player p : players.values()) {
            p.cleanup();
        }
        players.clear();
    }

    // ==================== 消息处理 ====================

    /**
     * 处理 JoinRoom 消息。
     *
     * <p>回复玩家的座位 ID、随机种子、其他玩家列表和加载进度。
     * 这是客户端加入房间后收到的第一个游戏消息。
     */
    private void handleJoinRoom(Player player) {
        Message.S2C_JoinRoomMsg.Builder msg = Message.S2C_JoinRoomMsg.newBuilder()
                .setRoomseatid(player.getIdx())
                .setRandomSeed(randomSeed);

        // 添加其他玩家的信息（不包括自己）
        for (Player v : players.values()) {
            if (player.getId() == v.getId()) {
                continue;
            }
            msg.addOthers(v.getId());
            msg.addPros(v.getLoadingProgress());
        }

        player.sendMessage(MsgPacket.newInstance((byte) Message.ID.MSG_JoinRoom_VALUE, msg.build()));
    }

    /**
     * 处理 Progress 消息。
     *
     * <p>更新玩家的加载进度，并广播给其他玩家（发送者除外）。
     * 只在 READY 状态处理（GAMING 状态忽略）。
     */
    private void handleProgress(Player player, MsgPacket msg) {
        if (state.ordinal() > State.READY.ordinal()) {
            return;
        }
        try {
            Message.C2S_ProgressMsg progress = msg.unmarshal(Message.C2S_ProgressMsg.getDefaultInstance());
            player.setLoadingProgress(progress.getPro());

            // 广播给其他玩家（发送者除外）
            Message.S2C_ProgressMsg broadcastMsg = Message.S2C_ProgressMsg.newBuilder()
                    .setId(player.getId())
                    .setPro(progress.getPro())
                    .build();
            broadcastExclude(MsgPacket.newInstance((byte) Message.ID.MSG_Progress_VALUE, broadcastMsg), player.getId());
        } catch (Exception e) {
            log.error("[game({})] handleProgress error: {}", this.id, e.getMessage());
        }
    }

    /**
     * 处理 Heartbeat 消息。
     *
     * <p>回复心跳并刷新玩家的心跳时间。
     * 心跳时间用于检测玩家网络状况。
     */
    private void handleHeartbeat(Player player) {
        player.sendMessage(MsgPacket.newInstance((byte) Message.ID.MSG_Heartbeat_VALUE, (Object) null));
        player.refreshHeartbeatTime();
    }

    /**
     * 处理 Ready 消息。
     *
     * <p>标记玩家已准备。如果在 GAMING 状态收到 Ready，
     * 说明是断线重连，触发 doReconnect 批量发送历史帧。
     */
    private void handleReady(Player player) {
        if (state == State.READY) {
            doReady(player);
        } else if (state == State.GAMING) {
            doReady(player);
            // 重连进来，批量发送历史帧
            doReconnect(player);
            log.warn("[game({})] doReconnect [{}]", this.id, player.getId());
        } else {
            log.error("[game({})] MSG_Ready player[{}] state error:[{}]", this.id, player.getId(), state);
        }
    }

    /**
     * 处理 Input 消息。
     *
     * <p>将玩家输入推入帧缓冲区，并设置 dirty 标志强制下一帧广播。
     * 这是帧同步的核心：收集所有玩家的输入，在下一帧广播。
     */
    private void handleInput(Player player, MsgPacket msg) {
        try {
            Message.C2S_InputMsg input = msg.unmarshal(Message.C2S_InputMsg.getDefaultInstance());
            if (!pushInput(player, input)) {
                log.warn("[game({})] processMsg player[{}] pushInput failed", this.id, player.getId());
                return;
            }
            // 收到输入后设置 dirty 标志，下一帧强制广播（客户端要求）
            dirty = true;
        } catch (Exception e) {
            log.error("[game({})] handleInput error: {}", this.id, e.getMessage());
        }
    }

    /**
     * 处理 Result 消息。
     *
     * <p>记录玩家提交的结果（获胜者 ID），并回复确认。
     * 当所有在线玩家都提交结果时，游戏结束。
     */
    private void handleResult(Player player, MsgPacket msg) {
        try {
            Message.C2S_ResultMsg resultMsg = msg.unmarshal(Message.C2S_ResultMsg.getDefaultInstance());
            result.put(player.getId(), resultMsg.getWinnerID());
            log.info("[game({})] MSG_Result player[{}] winner=[{}]", this.id, player.getId(), resultMsg.getWinnerID());
            player.sendMessage(MsgPacket.newInstance((byte) Message.ID.MSG_Result_VALUE, (Object) null));
        } catch (Exception e) {
            log.error("[game({})] handleResult error: {}", this.id, e.getMessage());
        }
    }

    // ==================== Tick 状态机 ====================

    /**
     * READY 状态的 tick 处理。
     *
     * <p>检查是否所有玩家已准备，或超时强制开始。
     * 如果超时且没有在线玩家，直接结束游戏。
     *
     * @param now 当前时间戳（秒）
     * @return true 继续运行
     */
    private boolean tickReady(long now) {
        long delta = now - startTime;
        int maxReadySec = properties.getGame().getMaxReadyTimeSeconds();

        if (delta < maxReadySec) {
            // 未超时：检查是否所有玩家已准备
            if (checkReady()) {
                doStart();
                state = State.GAMING;
            }
        } else {
            // 超时：如果有在线玩家则强制开始，否则结束
            if (getOnlinePlayerCount() > 0) {
                doStart();
                state = State.GAMING;
                log.warn("[game({})] force start game because ready state is timeout", this.id);
            } else {
                state = State.OVER;
                log.error("[game({})] game over!! nobody ready", this.id);
            }
        }
        return true;
    }

    /**
     * GAMING 状态的 tick 处理。
     *
     * <p>每帧执行：
     * <ol>
     *   <li>检查游戏是否结束（所有在线玩家提交结果）</li>
     *   <li>检查是否超时（帧数超限）</li>
     *   <li>推进帧计数器（{@link Lockstep#tick}）</li>
     *   <li>广播帧数据（{@link #broadcastFrameData}）</li>
     * </ol>
     *
     * @return true 继续运行
     */
    private boolean tickGaming() {
        if (checkOver()) {
            state = State.OVER;
            log.info("[game({})] game over successfully!!", this.id);
            return true;
        }

        if (isTimeout()) {
            state = State.OVER;
            log.warn("[game({})] game timeout", this.id);
            return true;
        }

        // 推进帧计数器
        lockstep.tick();
        // 广播帧数据给所有玩家
        broadcastFrameData();
        return true;
    }

    // ==================== 游戏流程 ====================

    /**
     * 标记玩家已准备。
     *
     * <p>如果玩家已经准备过，忽略。
     */
    private void doReady(Player player) {
        if (player.isReady()) {
            return;
        }
        player.setReady(true);
        player.sendMessage(MsgPacket.newInstance((byte) Message.ID.MSG_Ready_VALUE, (Object) null));
    }

    /**
     * 检查是否所有玩家已准备。
     *
     * @return true 如果所有玩家都已准备
     */
    private boolean checkReady() {
        for (Player v : players.values()) {
            if (!v.isReady()) {
                return false;
            }
        }
        return true;
    }

    /**
     * 开始游戏。
     *
     * <p>重置帧计数器，标记所有玩家已准备，广播 Start 消息。
     */
    private void doStart() {
        clientFrameCount = 0;
        lockstep.reset();
        for (Player v : players.values()) {
            v.setReady(true);
            v.setLoadingProgress(100);
        }
        startTime = System.currentTimeMillis() / 1000;

        Message.S2C_StartMsg startMsg = Message.S2C_StartMsg.newBuilder()
                .setTimeStamp(startTime)
                .build();
        broadcast(MsgPacket.newInstance((byte) Message.ID.MSG_Start_VALUE, startMsg));

        listener.onGameStart(this.id);
    }

    /**
     * 游戏结束处理。
     *
     * <p>触发 onGameOver 回调，房间层据此设置 closed 标志。
     */
    private void doGameOver() {
        listener.onGameOver(this.id);
    }

    /**
     * 将玩家输入推入帧缓冲区。
     *
     * @param p 玩家对象
     * @param msg 输入消息
     * @return true 成功，false 重复输入
     */
    private boolean pushInput(Player p, Message.C2S_InputMsg msg) {
        Message.InputData cmd = Message.InputData.newBuilder()
                .setId(p.getId())
                .setSid(msg.getSid())
                .setX(msg.getX())
                .setY(msg.getY())
                .setRoomseatid(p.getIdx())
                .build();
        return lockstep.pushCmd(cmd);
    }

    /**
     * 断线重连：发送 Start + 批量重放所有历史帧。
     *
     * <p>当玩家在 GAMING 状态重新发送 Ready 时调用。
     * 批量发送历史帧使玩家快速追赶到当前帧。
     *
     * <p>每 {@code maxFrameDataPerMsg} 帧打包一个消息，避免单个消息过大。
     */
    private void doReconnect(Player p) {
        // 1. 发送 Start 消息
        Message.S2C_StartMsg startMsg = Message.S2C_StartMsg.newBuilder()
                .setTimeStamp(startTime)
                .build();
        p.sendMessage(MsgPacket.newInstance((byte) Message.ID.MSG_Start_VALUE, startMsg));

        // 2. 批量重放所有历史帧
        long framesCount = clientFrameCount;
        int maxPerMsg = properties.getGame().getMaxFrameDataPerMsg();
        int count = 0;
        Message.S2C_FrameMsg.Builder frameMsg = Message.S2C_FrameMsg.newBuilder();

        for (long i = 0; i < framesCount; i++) {
            Lockstep.FrameData frameData = lockstep.getFrame(i);
            if (frameData == null && i != (framesCount - 1)) {
                continue;
            }

            Message.FrameData fd = Message.FrameData.newBuilder()
                    .setFrameID((int) i)
                    .build();

            if (frameData != null) {
                Message.FrameData fdWithInput = Message.FrameData.newBuilder()
                        .setFrameID((int) i)
                        .addAllInput(frameData.cmds)
                        .build();
                frameMsg.addFrames(fdWithInput);
            } else {
                frameMsg.addFrames(fd);
            }
            count++;

            // 每 maxPerMsg 帧或最后一帧发送一次
            if (count >= maxPerMsg || i == (framesCount - 1)) {
                p.sendMessage(MsgPacket.newInstance((byte) Message.ID.MSG_Frame_VALUE, frameMsg.build()));
                count = 0;
                frameMsg = Message.S2C_FrameMsg.newBuilder();
            }
        }

        // 3. 更新玩家的帧游标
        p.setSendFrameCount(clientFrameCount);
    }

    // ==================== 帧广播 ====================

    /**
     * 广播帧数据 —— 帧同步的热路径。
     *
     * <p>每帧执行一次，将新帧数据发送给所有在线玩家。
     * 使用每玩家独立的 {@code sendFrameCount} 游标实现增量更新。
     *
     * <p><b>广播触发条件</b>（满足其一）：
     * <ul>
     *   <li>{@code dirty == true}：收到玩家输入，强制广播</li>
     *   <li>{@code framesCount - clientFrameCount >= broadcastOffsetFrames}：帧差超过阈值</li>
     * </ul>
     *
     * <p><b>跳过条件</b>：
     * <ul>
     *   <li>玩家不在线</li>
     *   <li>玩家未准备</li>
     *   <li>玩家网络不佳（心跳超时）</li>
     * </ul>
     */
    private void broadcastFrameData() {
        long framesCount = lockstep.getFrameCount();
        int broadcastOffset = properties.getGame().getBroadcastOffsetFrames();
        int maxPerMsg = properties.getGame().getMaxFrameDataPerMsg();
        int badThresholdSec = properties.getGame().getBadNetworkThresholdSeconds();

        // 检查是否需要广播
        if (!dirty && framesCount - clientFrameCount < broadcastOffset) {
            return;
        }

        // 重置脏标志，更新客户端帧计数
        dirty = false;
        clientFrameCount = framesCount;

        long now = System.currentTimeMillis() / 1000;

        // 遍历每个玩家，发送增量帧数据
        for (Player p : players.values()) {
            // 跳过掉线的玩家
            if (!p.isOnline()) {
                continue;
            }
            // 跳过未准备的玩家
            if (!p.isReady()) {
                continue;
            }
            // 跳过网络不好的玩家（心跳超时）
            if (now - p.getLastHeartbeatTime() >= badThresholdSec) {
                continue;
            }

            // 从该玩家的帧游标开始发送
            long i = p.getSendFrameCount();
            int count = 0;
            Message.S2C_FrameMsg.Builder msg = Message.S2C_FrameMsg.newBuilder();

            for (; i < framesCount; i++) {
                Lockstep.FrameData frameData = lockstep.getFrame(i);
                // 跳过无输入的帧（除非是最后一帧）
                if (frameData == null && i != (framesCount - 1)) {
                    continue;
                }

                Message.FrameData.Builder fd = Message.FrameData.newBuilder()
                        .setFrameID((int) i);
                if (frameData != null) {
                    fd.addAllInput(frameData.cmds);
                }
                msg.addFrames(fd.build());
                count++;

                // 每 maxPerMsg 帧或最后一帧发送一次
                if (i == (framesCount - 1) || count >= maxPerMsg) {
                    p.sendMessage(MsgPacket.newInstance((byte) Message.ID.MSG_Frame_VALUE, msg.build()));
                    count = 0;
                    msg = Message.S2C_FrameMsg.newBuilder();
                }
            }

            // 更新玩家的帧游标
            p.setSendFrameCount(framesCount);
        }
    }

    // ==================== 广播辅助 ====================

    /**
     * 广播消息给所有玩家。
     *
     * @param msg 要广播的消息
     */
    private void broadcast(MsgPacket msg) {
        for (Player v : players.values()) {
            v.sendMessage(msg);
        }
    }

    /**
     * 广播消息给除指定玩家外的所有玩家。
     *
     * <p>用于 Progress 消息：发送者已经知道自己的进度，无需广播给自己。
     *
     * @param msg 要广播的消息
     * @param excludeId 排除的玩家 ID
     */
    private void broadcastExclude(MsgPacket msg, long excludeId) {
        for (Player v : players.values()) {
            if (v.getId() == excludeId) {
                continue;
            }
            v.sendMessage(msg);
        }
    }

    // ==================== 状态检查 ====================

    /**
     * 检查游戏是否结束。
     *
     * <p>当所有在线玩家都提交了结果时，游戏结束。
     * 离线玩家不参与判断（可能已永久断开）。
     *
     * @return true 如果游戏应该结束
     */
    private boolean checkOver() {
        // 只要有人没发结果并且还在线，就不结束
        for (Player v : players.values()) {
            if (!v.isOnline()) {
                continue;
            }
            if (!result.containsKey(v.getId())) {
                return false;
            }
        }
        return true;
    }

    /**
     * 检查是否超时（帧数超限）。
     *
     * <p>这是游戏的安全网，防止无限运行。
     * 最大帧数由配置决定（默认 30*60*3 + 100 = 5500 帧，约 3 分钟）。
     *
     * @return true 如果帧数超限
     */
    private boolean isTimeout() {
        return lockstep.getFrameCount() > properties.getGame().getMaxGameFrames();
    }

    /**
     * 获取在线玩家数量。
     *
     * @return 在线玩家数
     */
    private int getOnlinePlayerCount() {
        int count = 0;
        for (Player v : players.values()) {
            if (v.isOnline()) {
                count++;
            }
        }
        return count;
    }

    // ==================== Getters ====================

    public long getId() { return id; }
    public State getState() { return state; }
    public Map<Long, Long> getResult() { return result; }
}
