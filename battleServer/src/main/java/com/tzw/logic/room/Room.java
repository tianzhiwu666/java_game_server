package com.tzw.logic.room;

import com.tzw.config.LockstepProperties;
import com.tzw.logic.RoomManager;
import com.tzw.logic.game.Game;
import com.tzw.logic.game.GameListener;
import com.tzw.network.Conn;
import com.tzw.network.ConnCallback;
import com.tzw.network.Packet;
import com.tzw.packet.MsgPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 战斗房间 —— 帧同步服务器的核心抽象。
 *
 * <p><b>这是整个系统中最重要的类。</b>镜像 Go 参考实现中的 {@code logic/room/room.go}。
 * 每个房间运行一个单线程事件循环，是帧同步确定性的基石。
 *
 * <h3>单线程事件循环模式（Actor 模型）</h3>
 * <p>房间采用 Actor 模型设计：
 * <ul>
 *   <li>每个房间是一个独立的 Actor，拥有私有状态（{@link Game}）</li>
 *   <li>外部通过消息（{@link BlockingQueue}）与房间通信，不直接调用方法</li>
 *   <li>房间线程串行处理消息，无需锁即可保证状态一致性</li>
 * </ul>
 *
 * <p><b>为什么单线程？</b>帧同步的核心要求是确定性：所有客户端必须在相同帧看到相同状态。
 * 多线程并发修改游戏状态会导致竞态条件，破坏确定性。
 * 单线程事件循环从根本上消除了这个问题。
 *
 * <h3>三个 BlockingQueue 通道</h3>
 * <p>房间通过三个队列与外部通信：
 * <ul>
 *   <li><b>msgQ</b>（容量 2048）：入站消息队列。Netty 线程生产（{@link #onMessage}），
 *       房间线程消费。承载所有游戏逻辑消息（JoinRoom、Progress、Ready、Input、Result）。</li>
 *   <li><b>inChan</b>（容量 8）：连接加入队列。Netty 线程生产（{@link #onConnect}），
 *       房间线程消费。承载新建立的连接。</li>
 *   <li><b>outChan</b>（容量 8）：连接离开队列。Netty 线程生产（{@link #onClose}），
 *       房间线程消费。承载断开的连接。</li>
 * </ul>
 *
 * <p>容量选择：msgQ 较大（2048）以应对消息突发；inChan/outChan 较小（8）因为连接事件频率低。
 *
 * <h3>Netty 线程与房间线程的安全通信</h3>
 * <p>Netty 线程和房间线程通过 BlockingQueue 通信，这是线程安全的：
 * <ul>
 *   <li>Netty 线程调用 {@code offer()} 放入消息，非阻塞</li>
 *   <li>房间线程调用 {@code poll(timeout)} 取出消息，带超时以便检查 tick</li>
 *   <li>房间状态（Game 对象）只在房间线程中修改，Netty 线程不直接访问</li>
 * </ul>
 *
 * <h3>Tick 定时逻辑（30Hz）</h3>
 * <p>房间以固定频率（默认 30Hz）推进游戏状态：
 * <ul>
 *   <li>{@code tickMs = 1000 / frequency} 计算每帧毫秒数（约 33ms）</li>
 *   <li>每次循环计算 {@code waitMs = min(nextTick - now, 50)}，确保至少每 50ms 检查一次</li>
 *   <li>当 {@code now >= nextTick} 时，调用 {@link Game#tick} 推进游戏状态</li>
 *   <li>{@code nextTick += tickMs} 累加，而非设置为 {@code now + tickMs}，
 *       避免长时间阻塞后的"追赶"问题</li>
 * </ul>
 *
 * <h3>超时机制</h3>
 * <p>房间有总超时时间（默认 5 分钟）：
 * <ul>
 *   <li>{@code deadline = startTime + timeoutMs}</li>
 *   <li>每次循环检查 {@code now >= deadline}，超时则退出循环</li>
 *   <li>这是最后的安全网，防止房间无限运行</li>
 * </ul>
 *
 * <h3>关闭序列</h3>
 * <ol>
 *   <li>外部调用 {@link #stop()}，设置 {@code running = false} 并中断线程</li>
 *   <li>房间线程在下一次循环检查 {@code running} 标志，退出 while 循环</li>
 *   <li>调用 {@link Game#cleanup()} 清理所有玩家连接</li>
 *   <li>记录运行总时间并退出</li>
 * </ol>
 *
 * <h3>线程安全</h3>
 * <ul>
 *   <li>{@link #closed} 使用 {@link AtomicBoolean}，因为 {@link Router} 在 Netty 线程读取它</li>
 *   <li>{@link #loopThread} 和 {@link #running} 使用 {@code volatile} 保证可见性</li>
 *   <li>其他字段只在房间线程中访问，无需同步</li>
 * </ul>
 */
public class Room implements ConnCallback, GameListener {

    private static final Logger log = LoggerFactory.getLogger(Room.class);

    /** 房间 ID（唯一标识） */
    private final long roomId;

    /** 参与玩家 ID 列表（不可变，创建时确定） */
    private final List<Long> players;

    /** 房间类型 ID（预留） */
    private final int typeId;

    /** 房间是否已关闭（AtomicBoolean 因为被 Router 跨线程读取） */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** 房间创建时间戳（秒） */
    private final long timeStamp;

    /** 房间密钥（用于身份验证，当前为固定值） */
    private final String secretKey;

    /** 逻辑服务器标识（用于多服务器部署） */
    private final String logicServer;

    /** 配置对象 */
    private final LockstepProperties properties;

    /**
     * 入站消息队列（容量 2048）。
     *
     * <p>Netty 线程通过 {@link #onMessage} 放入消息，
     * 房间线程通过 {@link #run} 循环消费。
     * 容量 2048 可应对消息突发，同时避免内存无限增长。
     */
    private final BlockingQueue<PacketWrapper> msgQ = new LinkedBlockingQueue<>(2048);

    /**
     * 连接加入队列（容量 8）。
     *
     * <p>Netty 线程通过 {@link #onConnect} 放入新连接，
     * 房间线程消费并调用 {@link Game#joinGame}。
     */
    private final BlockingQueue<Conn> inChan = new LinkedBlockingQueue<>(8);

    /**
     * 连接离开队列（容量 8）。
     *
     * <p>Netty 线程通过 {@link #onClose} 放入断开连接，
     * 房间线程消费并调用 {@link Game#leaveGame}。
     */
    private final BlockingQueue<Conn> outChan = new LinkedBlockingQueue<>(8);

    /** 游戏逻辑对象（房间的核心状态） */
    private final Game game;

    /** 事件循环线程引用（用于 stop() 中断） */
    private volatile Thread loopThread;

    /** 事件循环运行标志（volatile 保证 stop() 的可见性） */
    private volatile boolean running = false;

    public Room(long roomId, int typeId, List<Long> playerIds, int randomSeed, String logicServer, LockstepProperties properties) {
        this.roomId = roomId;
        this.players = playerIds;
        this.typeId = typeId;
        this.timeStamp = System.currentTimeMillis() / 1000;
        this.logicServer = logicServer;
        this.properties = properties;
        this.secretKey = "test_room";
        // 创建游戏逻辑对象，将 this 作为 GameListener 回调
        this.game = new Game(roomId, playerIds, randomSeed, this, properties);
    }

    public long id() { return roomId; }
    public String secretKey() { return secretKey; }
    public long timeStamp() { return timeStamp; }

    /**
     * 检查房间是否已结束。
     *
     * <p>由 {@link Router#handleConnect} 在 Netty 线程中调用，
     * 因此使用 {@link AtomicBoolean} 保证线程安全。
     *
     * @return true 如果房间已结束
     */
    public boolean isOver() {
        return closed.get();
    }

    /**
     * 检查玩家是否属于该房间。
     *
     * <p>由 {@link Router#handleConnect} 在 Netty 线程中调用。
     * {@link #players} 是不可变列表，因此读操作是线程安全的。
     *
     * @param id 玩家 ID
     * @return true 如果玩家属于该房间
     */
    public boolean hasPlayer(long id) {
        return players.contains(id);
    }

    // ==================== ConnCallback ====================

    /**
     * 连接建立回调。
     *
     * <p>由 Netty 线程调用。将连接放入 inChan，由房间线程消费。
     * 同时将连接的回调设置为 this，使后续消息路由到 {@link #onMessage}。
     *
     * @param conn 新建立的连接
     * @return true 表示接受连接
     */
    @Override
    public boolean onConnect(Conn conn) {
        conn.setCallback(this);
        inChan.offer(conn);
        log.warn("[room({})] OnConnect {}", roomId, conn.getExtraData());
        return true;
    }

    /**
     * 消息到达回调。
     *
     * <p>由 Netty 线程调用。将消息包装为 {@link PacketWrapper} 放入 msgQ，
     * 由房间线程消费。
     *
     * <p><b>注意</b>：此处不处理消息，只是放入队列。
     * 实际处理在 {@link #run} 循环中，由房间单线程完成。
     *
     * @param conn 来源连接
     * @param packet 消息数据包
     * @return true 表示消息已接受（放入队列）
     */
    @Override
    public boolean onMessage(Conn conn, Packet packet) {
        Object id = conn.getExtraData();
        if (!(id instanceof Long)) {
            log.error("[room] OnMessage error conn don't have id");
            return false;
        }
        msgQ.offer(new PacketWrapper((Long) id, (MsgPacket) packet));
        return true;
    }

    /**
     * 连接关闭回调。
     *
     * <p>由 Netty 线程调用。将连接放入 outChan，由房间线程消费。
     *
     * @param conn 关闭的连接
     */
    @Override
    public void onClose(Conn conn) {
        outChan.offer(conn);
        Object id = conn.getExtraData();
        log.warn("[room({})] OnClose {}", roomId, id);
    }

    // ==================== GameListener ====================

    /**
     * 玩家加入游戏回调。
     *
     * <p>由 {@link Game#joinGame} 调用，在房间线程中执行。
     * 可用于触发额外的业务逻辑（如通知匹配系统）。
     */
    @Override
    public void onJoinGame(long roomId, long playerId) {
        log.warn("[room({})] onJoinGame {}", roomId, playerId);
    }

    /**
     * 游戏开始回调。
     *
     * <p>由 {@link Game#doStart} 调用，在房间线程中执行。
     */
    @Override
    public void onGameStart(long roomId) {
        log.warn("[room({})] onGameStart {}", roomId);
    }

    /**
     * 玩家离开游戏回调。
     *
     * <p>由 {@link Game#leaveGame} 调用，在房间线程中执行。
     */
    @Override
    public void onLeaveGame(long roomId, long playerId) {
        log.warn("[room({})] onLeaveGame {} {}", roomId, playerId);
    }

    /**
     * 游戏结束回调。
     *
     * <p>由 {@link Game#doGameOver} 调用，在房间线程中执行。
     * 设置 {@link #closed} 标志，阻止新连接加入。
     */
    @Override
    public void onGameOver(long roomId) {
        closed.set(true);
        // 通知匹配服务：携带所有玩家提交的胜负结果（playerId → winnerID）
        var callback = RoomManager.getGameOverCallback();
        if (callback != null) {
            try {
                callback.accept(roomId, game.getResult());
            } catch (Exception e) {
                log.error("[room({})] game over callback error: {}", roomId, e.getMessage(), e);
            }
        }
        log.warn("[room({})] onGameOver {}", roomId);
    }

    // ==================== 主循环 ====================

    /**
     * 主事件循环 —— 房间的核心。
     *
     * <p>在单线程中运行，由 30Hz 时钟和 5 分钟超时驱动。
     * 循环处理：
     * <ol>
     *   <li>检查超时</li>
     *   <li>从 msgQ 取出消息并处理（带超时以便检查 tick）</li>
     *   <li>从 inChan 取出新连接并加入游戏</li>
     *   <li>从 outChan 取出断开连接并离开游戏</li>
     *   <li>检查 tick 时间，推进游戏状态</li>
     * </ol>
     *
     * <p><b>关键设计</b>：使用 {@code poll(timeout)} 而非 {@code take()}，
     * 确保即使没有消息也能定期检查 tick 和超时。
     */
    public void run() {
        loopThread = Thread.currentThread();
        running = true;

        // 计算 tick 间隔和超时时间
        long tickMs = properties.getRoom().getTickMs();
        long timeoutMs = properties.getRoom().getTimeoutMs();
        long deadline = System.currentTimeMillis() + timeoutMs;
        long nextTick = System.currentTimeMillis() + tickMs;

        log.info("[room({})] running...", roomId);

        while (running) {
            try {
                // 1. 检查总超时（安全网，防止房间无限运行）
                long now = System.currentTimeMillis();
                if (now >= deadline) {
                    log.error("[room({})] time out", roomId);
                    break;
                }

                // 2. 计算下次 tick 的等待时间
                // 取 min(nextTick - now, 50) 确保至少每 50ms 检查一次
                long waitMs = Math.min(nextTick - now, 50);
                if (waitMs < 1) waitMs = 1;

                // 3. 处理消息（带超时以便检查 tick）
                // 使用 poll 而非 take，确保不会无限阻塞
                PacketWrapper pw = msgQ.poll(waitMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (pw != null) {
                    game.processMsg(pw.playerId, pw.packet);
                }

                // 4. 处理新连接加入
                Conn in = inChan.poll();
                if (in != null) {
                    Long id = (Long) in.getExtraData();
                    if (id != null) {
                        if (game.joinGame(id, in)) {
                            log.info("[room({})] player[{}] join room ok", roomId, id);
                        } else {
                            log.error("[room({})] player[{}] join room failed", roomId, id);
                            in.close();
                        }
                    } else {
                        in.close();
                    }
                }

                // 5. 处理连接离开
                Conn out = outChan.poll();
                if (out != null) {
                    Long id = (Long) out.getExtraData();
                    if (id != null) {
                        game.leaveGame(id);
                    }
                }

                // 6. Tick：推进游戏状态
                now = System.currentTimeMillis();
                if (now >= nextTick) {
                    if (!game.tick(now / 1000)) {
                        log.info("[room({})] tick over", roomId);
                        break;
                    }
                    // 累加而非设置，避免长时间阻塞后的"追赶"问题
                    nextTick += tickMs;
                }

            } catch (InterruptedException e) {
                // 被 stop() 中断，退出循环
                Thread.currentThread().interrupt();
                break;
            }
        }

        // 清理：关闭所有玩家连接
        game.cleanup();
        log.warn("[room({})] quit! total time={}s", roomId, (System.currentTimeMillis() / 1000) - timeStamp);
    }

    /**
     * 强制关闭房间。
     *
     * <p>由 {@link RoomManager#stop()} 调用。
     * 设置 {@code running = false} 并中断线程，触发 {@link #run} 循环退出。
     *
     * <p><b>注意</b>：该方法只是触发退出，实际清理工作在 {@link #run} 的 finally 块中完成。
     */
    public void stop() {
        running = false;
        closed.set(true);
        if (loopThread != null) {
            loopThread.interrupt();
        }
    }

    /**
     * 内部类：消息包装器。
     *
     * <p>将玩家 ID 和消息包绑定在一起，因为消息处理需要知道发送者。
     * 使用 {@link Conn#getExtraData} 获取玩家 ID，避免重复解析消息头。
     */
    private static class PacketWrapper {
        final long playerId;
        final MsgPacket packet;
        PacketWrapper(long playerId, MsgPacket packet) {
            this.playerId = playerId;
            this.packet = packet;
        }
    }
}
