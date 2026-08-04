package com.tzw.logic;

import com.tzw.config.LockstepProperties;
import com.tzw.logic.room.Room;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 房间管理器 —— 管理所有活跃房间的创建、查找和销毁。
 *
 * <p>镜像 Go 参考实现中的 {@code logic/manager.go}。
 * 房间 ID → {@link Room} 的并发映射，是房间生命周期的唯一入口。
 *
 * <h3>在帧同步系统中的角色</h3>
 * <p>该类是房间管理的顶层组件，提供：
 * <ul>
 *   <li>{@link #createRoom} — 创建房间并启动其事件循环</li>
 *   <li>{@link #getRoom} — 根据 ID 查找房间（供 Router 使用）</li>
 *   <li>{@link #stop} — 停止所有房间（供服务器关闭时使用）</li>
 * </ul>
 *
 * <h3>每房间单线程执行器模式</h3>
 * <p>这是帧同步服务器的核心设计模式，镜像 Go 的 goroutine：
 * <ul>
 *   <li>每个房间绑定一个 {@code newSingleThreadExecutor}，确保房间逻辑串行执行</li>
 *   <li>线程命名为 "room-{id}"，便于调试和日志追踪</li>
 *   <li>线程设为守护线程（daemon），避免阻止 JVM 退出</li>
 *   <li>房间退出后自动清理映射和关闭执行器</li>
 * </ul>
 *
 * <p><b>为什么单线程？</b>帧同步的核心是确定性：所有客户端必须在相同帧看到相同状态。
 * 单线程事件循环消除了并发竞争，无需锁即可保证状态一致性。
 * 这是与 Go 的"每房间一个 goroutine"模式等价的 Java 实现。
 *
 * <h3>线程安全</h3>
 * <ul>
 *   <li>{@link #rooms} 和 {@link #roomExecutors} 使用 {@link ConcurrentHashMap}，
 *       保证并发创建/查找的安全性</li>
 *   <li>{@link #createRoom} 使用 {@link ConcurrentHashMap#putIfAbsent} 原子操作，
 *       防止重复创建同 ID 房间</li>
 *   <li>房间内部的 {@link Room} 状态由房间单线程访问，无需同步</li>
 * </ul>
 */
public class RoomManager {

    private static final Logger log = LoggerFactory.getLogger(RoomManager.class);

    /** 房间 ID → Room 对象的并发映射 */
    private final ConcurrentHashMap<Long, Room> rooms = new ConcurrentHashMap<>();

    /** 房间 ID → 单线程执行器的并发映射（镜像 Go 的 goroutine） */
    private final ConcurrentHashMap<Long, ExecutorService> roomExecutors = new ConcurrentHashMap<>();

    /** 配置对象，传递给每个房间 */
    private final LockstepProperties properties;

    /**
     * 房间对战结束回调（由 MatchService 设置）。
     * 参数：roomId + 玩家提交的胜负结果映射（playerId → winnerID）。
     */
    private static volatile java.util.function.BiConsumer<Long, java.util.Map<Long, Long>> gameOverCallback;

    /** 设置对战结束回调 */
    public static void setGameOverCallback(
            java.util.function.BiConsumer<Long, java.util.Map<Long, Long>> callback) {
        gameOverCallback = callback;
    }

    /** 获取对战结束回调 */
    public static java.util.function.BiConsumer<Long, java.util.Map<Long, Long>> getGameOverCallback() {
        return gameOverCallback;
    }

    public RoomManager(LockstepProperties properties) {
        this.properties = properties;
    }

    /**
     * 创建房间并启动其事件循环。
     *
     * <p>创建流程：
     * <ol>
     *   <li>构造 {@link Room} 对象（包含 {@link com.tzw.logic.game.Game}）</li>
     *   <li>使用 {@link ConcurrentHashMap#putIfAbsent} 原子插入，防止重复创建</li>
     *   <li>创建单线程执行器并提交 {@link Room#run()} 任务</li>
     *   <li>任务完成后自动清理映射和关闭执行器</li>
     * </ol>
     *
     * @param id 房间 ID（唯一标识）
     * @param typeId 房间类型 ID（预留，当前未使用）
     * @param playerIds 参与玩家 ID 列表
     * @param randomSeed 随机种子（用于确定性计算）
     * @param logicServer 逻辑服务器标识（用于多服务器部署）
     * @return 创建的房间对象
     * @throws IllegalArgumentException 如果房间 ID 已存在
     */
    public Room createRoom(long id, int typeId, List<Long> playerIds, int randomSeed, String logicServer) {
        // 构造房间对象（此时不启动事件循环）
        Room room = new Room(id, typeId, playerIds, randomSeed, logicServer, properties);

        // 原子插入：如果房间已存在则抛出异常
        Room existing = rooms.putIfAbsent(id, room);
        if (existing != null) {
            throw new IllegalArgumentException("room id[" + id + "] exists");
        }

        // 每房间一个单线程执行器（镜像 Go 的 goroutine）
        // 线程命名便于调试，daemon 标志防止阻止 JVM 退出
        ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "room-" + id);
            t.setDaemon(true);
            return t;
        });
        roomExecutors.put(id, executor);

        // 提交房间事件循环任务
        // finally 块确保房间退出后自动清理，防止内存泄漏
        executor.submit(() -> {
            try {
                room.run();
            } finally {
                rooms.remove(id);
                roomExecutors.remove(id);
                executor.shutdown();
                log.info("[RoomManager] room {} removed, remaining={}", id, rooms.size());
            }
        });

        return room;
    }

    /**
     * 根据 ID 获取房间。
     *
     * <p>由 {@link Router#handleConnect} 调用，用于校验房间存在性。
     * 该操作是纯读操作，不涉及房间内部状态修改。
     *
     * @param id 房间 ID
     * @return 房间对象，如果不存在则返回 null
     */
    public Room getRoom(long id) {
        return rooms.get(id);
    }

    /**
     * 获取当前活跃房间数量。
     *
     * <p>用于监控和 REST API。
     *
     * @return 房间数量
     */
    public int roomNum() {
        return rooms.size();
    }

    /**
     * 停止所有房间。
     *
     * <p>由 {@link LockStepServer#stop()} 调用，在服务器关闭时执行。
     * 停止流程：
     * <ol>
     *   <li>调用每个房间的 {@link Room#stop()} 方法，触发事件循环退出</li>
     *   <li>调用每个执行器的 {@link ExecutorService#shutdownNow()}，中断阻塞的线程</li>
     *   <li>清空映射，释放引用</li>
     * </ol>
     *
     * <p><b>注意</b>：{@link Room#stop()} 只是设置标志位并中断线程，
     * 房间的实际清理工作（{@link com.tzw.logic.game.Game#cleanup}）
     * 在 {@link Room#run()} 的 finally 块中完成。
     */
    public void stop() {
        log.info("[RoomManager] stopping all rooms, count={}", rooms.size());
        for (Room room : rooms.values()) {
            room.stop();
        }
        for (ExecutorService executor : roomExecutors.values()) {
            executor.shutdownNow();
        }
        rooms.clear();
        roomExecutors.clear();
    }
}
