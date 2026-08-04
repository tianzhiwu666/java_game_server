package com.tzw.logic.match;

import com.tzw.logic.RoomManager;
import com.tzw.logic.room.Room;
import com.tzw.mq.MatchCreateEvent;
import com.tzw.mq.MatchReadyEvent;
import com.tzw.mq.MatchResultEvent;
import com.tzw.mq.MqProducer;
import com.tzw.mq.TypedMqConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 匹配服务 —— 战斗服务的匹配逻辑。
 *
 * <p>通过 MQ 订阅养成服务发布的 match.create 事件：
 * <ol>
 *   <li>同模式玩家进入等待队列，凑满 {@link #PLAYERS_PER_ROOM} 人后创建房间</li>
 *   <li>单人等待超过 {@link #SOLO_MATCH_TIMEOUT_MS} 时单独开房（便于单机调试）</li>
 *   <li>创建房间后向每个玩家发布 match.ready（含房间地址与 token）</li>
 * </ol>
 *
 * <p>房间对战结束时（由 {@link Room} 经 {@link RoomManager} 回调触发），
 * 根据所有玩家提交的胜负结果判定赢家，并逐玩家发布 match.result。
 */
public class MatchService {

    private static final Logger log = LoggerFactory.getLogger(MatchService.class);

    /** 每局玩家数（2 人配对） */
    private static final int PLAYERS_PER_ROOM = 2;

    /** 单人等待超时：超过后单独开房，避免一直排不到人 */
    private static final long SOLO_MATCH_TIMEOUT_MS = 10_000;

    private final RoomManager roomManager;
    private final MqProducer mqProducer;
    private final TypedMqConsumer mqConsumer;

    /** 房间 ID → 参与玩家列表 */
    private final Map<Long, List<Long>> roomPlayers = new ConcurrentHashMap<>();

    /** 等待中的玩家：匹配模式 → 等待队列（跨线程安全） */
    private final Map<Integer, ConcurrentLinkedDeque<WaitingPlayer>> waitingByMode = new ConcurrentHashMap<>();

    /** 房间 ID 生成器 */
    private final AtomicLong roomIdSeq = new AtomicLong(System.currentTimeMillis() * 1000);

    /** 战斗服务地址（写入 match.ready 通知客户端连接） */
    private final String roomHost;
    private final int roomPort;

    /** 定时清扫超时单人玩家 */
    private final ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "match-sweeper");
        t.setDaemon(true);
        return t;
    });

    private record WaitingPlayer(long playerId, long enqueueTime) {}

    public MatchService(RoomManager roomManager, MqProducer mqProducer, TypedMqConsumer mqConsumer) {
        this(roomManager, mqProducer, mqConsumer, "127.0.0.1", 10086);
    }

    public MatchService(RoomManager roomManager, MqProducer mqProducer, TypedMqConsumer mqConsumer,
                        String roomHost, int roomPort) {
        this.roomManager = roomManager;
        this.mqProducer = mqProducer;
        this.mqConsumer = mqConsumer;
        this.roomHost = roomHost;
        this.roomPort = roomPort;

        // 注册对战结束回调（Room 游戏结束时触发）
        RoomManager.setGameOverCallback(this::onRoomGameOver);

        // 订阅匹配请求
        subscribeMatchCreate();

        // 定期把超时未配对的单人玩家开 solo 房
        sweeper.scheduleWithFixedDelay(this::sweepWaiting, SOLO_MATCH_TIMEOUT_MS, 2_000, TimeUnit.MILLISECONDS);
    }

    /**
     * 订阅匹配创建事件
     */
    private void subscribeMatchCreate() {
        mqConsumer.subscribeTyped("match.create", MatchCreateEvent.class, this::handleMatchCreate);
        log.info("[MatchService] subscribed to match.create");
    }

    /**
     * 处理匹配创建请求（MQ 消费线程调用，同实例内串行）。
     *
     * <p>优先与同模式等待玩家配对；无等待玩家则入队等待；
     * 入队前先把超时的单人玩家开 solo 房。
     */
    void handleMatchCreate(MatchCreateEvent event) {
        long playerId = event.playerId();
        int mode = event.mode();
        log.info("[MatchService] handle match.create: playerId={}, mode={}", playerId, mode);

        try {
            ConcurrentLinkedDeque<WaitingPlayer> queue =
                    waitingByMode.computeIfAbsent(mode, m -> new ConcurrentLinkedDeque<>());

            // 1. 优先配对：有等待玩家则两人开房
            WaitingPlayer peer = queue.poll();
            if (peer != null) {
                startRoom(List.of(peer.playerId(), playerId), mode);
                return;
            }

            // 2. 无人可配，入队等待（超时单人由 sweepWaiting 定时开 solo 房）
            long now = System.currentTimeMillis();
            queue.offer(new WaitingPlayer(playerId, now));
            log.info("[MatchService] player {} waiting for match, mode={}", playerId, mode);
        } catch (Exception e) {
            log.error("[MatchService] failed to handle match.create: playerId={}, error={}",
                    playerId, e.getMessage(), e);
        }
    }

    /**
     * 定时清扫：把超过 {@link #SOLO_MATCH_TIMEOUT_MS} 仍未配对的单人玩家开 solo 房。
     */
    private void sweepWaiting() {
        long now = System.currentTimeMillis();
        for (Map.Entry<Integer, ConcurrentLinkedDeque<WaitingPlayer>> entry : waitingByMode.entrySet()) {
            ConcurrentLinkedDeque<WaitingPlayer> queue = entry.getValue();
            WaitingPlayer stale = queue.peek();
            if (stale != null && now - stale.enqueueTime() >= SOLO_MATCH_TIMEOUT_MS) {
                queue.poll();
                log.info("[MatchService] solo match for player {} (mode={})", stale.playerId(), entry.getKey());
                startRoom(List.of(stale.playerId()), entry.getKey());
            }
        }
    }

    /**
     * 创建房间并向所有玩家发布 match.ready。
     */
    private void startRoom(List<Long> players, int mode) {
        long roomId = generateRoomId();
        Room room = roomManager.createRoom(
                roomId,
                mode,
                players,
                (int) (System.currentTimeMillis() % Integer.MAX_VALUE),
                "battle-server");
        roomPlayers.put(roomId, new ArrayList<>(players));

        log.info("[MatchService] room created: roomId={}, players={}", roomId, players);

        for (Long playerId : players) {
            String token = generateToken(playerId, roomId);
            MatchReadyEvent readyEvent = new MatchReadyEvent(roomId, playerId, roomHost, roomPort, token);
            mqProducer.send("match.ready", readyEvent);
        }
    }

    /**
     * 房间对战结束回调（Room 线程调用）。
     *
     * <p>根据玩家提交的胜负结果（playerId → winnerID）判定赢家，
     * 逐玩家发布 match.result，由养成服发放奖励。
     */
    private void onRoomGameOver(long roomId, Map<Long, Long> results) {
        List<Long> players = roomPlayers.remove(roomId);
        if (players == null) {
            log.warn("[MatchService] room {} not found in player map", roomId);
            return;
        }

        long winnerId = determineWinner(results);
        log.info("[MatchService] room game over: roomId={}, players={}, winner={}",
                roomId, players, winnerId);

        for (Long playerId : players) {
            boolean win = winnerId != 0 && winnerId == playerId;
            long expGain = win ? 100 : 30;
            long goldGain = win ? 50 : 10;
            MatchResultEvent resultEvent = new MatchResultEvent(roomId, playerId, win, expGain, goldGain, 0);
            mqProducer.send("match.result", resultEvent);
        }
    }

    /**
     * 判定赢家：取所有玩家上报 winnerID 的多数票。
     * 票数并列或无人上报时返回 0（平局，无胜者）。
     */
    private long determineWinner(Map<Long, Long> results) {
        if (results == null || results.isEmpty()) {
            return 0;
        }
        Map<Long, Integer> votes = new HashMap<>();
        for (Long winner : results.values()) {
            if (winner != null && winner != 0) {
                votes.merge(winner, 1, Integer::sum);
            }
        }
        long best = 0;
        int bestVotes = 0;
        boolean tie = false;
        for (Map.Entry<Long, Integer> entry : votes.entrySet()) {
            if (entry.getValue() > bestVotes) {
                best = entry.getKey();
                bestVotes = entry.getValue();
                tie = false;
            } else if (entry.getValue() == bestVotes) {
                tie = true;
            }
        }
        return tie ? 0 : best;
    }

    private long generateRoomId() {
        return roomIdSeq.incrementAndGet();
    }

    private String generateToken(long playerId, long roomId) {
        return UUID.randomUUID().toString().replace("-", "") + "_" + playerId + "_" + roomId;
    }

    /**
     * 关闭匹配服务（停止清扫线程），由战斗服 stop() 调用。
     */
    public void close() {
        sweeper.shutdownNow();
        log.info("[MatchService] closed");
    }

    public RoomManager getRoomManager() {
        return roomManager;
    }
}
