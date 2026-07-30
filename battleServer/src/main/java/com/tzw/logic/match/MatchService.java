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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 匹配服务 —— 战斗服务的匹配逻辑。
 *
 * <p>订阅养成服务发布的 match.create 事件，创建房间并返回 match.ready。
 * 房间对战结束时，发布 match.result 给养成服务。
 */
public class MatchService {

    private static final Logger log = LoggerFactory.getLogger(MatchService.class);

    private final RoomManager roomManager;
    private final MqProducer mqProducer;
    private final TypedMqConsumer mqConsumer;

    /** 房间 ID → 玩家 ID 映射 */
    private final Map<Long, Long> roomPlayerMap = new ConcurrentHashMap<>();

    /** 战斗服务地址 */
    private final String roomHost = "127.0.0.1";
    private final int roomPort = 10086;

    public MatchService(RoomManager roomManager, MqProducer mqProducer, TypedMqConsumer mqConsumer) {
        this.roomManager = roomManager;
        this.mqProducer = mqProducer;
        this.mqConsumer = mqConsumer;

        // 注册对战结束回调
        RoomManager.setGameOverCallback(this::onRoomGameOver);

        // 订阅匹配请求
        subscribeMatchCreate();
    }

    /**
     * 订阅匹配创建事件
     */
    private void subscribeMatchCreate() {
        mqConsumer.subscribeTyped("match.create", MatchCreateEvent.class, this::handleMatchCreate);
        log.info("[MatchService] subscribed to match.create");
    }

    /**
     * 处理匹配创建请求
     */
    private void handleMatchCreate(MatchCreateEvent event) {
        long playerId = event.playerId();
        int mode = event.mode();

        log.info("[MatchService] handle match.create: playerId={}, mode={}", playerId, mode);

        try {
            long roomId = generateRoomId();
            Room room = roomManager.createRoom(
                    roomId,
                    mode,
                    java.util.Collections.singletonList(playerId),
                    (int) (System.currentTimeMillis() % Integer.MAX_VALUE),
                    "battle-server"
            );

            roomPlayerMap.put(roomId, playerId);
            String token = generateToken(playerId, roomId);

            MatchReadyEvent readyEvent = new MatchReadyEvent(roomId, playerId, roomHost, roomPort, token);
            mqProducer.send("match.ready", readyEvent);

            log.info("[MatchService] room created: roomId={}, playerId={}", roomId, playerId);

        } catch (Exception e) {
            log.error("[MatchService] failed to create room: playerId={}, error={}", playerId, e.getMessage(), e);
        }
    }

    /**
     * 房间对战结束回调
     */
    private void onRoomGameOver(long roomId, boolean win) {
        Long playerId = roomPlayerMap.remove(roomId);
        if (playerId == null) {
            log.warn("[MatchService] room {} not found in player map", roomId);
            return;
        }

        log.info("[MatchService] room game over: roomId={}, playerId={}, win={}", roomId, playerId, win);

        long expGain = win ? 100 : 30;
        long goldGain = win ? 50 : 10;

        MatchResultEvent resultEvent = new MatchResultEvent(roomId, playerId, win, expGain, goldGain, 0);
        mqProducer.send("match.result", resultEvent);
    }

    private long generateRoomId() {
        return System.currentTimeMillis() * 1000 + (long) (Math.random() * 1000);
    }

    private String generateToken(long playerId, long roomId) {
        return UUID.randomUUID().toString().replace("-", "") + "_" + playerId + "_" + roomId;
    }

    public RoomManager getRoomManager() {
        return roomManager;
    }
}
