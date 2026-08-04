package com.tzw.logic.match;

import com.tzw.config.LockstepProperties;
import com.tzw.logic.RoomManager;
import com.tzw.mq.InMemoryMqAdapter;
import com.tzw.mq.MatchCreateEvent;
import com.tzw.mq.MatchReadyEvent;
import com.tzw.mq.MatchResultEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 匹配服务单元测试：双人配对、房间创建、胜负结算。
 *
 * <p>使用 {@link InMemoryMqAdapter} 模拟 MQ，不依赖真实 Redis。
 */
class MatchServiceTest {

    /** 记录发布的 match.ready / match.result，其余透传 */
    private static class CollectingMq extends InMemoryMqAdapter {
        final List<MatchReadyEvent> ready = new ArrayList<>();
        final List<MatchResultEvent> results = new ArrayList<>();

        @Override
        public void send(String topic, Object message) {
            if ("match.ready".equals(topic)) {
                ready.add((MatchReadyEvent) message);
            } else if ("match.result".equals(topic)) {
                results.add((MatchResultEvent) message);
            }
            super.send(topic, message);
        }
    }

    @Test
    void singlePlayerWaitsThenPairs() {
        CollectingMq mq = new CollectingMq();
        RoomManager roomManager = new RoomManager(new LockstepProperties());
        MatchService matchService = new MatchService(roomManager, mq, mq);
        try {
            // 第一个玩家进入等待队列，不应创建房间
            matchService.handleMatchCreate(new MatchCreateEvent(1L, 1));
            assertEquals(0, roomManager.roomNum());
            assertTrue(mq.ready.isEmpty());

            // 第二个玩家到达，两人配对成功
            matchService.handleMatchCreate(new MatchCreateEvent(2L, 1));
            assertEquals(1, roomManager.roomNum());
            assertEquals(2, mq.ready.size());
            assertEquals(mq.ready.get(0).roomId(), mq.ready.get(1).roomId());
            assertEquals(1L, mq.ready.get(0).playerId());
            assertEquals(2L, mq.ready.get(1).playerId());
            assertEquals(10086, mq.ready.get(0).roomPort());
        } finally {
            matchService.close();
            roomManager.stop();
        }
    }

    @Test
    void differentModesDoNotPair() {
        CollectingMq mq = new CollectingMq();
        RoomManager roomManager = new RoomManager(new LockstepProperties());
        MatchService matchService = new MatchService(roomManager, mq, mq);
        try {
            matchService.handleMatchCreate(new MatchCreateEvent(1L, 1));
            matchService.handleMatchCreate(new MatchCreateEvent(2L, 2));
            assertEquals(0, roomManager.roomNum());
            assertTrue(mq.ready.isEmpty());
        } finally {
            matchService.close();
            roomManager.stop();
        }
    }

    @Test
    void gameOverPublishesResultPerPlayer() {
        CollectingMq mq = new CollectingMq();
        RoomManager roomManager = new RoomManager(new LockstepProperties());
        MatchService matchService = new MatchService(roomManager, mq, mq);
        try {
            matchService.handleMatchCreate(new MatchCreateEvent(1L, 1));
            matchService.handleMatchCreate(new MatchCreateEvent(2L, 1));
            long roomId = mq.ready.get(0).roomId();

            // 两名玩家都上报 1 号获胜
            RoomManager.getGameOverCallback().accept(roomId, Map.of(1L, 1L, 2L, 1L));

            assertEquals(2, mq.results.size());
            MatchResultEvent p1 = mq.results.stream().filter(r -> r.playerId() == 1L).findFirst().orElseThrow();
            MatchResultEvent p2 = mq.results.stream().filter(r -> r.playerId() == 2L).findFirst().orElseThrow();
            assertTrue(p1.win());
            assertEquals(100, p1.expGain());
            assertEquals(50, p1.goldGain());
            assertTrue(!p2.win());
            assertEquals(30, p2.expGain());
            assertEquals(10, p2.goldGain());
        } finally {
            matchService.close();
            roomManager.stop();
        }
    }

    @Test
    void conflictingResultsAreDraw() {
        CollectingMq mq = new CollectingMq();
        RoomManager roomManager = new RoomManager(new LockstepProperties());
        MatchService matchService = new MatchService(roomManager, mq, mq);
        try {
            matchService.handleMatchCreate(new MatchCreateEvent(1L, 1));
            matchService.handleMatchCreate(new MatchCreateEvent(2L, 1));
            long roomId = mq.ready.get(0).roomId();

            // 各说各赢 → 平局，无人获胜
            RoomManager.getGameOverCallback().accept(roomId, Map.of(1L, 1L, 2L, 2L));

            assertEquals(2, mq.results.size());
            assertTrue(mq.results.stream().noneMatch(MatchResultEvent::win));
        } finally {
            matchService.close();
            roomManager.stop();
        }
    }
}
