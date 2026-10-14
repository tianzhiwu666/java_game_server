package com.tzw.logic.growth.handler;

import com.tzw.logic.growth.GrowthMessenger;
import com.tzw.logic.growth.PlayerGrowthState;
import com.tzw.mq.MatchCreateEvent;
import com.tzw.mq.MatchReadyEvent;
import com.tzw.mq.MatchResultEvent;
import com.tzw.mq.MqProducer;
import com.tzw.mq.MqTopics;
import com.tzw.packet.MsgPacket;
import com.tzw.pb.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 匹配业务处理器 —— 从 {@link com.tzw.logic.growth.GrowthActor} 逐行搬移，行为不变。
 *
 * <p>覆盖三段流程：
 * <ul>
 *   <li>{@link #handleEnterMatch()}：发布 match.create 到 MQ</li>
 *   <li>{@link #handleMatchReady}：匹配成功 → 推送房间地址给客户端</li>
 *   <li>{@link #handleMatchResult}：对战结算 → 发放奖励 + 自动升级 + 推送结算</li>
 * </ul>
 */
public class MatchHandler {

    /** 默认匹配模式（与拆分前 GrowthActor 内联常量一致） */
    private static final int DEFAULT_MATCH_MODE = 1;

    private static final Logger log = LoggerFactory.getLogger(MatchHandler.class);

    private final long playerId;
    private final PlayerGrowthState state;
    private final GrowthMessenger messenger;
    private final MqProducer mqProducer;

    public MatchHandler(long playerId, PlayerGrowthState state, GrowthMessenger messenger,
                        MqProducer mqProducer) {
        this.playerId = playerId;
        this.state = state;
        this.messenger = messenger;
        this.mqProducer = mqProducer;
    }

    /**
     * 处理进入匹配请求
     *
     * <p>发布 MatchCreateEvent 到 MQ，战斗服务订阅后创建房间。
     */
    public void handleEnterMatch() {
        log.info("[GrowthActor-{}] enter match", playerId);

        // 发布匹配请求到 MQ
        if (mqProducer != null) {
            MatchCreateEvent event = new MatchCreateEvent(playerId, DEFAULT_MATCH_MODE);
            mqProducer.send(MqTopics.MATCH_CREATE, event);
            log.info("[GrowthActor-{}] published match.create event", playerId);
        } else {
            log.warn("[GrowthActor-{}] mqProducer not available", playerId);
        }
    }

    /**
     * 处理匹配成功事件 — 通知客户端进入战斗
     *
     * @param event 匹配就绪事件
     */
    public void handleMatchReady(MatchReadyEvent event) {
        log.info("[GrowthActor-{}] match ready: roomId={}, token={}", playerId, event.roomId(), event.token());

        // 推送匹配成功消息给客户端
        Message.S2C_MatchReadyMsg matchReady = Message.S2C_MatchReadyMsg.newBuilder()
                .setRoomHost(event.roomHost())
                .setRoomPort(event.roomPort())
                .setToken(event.token())
                .setRoomID(event.roomId())
                .build();
        messenger.send(MsgPacket.newInstance((byte) Message.ID.MSG_MatchReady_VALUE, matchReady));
    }

    /**
     * 处理对战结果 —— 发放奖励
     *
     * @param event 对战结果事件
     */
    public void handleMatchResult(MatchResultEvent event) {
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
        messenger.send(MsgPacket.newInstance((byte) Message.ID.MSG_MatchResult_VALUE, result));
    }
}
