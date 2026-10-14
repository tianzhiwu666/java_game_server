package com.tzw.logic.growth.handler;

import com.tzw.logic.growth.GrowthMessenger;
import com.tzw.logic.growth.PlayerGrowthState;
import com.tzw.packet.MsgPacket;
import com.tzw.pb.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 升级业务处理器 —— 从 {@link com.tzw.logic.growth.GrowthActor} 逐行搬移，行为不变。
 *
 * <p>只在玩家 Actor 单线程中调用，无需同步。
 */
public class UpgradeHandler {

    private static final Logger log = LoggerFactory.getLogger(UpgradeHandler.class);

    private final long playerId;
    private final PlayerGrowthState state;
    private final GrowthMessenger messenger;

    public UpgradeHandler(long playerId, PlayerGrowthState state, GrowthMessenger messenger) {
        this.playerId = playerId;
        this.state = state;
        this.messenger = messenger;
    }

    /**
     * 处理升级请求
     */
    public void handle(MsgPacket msg) {
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
        messenger.send(MsgPacket.newInstance((byte) Message.ID.MSG_UpgradeLevel_VALUE, response));
    }
}
