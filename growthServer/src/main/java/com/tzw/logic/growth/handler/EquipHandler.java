package com.tzw.logic.growth.handler;

import com.tzw.logic.growth.GrowthMessenger;
import com.tzw.logic.growth.PlayerGrowthState;
import com.tzw.packet.MsgPacket;
import com.tzw.pb.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 装备/卸下业务处理器 —— 从 {@link com.tzw.logic.growth.GrowthActor} 逐行搬移，行为不变。
 *
 * <p>只在玩家 Actor 单线程中调用，无需同步。
 */
public class EquipHandler {

    private static final Logger log = LoggerFactory.getLogger(EquipHandler.class);

    private final long playerId;
    private final PlayerGrowthState state;
    private final GrowthMessenger messenger;

    public EquipHandler(long playerId, PlayerGrowthState state, GrowthMessenger messenger) {
        this.playerId = playerId;
        this.state = state;
        this.messenger = messenger;
    }

    /**
     * 处理装备请求
     */
    public void handleEquip(MsgPacket msg) {
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
        messenger.send(MsgPacket.newInstance((byte) Message.ID.MSG_EquipItem_VALUE, response));
    }

    /**
     * 处理卸下装备请求
     */
    public void handleUnequip(MsgPacket msg) {
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
        messenger.send(MsgPacket.newInstance((byte) Message.ID.MSG_UnequipItem_VALUE, response));
    }
}
