package com.tzw.logic.growth;

import com.tzw.mq.MatchCreateEvent;
import com.tzw.mq.MatchReadyEvent;
import com.tzw.mq.MatchResultEvent;
import com.tzw.mq.MqProducer;
import com.tzw.network.SendQueueProbe;
import com.tzw.packet.MsgPacket;
import com.tzw.pb.Message;
import com.tzw.server.GrowthRouter.GrowthEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * GrowthActor 协议特征化测试 —— 锁定养成业务对客户端的响应字节与 MQ 发布行为。
 *
 * <p>行为基线见规格 {@code docs/superpowers/specs/2026-10-14-refactor-design.md} §3.3：
 * 拆分 Handler（Task 4/5）前后本测试都必须通过，且不得为迁就实现而修改断言。
 * 语义不当的 {@code ERR_RoomState} 错误码按"行为保持"原则原样锁定。
 *
 * <p>时序安全：Actor 在独立线程消费队列，所有断言前用轮询等待（10ms 间隔 / 2s 上限）。
 */
class GrowthActorProtocolTest {

    private static final long PLAYER_ID = 7L;
    private static final Set<Integer> GACHA_BONUS = Set.of(5, 15, 30, 50);

    private GrowthActor actor;
    private Thread thread;
    private SendQueueProbe probe;
    private RecordingMq mq;

    /** 记录 MQ 发布的 fake producer */
    private static final class RecordingMq implements MqProducer {
        final List<Object[]> sent = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void send(String topic, Object message) {
            sent.add(new Object[]{topic, message});
        }

        @Override
        public void sendAsync(String topic, Object message, Runnable callback) {
            send(topic, message);
        }
    }

    @BeforeEach
    void startActor() {
        probe = SendQueueProbe.create();
        mq = new RecordingMq();
        actor = new GrowthActor(PLAYER_ID, mq);
        thread = new Thread(actor, "test-growth-" + PLAYER_ID);
        thread.setDaemon(true);
        thread.start();

        actor.onConnect(probe.conn());
        // 连接建立立即推送一次 PlayerData（锁定现有行为）
        List<SendQueueProbe.Captured> first = await(1);
        assertEquals(List.of(Message.ID.MSG_PlayerData_VALUE), idsOf(first));
    }

    @AfterEach
    void stopActor() throws InterruptedException {
        actor.stop();
        thread.join(1000);
    }

    // ==================== 辅助 ====================

    private static MsgPacket pkt(int msgId, Object body) {
        return MsgPacket.newInstance((byte) msgId, body);
    }

    private static List<Integer> idsOf(List<SendQueueProbe.Captured> captured) {
        List<Integer> ids = new ArrayList<>();
        captured.forEach(c -> ids.add(c.msgID()));
        return ids;
    }

    /** 轮询等待至少 minCount 个响应包（2s 超时） */
    private List<SendQueueProbe.Captured> await(int minCount) {
        long deadline = System.currentTimeMillis() + 2000;
        List<SendQueueProbe.Captured> got = new ArrayList<>();
        while (System.currentTimeMillis() < deadline) {
            got.addAll(probe.drain());
            if (got.size() >= minCount) {
                return got;
            }
            sleep(10);
        }
        fail("等待消息超时：期望至少 " + minCount + " 包，实收 " + got.size() + " 包");
        return got;
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Message.S2C_UpgradeLevelMsg upgrade(SendQueueProbe.Captured c) throws Exception {
        assertEquals(Message.ID.MSG_UpgradeLevel_VALUE, c.msgID());
        return Message.S2C_UpgradeLevelMsg.parseFrom(c.body());
    }

    private static Message.S2C_GachaMsg gacha(SendQueueProbe.Captured c) throws Exception {
        assertEquals(Message.ID.MSG_Gacha_VALUE, c.msgID());
        return Message.S2C_GachaMsg.parseFrom(c.body());
    }

    // ==================== 升级 ====================

    @Test
    void upgradeSuccessThenInsufficientGold() throws Exception {
        actor.tell(pkt(Message.ID.MSG_UpgradeLevel_VALUE,
                Message.C2S_UpgradeLevelMsg.newBuilder().setTargetLevel(2).build()));
        Message.S2C_UpgradeLevelMsg r = upgrade(await(1).get(0));
        assertTrue(r.getSuccess());
        assertEquals(2, r.getNewLevel());
        assertEquals(900, r.getRemainGold());
        assertEquals(15, r.getNewAttack());
        assertEquals(Message.ERRORCODE.ERR_Ok, r.getErrorCode());

        actor.tell(pkt(Message.ID.MSG_UpgradeLevel_VALUE,
                Message.C2S_UpgradeLevelMsg.newBuilder().setTargetLevel(3).build()));
        r = upgrade(await(1).get(0));
        assertTrue(r.getSuccess());
        assertEquals(3, r.getNewLevel());
        assertEquals(500, r.getRemainGold());   // 1000-100-400
        assertEquals(20, r.getNewAttack());

        // 下一级费用 900 > 500：金币不足，ERR_RoomState 回显当前状态
        actor.tell(pkt(Message.ID.MSG_UpgradeLevel_VALUE,
                Message.C2S_UpgradeLevelMsg.newBuilder().setTargetLevel(4).build()));
        r = upgrade(await(1).get(0));
        assertFalse(r.getSuccess());
        assertEquals(Message.ERRORCODE.ERR_RoomState, r.getErrorCode());
        assertEquals(3, r.getNewLevel());
        assertEquals(500, r.getRemainGold());

        // 跨级请求非法：同样 ERR_RoomState，等级不变
        actor.tell(pkt(Message.ID.MSG_UpgradeLevel_VALUE,
                Message.C2S_UpgradeLevelMsg.newBuilder().setTargetLevel(6).build()));
        r = upgrade(await(1).get(0));
        assertFalse(r.getSuccess());
        assertEquals(Message.ERRORCODE.ERR_RoomState, r.getErrorCode());
        assertEquals(3, r.getNewLevel());
    }

    // ==================== 抽卡 ====================

    @Test
    void gachaInvalidTimes() throws Exception {
        actor.tell(pkt(Message.ID.MSG_Gacha_VALUE,
                Message.C2S_GachaMsg.newBuilder().setTimes(2).build()));
        Message.S2C_GachaMsg r = gacha(await(1).get(0));
        assertEquals(0, r.getItemsCount());
        assertEquals(1000, r.getRemainGold());
        assertEquals(Message.ERRORCODE.ERR_RoomState, r.getErrorCode());
    }

    @Test
    void gachaCostMatrix() throws Exception {
        // 十连：费用 900，gold 1000 → 100
        actor.tell(pkt(Message.ID.MSG_Gacha_VALUE,
                Message.C2S_GachaMsg.newBuilder().setTimes(10).build()));
        Message.S2C_GachaMsg r = gacha(await(1).get(0));
        assertEquals(Message.ERRORCODE.ERR_Ok, r.getErrorCode());
        assertEquals(100, r.getRemainGold());
        assertGachaItems(r, 10);

        // 再十连：100 < 900 不足
        actor.tell(pkt(Message.ID.MSG_Gacha_VALUE,
                Message.C2S_GachaMsg.newBuilder().setTimes(10).build()));
        r = gacha(await(1).get(0));
        assertEquals(Message.ERRORCODE.ERR_RoomState, r.getErrorCode());
        assertEquals(0, r.getItemsCount());
        assertEquals(100, r.getRemainGold());

        // 单抽恰好花光：100 → 0
        actor.tell(pkt(Message.ID.MSG_Gacha_VALUE,
                Message.C2S_GachaMsg.newBuilder().setTimes(1).build()));
        r = gacha(await(1).get(0));
        assertEquals(Message.ERRORCODE.ERR_Ok, r.getErrorCode());
        assertEquals(0, r.getRemainGold());
        assertGachaItems(r, 1);
    }

    /** 抽卡随机性用不变式锁定：件数合计与稀有度集合 */
    private void assertGachaItems(Message.S2C_GachaMsg r, int expectedCount) {
        int total = r.getItemsList().stream().mapToInt(Message.Item::getCount).sum();
        assertEquals(expectedCount, total);
        r.getItemsList().forEach(item -> {
            assertTrue(GACHA_BONUS.contains(item.getAttackBonus()),
                    "未知稀有度加成: " + item.getAttackBonus());
            assertTrue(item.getItemID() >= 1001 && item.getItemID() <= 1004,
                    "物品 ID 超出抽卡表范围: " + item.getItemID());
        });
    }

    // ==================== 装备 ====================

    @Test
    void equipFlow() throws Exception {
        // 先单抽获得随机物品
        actor.tell(pkt(Message.ID.MSG_Gacha_VALUE,
                Message.C2S_GachaMsg.newBuilder().setTimes(1).build()));
        gacha(await(1).get(0));

        long itemId = actor.getState().getInventory().keySet().iterator().next();
        int bonus = actor.getState().getInventory().get(itemId).getAttackBonus();

        // 装备：总攻击 = 基础 10 + 加成
        actor.tell(pkt(Message.ID.MSG_EquipItem_VALUE,
                Message.C2S_EquipItemMsg.newBuilder().setItemID(itemId).build()));
        SendQueueProbe.Captured eq = await(1).get(0);
        assertEquals(Message.ID.MSG_EquipItem_VALUE, eq.msgID());
        Message.S2C_EquipItemMsg e = Message.S2C_EquipItemMsg.parseFrom(eq.body());
        assertTrue(e.getSuccess());
        assertEquals(Message.ERRORCODE.ERR_Ok, e.getErrorCode());
        assertEquals(10 + bonus, e.getNewAttack());

        // 卸下：回到基础攻击
        actor.tell(pkt(Message.ID.MSG_UnequipItem_VALUE,
                Message.C2S_UnequipItemMsg.newBuilder().setItemID(itemId).build()));
        SendQueueProbe.Captured uq = await(1).get(0);
        assertEquals(Message.ID.MSG_UnequipItem_VALUE, uq.msgID());
        Message.S2C_UnequipItemMsg u = Message.S2C_UnequipItemMsg.parseFrom(uq.body());
        assertTrue(u.getSuccess());
        assertEquals(10, u.getNewAttack());

        // 重复卸下：当前装备为 0，不匹配 → ERR_RoomState
        actor.tell(pkt(Message.ID.MSG_UnequipItem_VALUE,
                Message.C2S_UnequipItemMsg.newBuilder().setItemID(itemId).build()));
        u = Message.S2C_UnequipItemMsg.parseFrom(await(1).get(0).body());
        assertFalse(u.getSuccess());
        assertEquals(Message.ERRORCODE.ERR_RoomState, u.getErrorCode());
    }

    // ==================== 心跳 / 数据推送 ====================

    @Test
    void heartbeatAndPushes() throws Exception {
        actor.tell(pkt(Message.ID.MSG_Heartbeat_VALUE, null));
        assertEquals(List.of(Message.ID.MSG_Heartbeat_VALUE), idsOf(await(1)));

        actor.tell(pkt(Message.ID.MSG_PlayerData_VALUE, null));
        List<SendQueueProbe.Captured> got = await(1);
        assertEquals(Message.ID.MSG_PlayerData_VALUE, got.get(0).msgID());
        Message.S2C_PlayerDataMsg pd = Message.S2C_PlayerDataMsg.parseFrom(got.get(0).body());
        assertEquals(PLAYER_ID, pd.getPlayerID());
        assertEquals(1, pd.getLevel());
        assertEquals(1000, pd.getGold());

        actor.tell(pkt(Message.ID.MSG_Inventory_VALUE, null));
        got = await(1);
        assertEquals(Message.ID.MSG_Inventory_VALUE, got.get(0).msgID());
        assertEquals(0, Message.S2C_InventoryMsg.parseFrom(got.get(0).body()).getItemsCount());
    }

    // ==================== 匹配 ====================

    @Test
    void enterMatchPublishesMq() {
        actor.tell(pkt(Message.ID.MSG_EnterMatch_VALUE, null));

        long deadline = System.currentTimeMillis() + 2000;
        while (System.currentTimeMillis() < deadline && mq.sent.isEmpty()) {
            sleep(10);
        }
        assertEquals(1, mq.sent.size());
        assertEquals("match.create", mq.sent.get(0)[0]);
        assertEquals(new MatchCreateEvent(PLAYER_ID, 1), mq.sent.get(0)[1]);
        // 现有行为：EnterMatch 不回客户端包
        assertTrue(probe.drain().isEmpty());
    }

    @Test
    void matchReadyEventPushesClient() throws Exception {
        actor.tellEvent(new GrowthEvent("match.ready",
                new MatchReadyEvent(55L, PLAYER_ID, "10.0.0.2", 10086, "tk")));

        SendQueueProbe.Captured c = await(1).get(0);
        assertEquals(Message.ID.MSG_MatchReady_VALUE, c.msgID());
        Message.S2C_MatchReadyMsg m = Message.S2C_MatchReadyMsg.parseFrom(c.body());
        assertEquals(55, m.getRoomID());
        assertEquals("10.0.0.2", m.getRoomHost());
        assertEquals(10086, m.getRoomPort());
        assertEquals("tk", m.getToken());
    }

    @Test
    void matchResultAutoLevelUp() throws Exception {
        // exp 100 ≥ 升级所需 100*1²：自动升到 2 级
        actor.tellEvent(new GrowthEvent("match.result",
                new MatchResultEvent(55L, PLAYER_ID, true, 100, 50, 0)));

        SendQueueProbe.Captured c = await(1).get(0);
        assertEquals(Message.ID.MSG_MatchResult_VALUE, c.msgID());
        Message.S2C_MatchResultMsg m = Message.S2C_MatchResultMsg.parseFrom(c.body());
        assertEquals(55, m.getRoomID());
        assertTrue(m.getWin());
        assertEquals(100, m.getExpGain());
        assertEquals(50, m.getGoldGain());
        assertEquals(2, m.getNewLevel());

        assertEquals(2, actor.getState().getLevel());
        assertEquals(1050, actor.getState().getGold());
    }
}
