package com.tzw.logic.growth.handler;

import com.tzw.logic.growth.GrowthMessenger;
import com.tzw.logic.growth.PlayerGrowthState;
import com.tzw.packet.MsgPacket;
import com.tzw.pb.Message;
import com.tzw.pb.Message.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * 抽卡业务处理器 —— 从 {@link com.tzw.logic.growth.GrowthActor} 逐行搬移，行为不变。
 *
 * <p>概率表数据驱动化（语义严格对应旧 if-else 链）：
 * 掷点 rarity ∈ [0,100)，落入首个 {@code rarity < upperExclusive} 的档。
 *
 * <p>{@link Random} 可注入（测试用固定种子），默认与拆分前一致为 {@code new Random()}。
 */
public class GachaHandler {

    private static final Logger log = LoggerFactory.getLogger(GachaHandler.class);

    /** 稀有度档：掷点小于 upperExclusive 落入本档；末档覆盖剩余概率 */
    private record RarityTier(int upperExclusive, long itemId, String name, int attackBonus) {}

    private static final RarityTier[] TIERS = {
            new RarityTier(5, 1001, "传说武器", 50),    // 5% 传说
            new RarityTier(20, 1002, "史诗武器", 30),    // 15% 史诗
            new RarityTier(50, 1003, "稀有武器", 15),    // 30% 稀有
            new RarityTier(100, 1004, "普通武器", 5),    // 50% 普通（兜底档）
    };

    /** 单抽费用 */
    private static final long COST_SINGLE = 100;

    /** 十连费用 */
    private static final long COST_TEN = 900;

    private final long playerId;
    private final PlayerGrowthState state;
    private final GrowthMessenger messenger;
    private final Random random;

    public GachaHandler(long playerId, PlayerGrowthState state, GrowthMessenger messenger) {
        this(playerId, state, messenger, new Random());
    }

    GachaHandler(long playerId, PlayerGrowthState state, GrowthMessenger messenger, Random random) {
        this.playerId = playerId;
        this.state = state;
        this.messenger = messenger;
        this.random = random;
    }

    /**
     * 处理抽卡请求
     */
    public void handle(MsgPacket msg) {
        try {
            Message.C2S_GachaMsg req = msg.unmarshal(
                    Message.C2S_GachaMsg.getDefaultInstance());
            int times = req.getTimes();

            // 只支持单抽和十连
            if (times != 1 && times != 10) {
                sendGachaResult(Collections.emptyList(), state.getGold(),
                        Message.ERRORCODE.ERR_RoomState);
                return;
            }

            // 计算费用
            long cost = times == 1 ? COST_SINGLE : COST_TEN;

            if (!state.spendGold(cost)) {
                sendGachaResult(Collections.emptyList(), state.getGold(),
                        Message.ERRORCODE.ERR_RoomState);
                return;
            }

            // 抽卡逻辑
            List<Item> items = new ArrayList<>();
            for (int i = 0; i < times; i++) {
                Item item = rollGacha();
                state.addItem(item);
                items.add(item);
            }

            log.info("[GrowthActor-{}] gacha times={}, cost={}", playerId, times, cost);

            sendGachaResult(items, state.getGold(), Message.ERRORCODE.ERR_Ok);

        } catch (Exception e) {
            log.error("[GrowthActor-{}] handleGacha error: {}", playerId, e.getMessage());
        }
    }

    /**
     * 抽卡随机逻辑
     *
     * @return 抽到的物品
     */
    private Item rollGacha() {
        int rarity = random.nextInt(100);
        for (RarityTier tier : TIERS) {
            if (rarity < tier.upperExclusive()) {
                return Item.newBuilder()
                        .setItemID(tier.itemId())
                        .setName(tier.name())
                        .setCount(1)
                        .setAttackBonus(tier.attackBonus())
                        .build();
            }
        }
        throw new IllegalStateException("gacha tier table must cover [0,100)");
    }

    private void sendGachaResult(List<Item> items, long remainGold,
                                  Message.ERRORCODE errorCode) {
        Message.S2C_GachaMsg response = Message.S2C_GachaMsg.newBuilder()
                .addAllItems(items)
                .setRemainGold(remainGold)
                .setErrorCode(errorCode)
                .build();
        messenger.send(MsgPacket.newInstance((byte) Message.ID.MSG_Gacha_VALUE, response));
    }
}
