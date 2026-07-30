package com.tzw.logic.growth;

import com.tzw.pb.Message;
import com.tzw.pb.Message.Item;
import com.tzw.pb.Message.S2C_PlayerDataMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 玩家养成状态 —— 单个玩家的所有养成数据。
 *
 * <p>包含等级、经验、金币、背包、装备等。
 * 此类只在 {@link GrowthActor} 的单线程中访问，无需额外同步。
 *
 * <h3>脏标记机制</h3>
 * <p>当状态变更时设置 {@link #dirty} = true，{@link com.tzw.dao.PersistScheduler}
 * 定期扫描脏数据并入库。
 */
public class PlayerGrowthState {

    /** 玩家 ID */
    private final long playerId;

    /** 等级 */
    private int level;

    /** 经验值 */
    private long exp;

    /** 金币 */
    private long gold;

    /** 基础攻击力（不含装备加成） */
    private int baseAttack;

    /** 背包：物品 ID → 物品 */
    private final ConcurrentHashMap<Long, Item> inventory = new ConcurrentHashMap<>();

    /** 当前装备的物品 ID（0 表示未装备） */
    private long equippedItemId;

    /** 脏标记：数据变更后需要入库 */
    private volatile boolean dirty;

    public PlayerGrowthState(long playerId) {
        this.playerId = playerId;
        this.level = 1;
        this.exp = 0;
        this.gold = 1000;  // 初始金币
        this.baseAttack = 10;
        this.equippedItemId = 0;
        this.dirty = false;
    }

    // ==================== 业务方法 ====================

    /**
     * 增加经验值
     *
     * @param amount 经验值数量
     */
    public void addExp(long amount) {
        this.exp += amount;
        this.dirty = true;
    }

    /**
     * 增加金币
     *
     * @param amount 金币数量
     */
    public void addGold(long amount) {
        this.gold += amount;
        this.dirty = true;
    }

    /**
     * 消费金币
     *
     * @param cost 消费数量
     * @return true 足够并扣除，false 不足
     */
    public boolean spendGold(long cost) {
        if (this.gold < cost) {
            return false;
        }
        this.gold -= cost;
        this.dirty = true;
        return true;
    }

    /**
     * 升级（如果经验足够）
     *
     * @return true 升级成功
     */
    public boolean tryLevelUp() {
        long requiredExp = getRequiredExp(level);
        if (exp < requiredExp) {
            return false;
        }
        exp -= requiredExp;
        level++;
        baseAttack += 5;  // 每级 +5 攻击力
        dirty = true;
        return true;
    }

    /**
     * 获取指定等级所需经验
     *
     * @param level 等级
     * @return 所需经验
     */
    public static long getRequiredExp(int level) {
        return 100L * level * level;  // 100 * level^2
    }

    /**
     * 添加物品到背包
     *
     * @param item 物品
     */
    public void addItem(Item item) {
        inventory.compute(item.getItemID(), (id, existing) -> {
            if (existing == null) {
                return item;
            } else {
                return Item.newBuilder(existing)
                        .setCount(existing.getCount() + item.getCount())
                        .build();
            }
        });
        dirty = true;
    }

    /**
     * 从背包移除物品
     *
     * @param itemId 物品 ID
     * @param count 数量
     * @return true 移除成功
     */
    public boolean removeItem(long itemId, int count) {
        Item existing = inventory.get(itemId);
        if (existing == null || existing.getCount() < count) {
            return false;
        }
        if (existing.getCount() == count) {
            inventory.remove(itemId);
        } else {
            inventory.put(itemId, Item.newBuilder(existing)
                    .setCount(existing.getCount() - count)
                    .build());
        }
        dirty = true;
        return true;
    }

    /**
     * 检查是否拥有物品
     *
     * @param itemId 物品 ID
     * @param count 数量
     * @return true 拥有足够数量
     */
    public boolean hasItem(long itemId, int count) {
        Item item = inventory.get(itemId);
        return item != null && item.getCount() >= count;
    }

    /**
     * 装备物品
     *
     * @param itemId 物品 ID
     * @return 之前装备的物品 ID（0 表示无）
     */
    public long equipItem(long itemId) {
        if (!inventory.containsKey(itemId)) {
            return -1;  // 物品不存在
        }
        long previous = equippedItemId;
        equippedItemId = itemId;
        dirty = true;
        return previous;
    }

    /**
     * 卸下装备
     */
    public void unequipItem() {
        equippedItemId = 0;
        dirty = true;
    }

    /**
     * 计算总攻击力（基础 + 装备加成）
     *
     * @return 总攻击力
     */
    public int getTotalAttack() {
        int total = baseAttack;
        if (equippedItemId > 0) {
            Item equipped = inventory.get(equippedItemId);
            if (equipped != null) {
                total = total + equipped.getAttackBonus();
            }
        }
        return total;
    }

    // ==================== 序列化 ====================

    /**
     * 转换为 protobuf 消息
     *
     * @return S2C_PlayerDataMsg
     */
    public S2C_PlayerDataMsg.Builder toProto() {
        return S2C_PlayerDataMsg.newBuilder()
                .setPlayerID(playerId)
                .setLevel(level)
                .setExp(exp)
                .setGold(gold)
                .setAttack(getTotalAttack())
                .setCurEquipItemID((int) equippedItemId)
                .addAllItems(inventory.values());
    }

    // ==================== Getter/Setter ====================

    public long getPlayerId() { return playerId; }
    public int getLevel() { return level; }
    public void setLevel(int level) { this.level = level; this.dirty = true; }
    public long getExp() { return exp; }
    public void setExp(long exp) { this.exp = exp; this.dirty = true; }
    public long getGold() { return gold; }
    public void setGold(long gold) { this.gold = gold; this.dirty = true; }
    public int getBaseAttack() { return baseAttack; }
    public void setBaseAttack(int baseAttack) { this.baseAttack = baseAttack; this.dirty = true; }
    public long getEquippedItemId() { return equippedItemId; }
    public ConcurrentHashMap<Long, Item> getInventory() { return inventory; }
    public boolean isDirty() { return dirty; }
    public void setDirty(boolean dirty) { this.dirty = dirty; }
    public void clearDirty() { this.dirty = false; }
}
