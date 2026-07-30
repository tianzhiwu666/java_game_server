package com.tzw.logic.game;

import com.tzw.pb.Message.InputData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 帧存储器 —— 确定性的回放日志。
 *
 * <p>镜像 Go 参考实现中的 {@code logic/game/lockstep.go}。
 * 这是帧同步的核心数据结构，存储所有玩家的输入历史。
 *
 * <h3>稀疏映射设计</h3>
 * <p>{@link #frames} 是 {@code Map<Long, FrameData>}，只存储有输入的帧。
 * 大多数帧可能没有输入（玩家未操作），使用稀疏映射可以节省内存。
 *
 * <h3>帧数据结构</h3>
 * <pre>
 * frameCount: 当前帧总数（单调递增）
 * frames: {
 *   0: FrameData{ index: 0, cmds: [InputData{player1}, InputData{player2}] }
 *   5: FrameData{ index: 5, cmds: [InputData{player1}] }
 *   // 帧 1-4 没有输入，不在 map 中
 * }
 * </pre>
 *
 * <h3>重复输入检测</h3>
 * <p>{@link #pushCmd} 会检查同一帧内是否已有该玩家的输入。
 * 如果有，返回 false 拒绝重复输入。这是防止客户端作弊的重要机制。
 *
 * <h3>断线重连支持</h3>
 * <p>{@link #getRangeFrames} 用于断线重连：服务器批量发送历史帧给重连的玩家，
 * 使其快速追赶到当前帧。
 *
 * <h3>线程安全</h3>
 * <p><b>此类只在房间单线程中访问，无需额外同步。</b>
 * 所有方法都由 {@link Game} 调用，而 {@link Game} 在房间单线程中运行。
 */
public class Lockstep {

    /**
     * 稀疏帧映射：帧索引 → 帧数据。
     *
     * <p>只存储有输入的帧，节省内存。使用 {@link HashMap} 因为帧索引不连续。
     */
    private final Map<Long, FrameData> frames = new HashMap<>();

    /**
     * 当前帧总数（单调递增）。
     *
     * <p>由 {@link #tick} 递增，表示游戏已经推进了多少帧。
     * 这是帧同步的"时钟"，所有客户端必须与此同步。
     */
    private long frameCount = 0;

    /**
     * 重置帧存储器。
     *
     * <p>在游戏开始时调用（{@link Game#doStart}），清空所有历史帧。
     * 重置后 {@link #frameCount} 归零，开始新的帧序列。
     */
    public void reset() {
        frames.clear();
        frameCount = 0;
    }

    /**
     * 获取当前帧总数。
     *
     * @return 当前帧计数
     */
    public long getFrameCount() {
        return frameCount;
    }

    /**
     * 将玩家输入追加到当前帧。
     *
     * <p>如果当前帧（{@link #frameCount}）还没有 {@link FrameData}，
     * 会自动创建。然后检查该玩家是否已在当前帧发送过输入，
     * 如果是则返回 false（拒绝重复输入）。
     *
     * <p><b>为什么拒绝重复输入？</b>客户端可能因网络重发或作弊而发送重复输入。
     * 拒绝重复输入保证每个玩家每帧最多一个输入，维护帧同步的确定性。
     *
     * @param cmd 输入数据（包含玩家 ID 和操作信息）
     * @return true 成功追加，false 同一玩家在同一帧内已发送过输入
     */
    public boolean pushCmd(InputData cmd) {
        long playerId = cmd.getId();
        FrameData frame = frames.computeIfAbsent(frameCount, FrameData::new);

        // 检查是否同一帧内已发送过（防止重复输入/作弊）
        for (InputData existing : frame.cmds) {
            if (existing.getId() == playerId) {
                return false;
            }
        }

        frame.cmds.add(cmd);
        return true;
    }

    /**
     * 推进到下一帧。
     *
     * <p>由 {@link Game#tickGaming} 在每帧调用。
     * 递增 {@link #frameCount}，表示时间推进一帧。
     *
     * @return 新的帧计数
     */
    public long tick() {
        frameCount++;
        return frameCount;
    }

    /**
     * 获取指定帧的数据。
     *
     * <p>如果该帧没有输入，返回 null（稀疏映射）。
     *
     * @param idx 帧索引
     * @return 帧数据，如果该帧无输入则返回 null
     */
    public FrameData getFrame(long idx) {
        return frames.get(idx);
    }

    /**
     * 获取指定范围内的帧数据。
     *
     * <p>用于断线重连：批量获取历史帧发送给重连的玩家。
     * 只返回有输入的帧（跳过 null）。
     *
     * @param from 起始帧（包含）
     * @param to 结束帧（包含）
     * @return 帧数据列表（只包含有输入的帧）
     */
    public List<FrameData> getRangeFrames(long from, long to) {
        List<FrameData> result = new ArrayList<>();
        for (long i = from; i <= to && i <= frameCount; i++) {
            FrameData frame = frames.get(i);
            if (frame != null) {
                result.add(frame);
            }
        }
        return result;
    }

    /**
     * 帧数据 —— 单帧内所有玩家的输入。
     *
     * <p>包含帧索引和该帧的所有输入命令列表。
     * 输入按到达顺序排列（在单线程中，顺序是确定的）。
     */
    public static class FrameData {
        /** 帧索引 */
        public final long index;

        /** 该帧的所有输入命令列表 */
        public final List<InputData> cmds = new ArrayList<>();

        FrameData(long index) {
            this.index = index;
        }
    }
}
