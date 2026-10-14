package com.tzw.logic.game;

import com.tzw.pb.Message;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * FrameBatchPacker 单元测试 —— 锁定帧打包语义：
 * 跳过无输入的空帧（区间末帧除外）；累计 maxPerMsg 帧或到达末帧时发出一包。
 * 语义与重构前 Game.doReconnect / Game.broadcastFrameData 的循环一致。
 */
class FrameBatchPackerTest {

    /** 推进 frameCount 帧；在 withInput 列出的帧号上放一条玩家 101 的输入 */
    private static Lockstep lockstep(int frameCount, int... withInput) {
        Lockstep ls = new Lockstep();
        for (int f = 0; f < frameCount; f++) {
            for (int w : withInput) {
                if (w == f) {
                    ls.pushCmd(Message.InputData.newBuilder().setId(101L).build());
                }
            }
            ls.tick();
        }
        return ls;
    }

    @Test
    void emptyRangeSendsNothing() {
        List<Message.S2C_FrameMsg> out = new ArrayList<>();
        int sent = FrameBatchPacker.pack(lockstep(0), 0, 0, 60, out::add);
        assertEquals(0, sent);
        assertTrue(out.isEmpty());
    }

    @Test
    void skipsEmptyFramesExceptLast() {
        // 帧 0..2，仅帧 1 有输入 → 发 1 包 {帧1(有输入), 帧2(空,末帧)}
        Lockstep ls = lockstep(3, 1);
        List<Message.S2C_FrameMsg> out = new ArrayList<>();
        int sent = FrameBatchPacker.pack(ls, 0, 3, 60, out::add);

        assertEquals(1, sent);
        Message.S2C_FrameMsg msg = out.get(0);
        assertEquals(2, msg.getFramesCount());
        assertEquals(1, msg.getFrames(0).getFrameID());
        assertEquals(1, msg.getFrames(0).getInputCount());
        assertEquals(2, msg.getFrames(1).getFrameID());
        assertEquals(0, msg.getFrames(1).getInputCount());
    }

    @Test
    void batchesByMaxPerMsg() {
        // 5 帧全有输入，maxPerMsg=2 → 3 包（2/2/1）
        Lockstep ls = lockstep(5, 0, 1, 2, 3, 4);
        List<Message.S2C_FrameMsg> out = new ArrayList<>();
        int sent = FrameBatchPacker.pack(ls, 0, 5, 2, out::add);

        assertEquals(3, sent);
        assertArrayEquals(new int[]{2, 2, 1},
                out.stream().mapToInt(Message.S2C_FrameMsg::getFramesCount).toArray());
    }

    @Test
    void startsFromGivenCursor() {
        // 增量广播语义：从 from（含）开始，from 之前的帧不出现
        Lockstep ls = lockstep(5, 0, 1, 2, 3, 4);
        List<Message.S2C_FrameMsg> out = new ArrayList<>();
        FrameBatchPacker.pack(ls, 3, 5, 60, out::add);

        assertEquals(1, out.size());
        assertEquals(3, out.get(0).getFrames(0).getFrameID());
        assertEquals(4, out.get(0).getFrames(1).getFrameID());
    }
}
