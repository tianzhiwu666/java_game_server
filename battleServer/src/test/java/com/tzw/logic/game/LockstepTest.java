package com.tzw.logic.game;

import com.tzw.pb.Message.InputData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Lockstep 帧缓冲单元测试
 */
class LockstepTest {

    private Lockstep lockstep;

    @BeforeEach
    void setUp() {
        lockstep = new Lockstep();
    }

    @Test
    void initialFrameCountIsZero() {
        assertEquals(0, lockstep.getFrameCount());
    }

    @Test
    void tickIncrementsFrameCount() {
        long count = lockstep.tick();
        assertEquals(1, count);
        assertEquals(1, lockstep.getFrameCount());
    }

    @Test
    void pushCmdAddsInputToCurrentFrame() {
        InputData cmd = InputData.newBuilder()
                .setId(100L).setSid(1).setX(10).setY(20).setRoomseatid(1)
                .build();

        assertTrue(lockstep.pushCmd(cmd));
        assertNotNull(lockstep.getFrame(0));
        assertEquals(1, lockstep.getFrame(0).cmds.size());
    }

    @Test
    void pushCmdRejectsDuplicatePlayerInSameFrame() {
        InputData cmd1 = InputData.newBuilder()
                .setId(100L).setSid(1).setX(10).setY(20).setRoomseatid(1)
                .build();
        InputData cmd2 = InputData.newBuilder()
                .setId(100L).setSid(2).setX(30).setY(40).setRoomseatid(1)
                .build();

        assertTrue(lockstep.pushCmd(cmd1));
        assertFalse(lockstep.pushCmd(cmd2), "同一玩家同一帧内重复输入应被拒绝");
    }

    @Test
    void pushCmdAllowsDifferentPlayersInSameFrame() {
        InputData cmd1 = InputData.newBuilder()
                .setId(100L).setSid(1).setX(10).setY(20).setRoomseatid(1)
                .build();
        InputData cmd2 = InputData.newBuilder()
                .setId(200L).setSid(2).setX(30).setY(40).setRoomseatid(2)
                .build();

        assertTrue(lockstep.pushCmd(cmd1));
        assertTrue(lockstep.pushCmd(cmd2));
        assertEquals(2, lockstep.getFrame(0).cmds.size());
    }

    @Test
    void pushCmdAllowsSamePlayerInDifferentFrames() {
        // 第 0 帧：玩家 100 输入
        InputData cmd1 = InputData.newBuilder()
                .setId(100L).setSid(1).setX(10).setY(20).setRoomseatid(1)
                .build();
        assertTrue(lockstep.pushCmd(cmd1));
        assertNotNull(lockstep.getFrame(0));

        lockstep.tick(); // 进入第 1 帧

        // 第 1 帧：同一玩家再次输入（应允许）
        InputData cmd2 = InputData.newBuilder()
                .setId(100L).setSid(2).setX(30).setY(40).setRoomseatid(1)
                .build();
        assertTrue(lockstep.pushCmd(cmd2));
        assertNotNull(lockstep.getFrame(1));
    }

    @Test
    void getFrameReturnsNullForEmptyFrame() {
        assertNull(lockstep.getFrame(0));
    }

    @Test
    void resetClearsAllState() {
        lockstep.pushCmd(InputData.newBuilder().setId(100L).build());
        lockstep.tick();
        lockstep.reset();

        assertEquals(0, lockstep.getFrameCount());
        assertNull(lockstep.getFrame(0));
    }

    @Test
    void getRangeFrames() {
        // 第 0 帧：玩家 100
        lockstep.pushCmd(InputData.newBuilder().setId(100L).build());
        lockstep.tick();
        // 第 1 帧：无输入
        lockstep.tick();
        // 第 2 帧：玩家 200
        lockstep.pushCmd(InputData.newBuilder().setId(200L).build());

        var frames = lockstep.getRangeFrames(0, 2);
        assertEquals(2, frames.size()); // 只有第 0 帧和第 2 帧有输入
        assertEquals(0, frames.get(0).index);
        assertEquals(2, frames.get(1).index);
    }
}
