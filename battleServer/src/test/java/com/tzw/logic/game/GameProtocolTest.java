package com.tzw.logic.game;

import com.tzw.config.LockstepProperties;
import com.tzw.network.SendQueueProbe;
import com.tzw.packet.MsgPacket;
import com.tzw.pb.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Game 协议特征化测试 —— 锁定帧同步对外行为（客户端收到的 msgID + protobuf 字节）。
 *
 * <p>行为基线见规格 {@code docs/superpowers/specs/2026-10-14-refactor-design.md} §3.2：
 * 重构（提取 FrameBatchPacker）前后本测试都必须通过，且不得为迁就实现而修改断言。
 *
 * <p>注意：{@code broadcastOffsetFrames} 保持默认 3，帧广播靠 dirty 标志或帧差触发，
 * 与线上行为一致。
 */
class GameProtocolTest {

    private static final long ROOM_ID = 1L;
    private static final long P1 = 101L;
    private static final long P2 = 102L;

    private LockstepProperties props;
    private RecordingListener listener;
    private Game game;
    private SendQueueProbe probe1;
    private SendQueueProbe probe2;

    /** 记录 GameListener 回调事件 */
    private static final class RecordingListener implements GameListener {
        final List<String> events = new ArrayList<>();

        @Override public void onJoinGame(long roomId, long playerId) { events.add("join:" + playerId); }
        @Override public void onGameStart(long roomId) { events.add("start"); }
        @Override public void onLeaveGame(long roomId, long playerId) { events.add("leave:" + playerId); }
        @Override public void onGameOver(long roomId) { events.add("over"); }
    }

    @BeforeEach
    void joinTwoPlayers() {
        props = new LockstepProperties();
        listener = new RecordingListener();
        game = new Game(ROOM_ID, List.of(P1, P2), 999, listener, props);
        probe1 = SendQueueProbe.create();
        probe2 = SendQueueProbe.create();

        assertTrue(game.joinGame(P1, probe1.conn()));
        assertTrue(game.joinGame(P2, probe2.conn()));
        // joinGame 回包 ConnectMsg（ERR_Ok）
        assertEquals(List.of(Message.ID.MSG_Connect_VALUE), probe1.drainIds());
        assertEquals(List.of(Message.ID.MSG_Connect_VALUE), probe2.drainIds());
    }

    private static MsgPacket pkt(int msgId, Object body) {
        return MsgPacket.newInstance((byte) msgId, body);
    }

    private static int framesOf(SendQueueProbe.Captured captured) throws Exception {
        assertEquals(Message.ID.MSG_Frame_VALUE, captured.msgID());
        return Message.S2C_FrameMsg.parseFrom(captured.body()).getFramesCount();
    }

    /** 双人 Ready → tick → 进入 GAMING，排干 Start 广播 */
    private void startGame() {
        game.processMsg(P1, pkt(Message.ID.MSG_Ready_VALUE, null));
        game.processMsg(P2, pkt(Message.ID.MSG_Ready_VALUE, null));
        assertEquals(List.of(Message.ID.MSG_Ready_VALUE), probe1.drainIds());
        assertEquals(List.of(Message.ID.MSG_Ready_VALUE), probe2.drainIds());

        game.tick(System.currentTimeMillis() / 1000);
        assertEquals(Game.State.GAMING, game.getState());
        assertEquals(List.of(Message.ID.MSG_Start_VALUE), probe1.drainIds());
        assertEquals(List.of(Message.ID.MSG_Start_VALUE), probe2.drainIds());
    }

    // ==================== 用例 ====================

    @Test
    void joinRoomRepliesSeatAndSeed() throws Exception {
        game.processMsg(P1, pkt(Message.ID.MSG_JoinRoom_VALUE, null));

        List<SendQueueProbe.Captured> got = probe1.drain();
        assertEquals(1, got.size());
        assertEquals(Message.ID.MSG_JoinRoom_VALUE, got.get(0).msgID());

        Message.S2C_JoinRoomMsg m = Message.S2C_JoinRoomMsg.parseFrom(got.get(0).body());
        assertEquals(1, m.getRoomseatid());
        assertEquals(999, m.getRandomSeed());
        assertEquals(List.of(P2), m.getOthersList());
        assertEquals(List.of(0), m.getProsList());

        // JoinRoom 只回发送者
        assertTrue(probe2.drain().isEmpty());
    }

    @Test
    void progressExcludesSenderAndIgnoredAfterGaming() throws Exception {
        game.processMsg(P1, pkt(Message.ID.MSG_Progress_VALUE,
                Message.C2S_ProgressMsg.newBuilder().setPro(50).build()));

        assertTrue(probe1.drain().isEmpty());
        List<SendQueueProbe.Captured> got2 = probe2.drain();
        assertEquals(1, got2.size());
        Message.S2C_ProgressMsg m = Message.S2C_ProgressMsg.parseFrom(got2.get(0).body());
        assertEquals(P1, m.getId());
        assertEquals(50, m.getPro());

        // GAMING 后 Progress 被忽略
        startGame();
        game.processMsg(P1, pkt(Message.ID.MSG_Progress_VALUE,
                Message.C2S_ProgressMsg.newBuilder().setPro(80).build()));
        assertTrue(probe2.drain().isEmpty());
    }

    @Test
    void readyBothStartsGame() throws Exception {
        game.processMsg(P1, pkt(Message.ID.MSG_Ready_VALUE, null));
        game.processMsg(P2, pkt(Message.ID.MSG_Ready_VALUE, null));
        probe1.drain();
        probe2.drain();

        game.tick(System.currentTimeMillis() / 1000);

        assertEquals(Game.State.GAMING, game.getState());
        assertTrue(listener.events.contains("start"));

        List<SendQueueProbe.Captured> got = probe1.drain();
        assertEquals(1, got.size());
        assertEquals(Message.ID.MSG_Start_VALUE, got.get(0).msgID());
        assertTrue(Message.S2C_StartMsg.parseFrom(got.get(0).body()).getTimeStamp() > 0);
        assertEquals(List.of(Message.ID.MSG_Start_VALUE), probe2.drainIds());
    }

    @Test
    void inputDirtyBroadcastsFrame() throws Exception {
        startGame();

        game.processMsg(P1, pkt(Message.ID.MSG_Input_VALUE,
                Message.C2S_InputMsg.newBuilder().setSid(11).setX(10).setY(20).build()));
        game.tick(System.currentTimeMillis() / 1000);

        // dirty 强制广播：帧 0（含 P1 输入）发给双方
        for (SendQueueProbe p : List.of(probe1, probe2)) {
            List<SendQueueProbe.Captured> got = p.drain();
            assertEquals(1, got.size());
            assertEquals(Message.ID.MSG_Frame_VALUE, got.get(0).msgID());

            Message.S2C_FrameMsg frame = Message.S2C_FrameMsg.parseFrom(got.get(0).body());
            assertEquals(1, frame.getFramesCount());
            Message.FrameData fd = frame.getFrames(0);
            assertEquals(0, fd.getFrameID());
            assertEquals(1, fd.getInputCount());

            Message.InputData in = fd.getInput(0);
            assertEquals(P1, in.getId());
            assertEquals(11, in.getSid());
            assertEquals(10, in.getX());
            assertEquals(20, in.getY());
            assertEquals(1, in.getRoomseatid());
        }
    }

    @Test
    void reconnectBatchesHistoryFrames() throws Exception {
        props.getGame().setMaxFrameDataPerMsg(2);
        startGame();

        for (int f = 0; f < 5; f++) {
            game.processMsg(P1, pkt(Message.ID.MSG_Input_VALUE,
                    Message.C2S_InputMsg.newBuilder().setSid(f).setX(f).setY(f).build()));
            game.tick(System.currentTimeMillis() / 1000);
        }
        probe1.drain();
        probe2.drain();

        // P2 在 GAMING 发 Ready → 断线重连：Start + 5 帧按 2/2/1 分包
        game.processMsg(P2, pkt(Message.ID.MSG_Ready_VALUE, null));

        List<SendQueueProbe.Captured> got = probe2.drain();
        assertEquals(4, got.size());
        assertEquals(Message.ID.MSG_Start_VALUE, got.get(0).msgID());
        assertArrayEquals(new int[]{2, 2, 1},
                new int[]{framesOf(got.get(1)), framesOf(got.get(2)), framesOf(got.get(3))});

        // P2 无 MSG_Ready 回执（已 ready，doReady 幂等跳过）
        assertTrue(probe1.drain().isEmpty());
    }

    @Test
    void gameOverAfterAllResults() {
        startGame();

        game.processMsg(P1, pkt(Message.ID.MSG_Result_VALUE,
                Message.C2S_ResultMsg.newBuilder().setWinnerID(P1).build()));
        game.processMsg(P2, pkt(Message.ID.MSG_Result_VALUE,
                Message.C2S_ResultMsg.newBuilder().setWinnerID(P1).build()));
        assertEquals(List.of(Message.ID.MSG_Result_VALUE), probe1.drainIds());
        assertEquals(List.of(Message.ID.MSG_Result_VALUE), probe2.drainIds());
        assertEquals(Map.of(P1, P1, P2, P1), game.getResult());

        long now = System.currentTimeMillis() / 1000;
        assertTrue(game.tick(now));   // GAMING → OVER
        assertEquals(Game.State.OVER, game.getState());
        assertTrue(game.tick(now));   // OVER → doGameOver → STOP
        assertEquals(Game.State.STOP, game.getState());
        assertTrue(listener.events.contains("over"));
        assertFalse(game.tick(now));  // STOP：房间退出事件循环
    }

    @Test
    void joinRejectedAfterOver() throws Exception {
        startGame();
        game.processMsg(P1, pkt(Message.ID.MSG_Result_VALUE,
                Message.C2S_ResultMsg.newBuilder().setWinnerID(P1).build()));
        game.processMsg(P2, pkt(Message.ID.MSG_Result_VALUE,
                Message.C2S_ResultMsg.newBuilder().setWinnerID(P1).build()));
        long now = System.currentTimeMillis() / 1000;
        game.tick(now);
        game.tick(now);
        assertEquals(Game.State.STOP, game.getState());
        probe1.drain();
        probe2.drain();

        // 结束后加入 → ERR_RoomState 的 ConnectMsg 发给玩家的【原连接】（Game.joinGame 行为），
        // 新传入的连接不接管、收不到任何包
        SendQueueProbe late = SendQueueProbe.create();
        assertTrue(game.joinGame(P1, late.conn()));

        List<SendQueueProbe.Captured> got = probe1.drain();
        assertEquals(1, got.size());
        assertEquals(Message.ID.MSG_Connect_VALUE, got.get(0).msgID());
        assertEquals(Message.ERRORCODE.ERR_RoomState,
                Message.S2C_ConnectMsg.parseFrom(got.get(0).body()).getErrorCode());
        assertTrue(late.drain().isEmpty());
    }
}
