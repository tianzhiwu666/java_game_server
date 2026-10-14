package com.tzw.logic.game;

import com.tzw.pb.Message;

import java.util.function.Consumer;

/**
 * 帧批量打包器 —— {@link Game#doReconnect} 与 {@link Game#broadcastFrameData}
 * 共用的打包语义（重构前提取，行为与两处原循环逐字节一致）。
 *
 * <p>打包规则：
 * <ul>
 *   <li>跳过无输入的空帧；区间最后一帧（to-1）即使是空帧也发送
 *       —— 保证客户端总能推进到最新帧号</li>
 *   <li>累计 {@code maxPerMsg} 帧或到达最后一帧时发出一包，避免单包过大</li>
 * </ul>
 *
 * <p>纯函数式：不持有状态，输入帧存储器与发送回调，由调用方决定游标推进。
 */
final class FrameBatchPacker {

    private FrameBatchPacker() {
    }

    /**
     * 从 from（含）到 to（不含）逐帧打包并通过 sender 发出。
     *
     * @param lockstep  帧缓冲区（房间单线程内访问）
     * @param from      起始帧，通常为玩家的 sendFrameCount 游标
     * @param to        结束帧（不含），通常为当前帧总数
     * @param maxPerMsg 单包最大帧数
     * @param sender    每条打包完成的 S2C_FrameMsg 的发送回调
     * @return 实际发出的包数
     */
    static int pack(Lockstep lockstep, long from, long to, int maxPerMsg,
                    Consumer<Message.S2C_FrameMsg> sender) {
        int sent = 0;
        int count = 0;
        Message.S2C_FrameMsg.Builder msg = Message.S2C_FrameMsg.newBuilder();

        for (long i = from; i < to; i++) {
            Lockstep.FrameData frameData = lockstep.getFrame(i);
            // 跳过无输入的帧（除非是最后一帧）
            if (frameData == null && i != (to - 1)) {
                continue;
            }

            Message.FrameData.Builder fd = Message.FrameData.newBuilder()
                    .setFrameID((int) i);
            if (frameData != null) {
                fd.addAllInput(frameData.cmds);
            }
            msg.addFrames(fd.build());
            count++;

            // 每 maxPerMsg 帧或最后一帧发送一次
            if (i == (to - 1) || count >= maxPerMsg) {
                sender.accept(msg.build());
                sent++;
                count = 0;
                msg = Message.S2C_FrameMsg.newBuilder();
            }
        }
        return sent;
    }
}
