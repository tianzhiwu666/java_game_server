package com.tzw.network;

import com.tzw.packet.MsgPacket;
import com.tzw.packet.MsgProtocol;
import io.netty.buffer.Unpooled;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * 测试探针：创建可捕获发送字节的 {@link Conn}。
 *
 * <p>原理：{@link Conn#flushSendQueue()} 是 package-private，本类与之同包名
 * （位于测试源码目录，classpath 模式下合法，项目无 JPMS 模块声明）。
 * writer 回调收集 {@code packet.serialize()} 的字节，
 * {@link #drain()} 用 {@link MsgProtocol} 反解为消息列表。
 *
 * <p>同款拷贝见 {@code battleServer/src/test/java/com/tzw/network/SendQueueProbe.java}。
 */
public final class SendQueueProbe {

    /** 捕获到的一条消息：msgID（0~255 无符号）+ protobuf body */
    public record Captured(int msgID, byte[] body) {}

    private final List<byte[]> flushed = new ArrayList<>();
    private final Conn conn;

    private SendQueueProbe() {
        this.conn = new Conn(
                InetSocketAddress.createUnresolved("test", 0),
                flushed::add,
                new NoopCallback(),
                new ServerConfig());
    }

    public static SendQueueProbe create() {
        return new SendQueueProbe();
    }

    public Conn conn() {
        return conn;
    }

    /** 排空发送队列并反解为消息列表 */
    public synchronized List<Captured> drain() {
        conn.flushSendQueue();
        List<Captured> out = new ArrayList<>();
        MsgProtocol protocol = new MsgProtocol();
        for (byte[] bytes : flushed) {
            MsgPacket pkt = (MsgPacket) protocol.decode(Unpooled.wrappedBuffer(bytes));
            if (pkt != null) {
                out.add(new Captured(pkt.getMessageID() & 0xFF, pkt.getData()));
            }
        }
        flushed.clear();
        return out;
    }

    /** 排空并只返回 msgID 序列（顺序敏感断言用） */
    public synchronized List<Integer> drainIds() {
        List<Integer> ids = new ArrayList<>();
        for (Captured c : drain()) {
            ids.add(c.msgID());
        }
        return ids;
    }

    private static final class NoopCallback implements ConnCallback {
        @Override
        public boolean onConnect(Conn conn) {
            return true;
        }

        @Override
        public boolean onMessage(Conn conn, Packet packet) {
            return true;
        }

        @Override
        public void onClose(Conn conn) {
        }
    }
}
