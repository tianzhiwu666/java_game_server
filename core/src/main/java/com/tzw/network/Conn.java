package com.tzw.network;

import java.net.SocketAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * ============================================================
 *  网络连接封装 — 线程安全的异步写入队列
 * ============================================================
 *
 * 【核心设计】
 * Conn 是网络层的"连接"抽象。它包装了远端地址和一个写入回调，
 * 提供非阻塞的异步写入能力。
 *
 * 【为什么需要异步写入？】
 * 在帧同步服务器中，广播帧数据时需要向多个客户端发送。
 * 如果同步写入（等待每个客户端发送完成），一个慢客户端会拖慢整个房间。
 * 异步写入解决了这个问题：数据先入队，由专门的发送线程统一刷新。
 *
 * 【线程模型】
 * - Netty 线程：调用 asyncWrite() → 数据入队（非阻塞）
 * - 发送线程：周期性调用 flushSendQueue() → 数据出队并写入网络
 * - 房间线程：不直接操作 Conn，通过回调接收数据
 *
 * 【发送队列】
 * 使用 {@link LinkedBlockingQueue} 作为缓冲区，容量由 {@link ServerConfig#getPacketSendChanLimit()} 控制。
 * 队列满时 asyncWrite() 返回 false，调用者可选择关闭连接。
 *
 * 【extraData 附加数据】
 * Conn 可以携带一个 Object 类型的附加数据。在本项目中：
 * - Router 在校验 Connect 后，将 playerID 存入 extraData
 * - Room 在收到消息时，从 extraData 取出 playerID 用于消息分发
 *
 * 【镜像 Go】
 * Go 的 {@code network.Conn}：包装 net.Conn，有 packetSendChan/packetReceiveChan 两个 channel
 * Java 端简化为只有发送队列（接收由 Netty 回调直接处理）。
 */
public class Conn {

    private final SocketAddress remoteAddress;
    /** 实际写入网络的回调（由 Server/TcpServer 注入，封装数据发送） */
    private final Consumer<byte[]> writer;
    /** 附加数据：存放 playerID（Long 类型） */
    private volatile Object extraData;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    /** 发送队列：Netty 线程生产，发送线程消费 */
    private final LinkedBlockingQueue<Packet> sendQueue;
    private volatile ConnCallback callback;

    public Conn(SocketAddress remoteAddress, Consumer<byte[]> writer, ConnCallback callback, ServerConfig config) {
        this.remoteAddress = remoteAddress;
        this.writer = writer;
        this.callback = callback;
        this.sendQueue = new LinkedBlockingQueue<>(config.getPacketSendChanLimit());
    }

    public Object getExtraData() {
        return extraData;
    }

    public void putExtraData(Object data) {
        this.extraData = data;
    }

    public SocketAddress remoteAddress() {
        return remoteAddress;
    }

    public boolean isClosed() {
        return closed.get();
    }

    /**
     * 动态切换回调。
     * 初始回调是 Router，Connect 成功后切换为 Room。
     * 注意：只能在 onConnect 中调用（此时还未启动并发读写）。
     */
    public void setCallback(ConnCallback callback) {
        this.callback = callback;
    }

    public ConnCallback getCallback() {
        return callback;
    }

    /**
     * 异步写入数据包（非阻塞）
     *
     * <p>数据入队后由发送线程统一刷新到网络。
     * 如果队列已满，返回 false（调用者应关闭连接）。
     *
     * @param packet 要发送的数据包
     * @return true 成功入队，false 队列已满或连接已关闭
     */
    public boolean asyncWrite(Packet packet) {
        if (closed.get()) {
            return false;
        }
        return sendQueue.offer(packet);
    }

    /**
     * 异步写入数据包（带超时）
     *
     * @param packet 要发送的数据包
     * @param timeout 超时时间
     * @param unit 时间单位
     * @return true 成功，false 超时/队列满/已关闭
     */
    public boolean asyncWrite(Packet packet, long timeout, TimeUnit unit) {
        if (closed.get()) {
            return false;
        }
        try {
            return sendQueue.offer(packet, timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * 关闭连接
     *
     * <p>设置关闭标志，触发 onClose 回调。
     * 使用 compareAndSet 确保只关闭一次。
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (callback != null) {
                callback.onClose(this);
            }
        }
    }

    /**
     * 内部：刷新发送队列，将缓冲的包写入网络
     *
     * <p>由 Server 的发送线程周期性调用（每 10ms）。
     * 遍历队列，逐个取出并调用 writer 写入网络。
     */
    void flushSendQueue() {
        Packet packet;
        while ((packet = sendQueue.poll()) != null) {
            if (closed.get()) {
                return;
            }
            writer.accept(packet.serialize());
        }
    }

    @Override
    public String toString() {
        return "Conn{" + remoteAddress + "}";
    }
}
