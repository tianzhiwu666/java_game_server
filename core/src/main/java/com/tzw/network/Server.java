package com.tzw.network;

import io.jpower.kcp.netty.ChannelOptionHelper;
import io.jpower.kcp.netty.UkcpChannel;
import io.jpower.kcp.netty.UkcpChannelOption;
import io.jpower.kcp.netty.UkcpServerChannel;
import io.netty.bootstrap.UkcpServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ============================================================
 *  网络服务器 — KCP（可靠 UDP）数据包的分发中枢
 * ============================================================
 *
 * 【核心职责】
 * 1. 监听 UDP 端口，接收客户端数据包（通过 KCP 协议）
 * 2. 按 KCP Channel 映射到 Conn 对象（KCP 提供类似 TCP 的连接抽象）
 * 3. 拆包：一个 KCP 包可能包含多个消息
 * 4. 分发：通过 ConnCallback 将消息交给上层处理
 *
 * 【线程模型】
 * ┌─────────────────────────────────────────────────────────┐
 * │  Netty EventLoop 线程（接收 UDP 数据，KCP 解包）          │
 * │    ↓ channelActive() 创建 Conn                           │
 * │    ↓ channelRead() 拆包                                  │
 * │    ↓ getOrCreateConn() 获取/创建 Conn                     │
 * │    ↓ callback.onMessage() 分发到 Router/Room             │
 * │    ↓ conn.flushSendQueue() 立即刷新发送（低延迟响应）      │
 * ├─────────────────────────────────────────────────────────┤
 * │  发送线程 net-send（每 10ms 刷新所有连接的发送队列）         │
 * │    ↓ 遍历 connMap，调用 conn.flushSendQueue()             │
 * ├─────────────────────────────────────────────────────────┤
 * │  房间线程（消费 BlockingQueue 中的消息）                    │
 *    ↓ game.processMsg() 处理游戏逻辑                         │
 * └─────────────────────────────────────────────────────────┘
 *
 * 【KCP vs 原始 UDP】
 * - 原始 UDP：无连接，用地址标识客户端，无重传，无保序
 * - KCP：有连接（UkcpChannel），ARQ 重传，保序，拥塞控制
 * - KCP 在 UDP 之上提供可靠传输，API 类似 TCP Channel
 *
 * 【连接管理】
 * connMap 是 ConcurrentHashMap，key 为 UkcpChannel。
 * 每个客户端对应一个 Conn 对象。KCP 提供连接状态感知，
 * channelInactive 时自动清理连接。
 *
 * 【KCP 参数说明】
 * - nodelay: 启用 nodelay 模式（牺牲带宽换延迟）
 * - interval: 内部 tick 间隔（10ms，匹配 30Hz 帧率）
 * - fastResend: 快速重传阈值（收到 2 个重复 ACK 就重传）
 * - nocwnd: 禁用拥塞窗口（帧同步小包不需要拥塞控制）
 * - MTU: 最大传输单元（1024，匹配现有包大小限制）
 */
public class Server {

    private static final Logger log = LoggerFactory.getLogger(Server.class);

    private final ServerConfig config;
    private final ConnCallback callback;
    private final Protocol protocol;
    /** 连接映射：KCP Channel → Conn */
    private final Map<UkcpChannel, Conn> connMap = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private EventLoopGroup eventLoopGroup;
    private UkcpServerChannel channel;
    private Thread sendThread;

    public Server(ServerConfig config, ConnCallback callback, Protocol protocol) {
        this.config = config;
        this.callback = callback;
        this.protocol = protocol;
    }

    /**
     * 启动 KCP 服务器
     *
     * @param address 监听地址
     */
    public void start(InetSocketAddress address) {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("server already running");
        }

        eventLoopGroup = new NioEventLoopGroup();

        // KCP: 使用 UkcpServerBootstrap 替代 Bootstrap
        UkcpServerBootstrap bootstrap = new UkcpServerBootstrap();
        bootstrap.group(eventLoopGroup)
                .channel(UkcpServerChannel.class)
                .childHandler(new ChannelInitializer<UkcpChannel>() {
                    @Override
                    protected void initChannel(UkcpChannel ch) {
                        ch.pipeline().addLast(new KcpServerHandler());
                    }
                });

        // KCP 参数配置：低延迟模式，匹配帧同步需求
        ChannelOptionHelper.nodelay(bootstrap, true, 10, 2, true)
                .childOption(UkcpChannelOption.UKCP_MTU, 1024);

        try {
            channel = (UkcpServerChannel) bootstrap.bind(address).sync().channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("server bind interrupted", e);
        }

        // 启动发送线程：周期性刷新所有连接的发送队列
        sendThread = new Thread(this::sendLoop, "net-send");
        sendThread.setDaemon(true);
        sendThread.start();
    }

    /**
     * 停止服务器
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            if (channel != null) {
                channel.close();
            }
            if (eventLoopGroup != null) {
                eventLoopGroup.shutdownGracefully();
            }
            if (sendThread != null) {
                sendThread.interrupt();
            }
            connMap.clear();
        }
    }

    /**
     * 获取或创建连接
     *
     * <p>如果该 Channel 已有活跃 Conn，直接返回；否则创建新 Conn 并触发 onConnect 回调。
     * 使用 compute 保证原子性。
     *
     * @param kcpChannel KCP 客户端通道
     * @return Conn 对象，如果创建失败返回 null
     */
    Conn getOrCreateConn(UkcpChannel kcpChannel) {
        return connMap.compute(kcpChannel, (ch, existing) -> {
            if (existing != null && !existing.isClosed()) {
                return existing;
            }
            // 创建 Conn，写入回调通过 KCP Channel 发送
            Conn conn = new Conn(ch.remoteAddress(), data -> {
                ByteBuf buf = Unpooled.wrappedBuffer(data);
                ch.writeAndFlush(buf);
            }, callback, config);
            if (!callback.onConnect(conn)) {
                conn.close();
                return null;
            }
            return conn;
        });
    }

    /**
     * 发送循环：周期性刷新所有连接的发送队列
     *
     * <p>每 10ms 遍历所有 Conn，将缓冲的数据包写入网络。
     * 这是"生产者-消费者"模式中的消费者。
     */
    private void sendLoop() {
        while (running.get()) {
            for (Conn conn : connMap.values()) {
                if (!conn.isClosed()) {
                    conn.flushSendQueue();
                }
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * KCP 数据包处理器
     *
     * <p>Netty 回调，在 EventLoop 线程中执行。
     * KCP 已经处理了重传和保序，这里直接拿到完整消息。
     * 拆包后逐个分发，如果 onMessage 返回 false 则关闭连接。
     */
    private class KcpServerHandler extends ChannelInboundHandlerAdapter {

        /**
         * 新客户端连接建立时调用
         *
         * <p>KCP 在 UDP 之上提供连接抽象，channelActive 表示新的 KCP 会话建立。
         * 此时创建 Conn 对象并触发 onConnect 回调。
         */
        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            UkcpChannel kcpChannel = (UkcpChannel) ctx.channel();
            getOrCreateConn(kcpChannel);
        }

        /**
         * 收到数据时调用
         *
         * <p>KCP 保证数据可靠有序，这里直接拆包处理。
         */
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            UkcpChannel kcpChannel = (UkcpChannel) ctx.channel();
            ByteBuf buf = (ByteBuf) msg;

            Conn conn = getOrCreateConn(kcpChannel);
            if (conn == null || conn.isClosed()) {
                buf.release();
                return;
            }

            try {
                // 拆包：一个 KCP 包可能包含多个消息
                Packet parsed;
                while ((parsed = protocol.decode(buf)) != null) {
                    boolean keepAlive = conn.getCallback().onMessage(conn, parsed);
                    if (!keepAlive) {
                        conn.close();
                        connMap.remove(kcpChannel);
                        return;
                    }
                }
            } finally {
                // 释放 ByteBuf（KCP 模式下需要手动释放）
                if (buf.refCnt() > 0) {
                    buf.release();
                }
            }

            // 收到包后立即尝试刷新发送（低延迟响应）
            conn.flushSendQueue();
        }

        /**
         * 连接断开时调用
         *
         * <p>KCP 会话结束时清理 Conn 对象。
         */
        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            UkcpChannel kcpChannel = (UkcpChannel) ctx.channel();
            Conn conn = connMap.remove(kcpChannel);
            if (conn != null) {
                conn.close();
            }
        }

        /**
         * 异常时调用
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("[server] exception on channel: {}", ctx.channel().remoteAddress(), cause);
            ctx.close();
        }
    }
}
