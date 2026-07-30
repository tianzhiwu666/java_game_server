package com.tzw.network;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ============================================================
 *  TCP 网络服务器 —— 养成系统的长连接传输层
 * ============================================================
 *
 * <p>与 {@link Server}（KCP 战斗服务器）并行运行，负责养成系统的 TCP 长连接。
 * 使用 Netty 的 {@link NioServerSocketChannel} 提供可靠的 TCP 传输。
 *
 * <h3>与 KCP Server 的对比</h3>
 * <table border="1">
 *   <tr><th>维度</th><th>TcpServer（本类）</th><th>Server（KCP）</th></tr>
 *   <tr><td>协议</td><td>TCP 长连接</td><td>KCP 可靠 UDP</td></tr>
 *   <tr><td>Channel</td><td>NioServerSocketChannel</td><td>UkcpServerChannel</td></tr>
 *   <tr><td>连接模型</td><td>有连接，Channel 即连接</td><td>有连接，UkcpChannel</td></tr>
 *   <tr><td>帧解码</td><td>LengthFieldBasedFrameDecoder</td><td>KCP 自带消息边界</td></tr>
 *   <tr><td>应用场景</td><td>养成系统（低频、可靠）</td><td>战斗系统（高频、低延迟）</td></tr>
 * </table>
 *
 * <h3>帧解码</h3>
 * <p>使用 {@link LengthFieldBasedFrameDecoder} 解决 TCP 粘包问题。
 * 与 {@link com.tzw.packet.MsgProtocol} 配合：
 * <pre>
 * TCP 字节流 → LengthFieldBasedFrameDecoder → 完整帧 → MsgProtocol.decode() → Packet
 * </pre>
 *
 * <h3>线程模型</h3>
 * <ul>
 *   <li>bossGroup：接受 TCP 连接（1 个线程）</li>
 *   <li>workerGroup：处理 I/O（默认 CPU 核心数 × 2）</li>
 *   <li>net-send 线程：每 10ms 刷新所有 Conn 的发送队列</li>
 * </ul>
 */
public class TcpServer {

    private static final Logger log = LoggerFactory.getLogger(TcpServer.class);

    private final ServerConfig config;
    private final ConnCallback callback;
    private final Protocol protocol;
    /** 连接映射：TCP Channel → Conn */
    private final Map<Channel, Conn> connMap = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel channel;
    private Thread sendThread;

    public TcpServer(ServerConfig config, ConnCallback callback, Protocol protocol) {
        this.config = config;
        this.callback = callback;
        this.protocol = protocol;
    }

    /**
     * 启动 TCP 服务器
     *
     * @param address 监听地址
     */
    public void start(InetSocketAddress address) {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("server already running");
        }

        // bossGroup 接受连接，workerGroup 处理 I/O
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        // 帧解码器：解决 TCP 粘包
                        // MsgPacket 格式：[2B dataLen][1B msgID][protobuf body]
                        // LengthFieldBasedFrameDecoder 参数：
                        //   maxFrameLength = 1024（与 MsgPacket.MAX_PACKET_LEN 一致）
                        //   lengthFieldOffset = 0（长度字段在开头）
                        //   lengthFieldLength = 2（2 字节长度）
                        //   lengthAdjustment = 1（长度字段后有 1 字节 msgID 不算在长度内）
                        //   initialBytesToStrip = 0（不剥离头部，MsgProtocol 需要读取）
                        p.addLast(new LengthFieldBasedFrameDecoder(
                                1024, 0, 2, 1, 0));
                        p.addLast(new TcpServerHandler());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true);  // 禁用 Nagle，降低延迟

        try {
            channel = bootstrap.bind(address).sync().channel();
            log.info("[TcpServer] started on {}", address);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("tcp server bind interrupted", e);
        }

        // 启动发送线程：周期性刷新所有连接的发送队列
        sendThread = new Thread(this::sendLoop, "net-send-tcp");
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
            if (bossGroup != null) {
                bossGroup.shutdownGracefully();
            }
            if (workerGroup != null) {
                workerGroup.shutdownGracefully();
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
     * @param ch TCP 通道
     * @return Conn 对象，如果创建失败返回 null
     */
    Conn getOrCreateConn(SocketChannel ch) {
        return connMap.compute(ch, (channel, existing) -> {
            if (existing != null && !existing.isClosed()) {
                return existing;
            }
            // 创建 Conn，写入回调通过 TCP Channel 发送
            Conn conn = new Conn(ch.remoteAddress(), data -> {
                ch.writeAndFlush(data);
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
     * TCP 数据包处理器
     *
     * <p>Netty 回调，在 workerGroup EventLoop 线程中执行。
     * LengthFieldBasedFrameDecoder 已经处理了粘包，这里直接拿到完整帧。
     */
    private class TcpServerHandler extends ChannelInboundHandlerAdapter {

        /**
         * 新 TCP 连接建立时调用
         */
        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            SocketChannel ch = (SocketChannel) ctx.channel();
            log.debug("[TcpServer] channelActive: {}", ch.remoteAddress());
            getOrCreateConn(ch);
        }

        /**
         * 收到数据时调用
         *
         * <p>LengthFieldBasedFrameDecoder 保证每个 channelRead 都是一帧完整数据。
         */
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            SocketChannel ch = (SocketChannel) ctx.channel();
            ByteBuf buf = (ByteBuf) msg;

            Conn conn = getOrCreateConn(ch);
            if (conn == null || conn.isClosed()) {
                buf.release();
                return;
            }

            try {
                // 拆包：一帧可能包含多个消息
                Packet parsed;
                while ((parsed = protocol.decode(buf)) != null) {
                    boolean keepAlive = conn.getCallback().onMessage(conn, parsed);
                    if (!keepAlive) {
                        conn.close();
                        connMap.remove(ch);
                        return;
                    }
                }
            } finally {
                // 释放 ByteBuf
                if (buf.refCnt() > 0) {
                    buf.release();
                }
            }

            // 收到包后立即尝试刷新发送（低延迟响应）
            conn.flushSendQueue();
        }

        /**
         * 连接断开时调用
         */
        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            SocketChannel ch = (SocketChannel) ctx.channel();
            log.debug("[TcpServer] channelInactive: {}", ch.remoteAddress());
            Conn conn = connMap.remove(ch);
            if (conn != null) {
                conn.close();
            }
        }

        /**
         * 异常时调用
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("[TcpServer] exception on channel: {}", ctx.channel().remoteAddress(), cause);
            ctx.close();
        }
    }
}
