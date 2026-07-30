package com.tzw.network;

import java.time.Duration;

/**
 * 网络服务器配置类。
 *
 * <p>镜像 Go 参考实现中的 {@code network.Config} 结构体。
 * 用于配置底层 Netty UDP 服务器的各项参数，控制连接行为和缓冲区大小。
 *
 * <h3>在帧同步系统中的角色</h3>
 * <p>该配置对象在 {@link LockStepServer#start()} 中被创建并传入 {@link Server}，
 * 是网络层的初始化参数。它不参与游戏逻辑，只影响网络 I/O 行为。
 *
 * <h3>设计决策</h3>
 * <ul>
 *   <li><b>发送/接收通道上限</b>：用于背压控制。当游戏逻辑处理较慢时，
 *       通道满后 {@code asyncWrite} 会返回 false，触发连接关闭，避免内存无限增长。</li>
 *   <li><b>读写超时</b>：Netty 的 {@code IdleStateHandler} 参数，
 *       用于检测死连接。在帧同步场景中，客户端通常以 30Hz 频率发送心跳，
 *       因此 5 秒超时足以容忍网络抖动。</li>
 * </ul>
 *
 * <h3>线程安全</h3>
 * <p>配置对象在服务器启动时创建，之后只被读取，不会被修改，因此是线程安全的。
 */
public class ServerConfig {

    /**
     * 发送通道缓冲区上限。
     *
     * <p>每个连接的异步写入队列最大容量。当队列满时，{@link Conn#asyncWrite(Packet)} 返回 false，
     * 上层可据此判断网络拥塞并关闭连接。
     *
     * <p>默认值 1024 是一个经验值：在 30Hz 帧率下，相当于约 34 秒的缓冲，
     * 足以应对短暂的网络波动，同时避免内存占用过大。
     */
    private int packetSendChanLimit = 1024;

    /**
     * 接收通道缓冲区上限。
     *
     * <p>每个连接的入站消息队列最大容量。Netty 的 {@code channelRead} 将数据包放入此队列，
     * 由房间的单线程事件循环消费。
     *
     * <p>默认值 1024 与发送通道对称，确保双向流量均衡处理。
     */
    private int packetReceiveChanLimit = 1024;

    /**
     * 连接读超时。
     *
     * <p>如果在此时间内未收到任何数据，Netty 触发 {@code READER_IDLE} 事件。
     * 在帧同步系统中，客户端以固定频率发送心跳，因此读超时可用于检测断线。
     *
     * <p>默认 5 秒，是帧间隔（33ms）的约 150 倍，足够容忍网络抖动。
     */
    private Duration connReadTimeout = Duration.ofSeconds(5);

    /**
     * 连接写超时。
     *
     * <p>如果在此时间内未写入任何数据，Netty 触发 {@code WRITER_IDLE} 事件。
     * 可用于主动发送心跳包保持连接活跃。
     *
     * <p>默认 5 秒，与读超时对称。
     */
    private Duration connWriteTimeout = Duration.ofSeconds(5);

    public int getPacketSendChanLimit() { return packetSendChanLimit; }
    public void setPacketSendChanLimit(int v) { this.packetSendChanLimit = v; }
    public int getPacketReceiveChanLimit() { return packetReceiveChanLimit; }
    public void setPacketReceiveChanLimit(int v) { this.packetReceiveChanLimit = v; }
    public Duration getConnReadTimeout() { return connReadTimeout; }
    public void setConnReadTimeout(Duration v) { this.connReadTimeout = v; }
    public Duration getConnWriteTimeout() { return connWriteTimeout; }
    public void setConnWriteTimeout(Duration v) { this.connWriteTimeout = v; }
}
