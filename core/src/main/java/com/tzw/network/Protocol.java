package com.tzw.network;

import io.netty.buffer.ByteBuf;

/**
 * ============================================================
 *  协议拆包接口 — 解决 TCP/UDP 流式传输的"粘包"问题
 * ============================================================
 *
 * 【什么是粘包？】
 * 网络传输是流式的，一个 UDP 数据包可能包含多个消息，也可能只包含消息的一部分。
 * 协议拆包器的作用是从字节流中正确地切分出一个个完整的消息包。
 *
 * 【工作原理】
 * 1. 读取头部，获取消息长度
 * 2. 检查缓冲区是否有足够的字节
 * 3. 如果足够，切分出一条消息返回
 * 4. 如果不足，返回 null，等待更多数据
 *
 * 【两种实现】
 * - {@link DefaultProtocol}：4 字节大端序长度前缀（TCP 备用）
 * - {@link com.tzw.packet.MsgProtocol}：2 字节长度 + 1 字节消息 ID（实际使用）
 *
 * 【镜像 Go】
 * Go 的 {@code network.Protocol}：{@code ReadPacket(io.Reader) (Packet, error)}
 * Java 端使用 Netty 的 {@link ByteBuf} 替代 {@code io.Reader}。
 */
public interface Protocol {

    /**
     * 从字节缓冲区解析出一个完整的数据包
     *
     * <p>如果缓冲区数据不足以构成一个完整包，返回 null（不消费字节）。
     * 调用者应在有更多数据时再次调用。
     *
     * @param buf 输入缓冲区
     * @return 解析出的数据包，如果数据不足则返回 null
     */
    Packet decode(ByteBuf buf);
}
