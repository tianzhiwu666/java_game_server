package com.tzw.network;

import io.netty.buffer.ByteBuf;

/**
 * 默认协议拆包器：4 字节大端序长度前缀 + 正文。
 *
 * <p>镜像 Go 参考实现中的 {@code network.DefaultProtocol.ReadPacket}。
 * 这是 Netty 的帧解码器，负责将字节流切分为完整的数据包。
 *
 * <h3>拆包逻辑</h3>
 * <p>解码过程分为三步：
 * <ol>
 *   <li><b>检查长度前缀</b>：至少需要 {@link DefaultPacket#HEADER_SIZE} (4) 字节才能读取长度。
 *       如果不足，返回 null 等待更多数据。</li>
 *   <li><b>读取长度并校验</b>：使用 {@link ByteBuf#markReaderIndex()} 标记当前位置，
 *       读取 4 字节大端序整数作为正文长度。如果超过 {@link DefaultPacket#MAX_BODY_SIZE}，
 *       抛出异常（可能是恶意数据包或协议错误）。</li>
 *   <li><b>检查正文完整性</b>：如果可读字节数不足，调用 {@link ByteBuf#resetReaderIndex()}
 *       回退到标记位置，返回 null 等待更多数据；否则读取正文并构造 {@link DefaultPacket}。</li>
 * </ol>
 *
 * <h3>为什么使用 mark/reset</h3>
 * <p>TCP 是流协议，数据可能分片到达。当长度前缀已到达但正文未完整到达时，
 * 必须先回退读取位置，否则下次解码会丢失数据。
 * mark/reset 机制确保不完整的数据包不会破坏字节流。
 *
 * <h3>在帧同步系统中的角色</h3>
 * <p>该类是协议栈的最底层，被 {@link Server} 使用。
 * 上层 {@link com.tzw.packet.MsgProtocol} 在 {@link DefaultPacket} 的基础上
 * 进一步解析消息 ID 和 protobuf 正文。
 *
 * <h3>线程安全</h3>
 * <p>无状态对象，线程安全。每个 {@link ByteBuf} 由单个 Netty 线程操作。
 */
public class DefaultProtocol implements Protocol {

    /**
     * 从字节缓冲区解码一个数据包。
     *
     * <p>如果数据不完整（长度前缀或正文未到达），返回 null。
     * 如果正文长度超过限制，抛出 {@link IllegalArgumentException}。
     *
     * @param buf Netty 字节缓冲区
     * @return 解码成功的数据包，或 null（数据不完整）
     * @throws IllegalArgumentException 正文长度超过 {@link DefaultPacket#MAX_BODY_SIZE}
     */
    @Override
    public Packet decode(ByteBuf buf) {
        // 至少需要 4 字节读取长度前缀，否则等待更多数据
        if (buf.readableBytes() < DefaultPacket.HEADER_SIZE) {
            return null;
        }

        // 标记当前位置，以便在正文不完整时回退
        buf.markReaderIndex();
        int length = buf.readInt(); // 读取 4 字节大端序长度

        // 长度校验：防止恶意数据包导致内存溢出
        if (length > DefaultPacket.MAX_BODY_SIZE) {
            throw new IllegalArgumentException("packet size " + length + " exceeds limit " + DefaultPacket.MAX_BODY_SIZE);
        }

        // 正文未完整到达，回退读取位置等待下次解码
        if (buf.readableBytes() < length) {
            buf.resetReaderIndex();
            return null;
        }

        // 正文完整，读取并构造数据包
        byte[] body = new byte[length];
        buf.readBytes(body);
        return DefaultPacket.newInstance(body);
    }
}
