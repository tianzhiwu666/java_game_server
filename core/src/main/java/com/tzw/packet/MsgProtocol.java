package com.tzw.packet;

import com.tzw.network.Packet;
import com.tzw.network.Protocol;
import io.netty.buffer.ByteBuf;

/**
 * 消息协议拆包器
 *
 * <p>镜像 Go 的 {@code pb_packet.MsgProtocol.ReadPacket}
 *
 * <p>解析格式：[2 字节数据长度][1 字节消息 ID][protobuf 正文]
 */
public class MsgProtocol implements Protocol {

    @Override
    public Packet decode(ByteBuf buf) {
        // 至少需要 3 字节（2 字节长度 + 1 字节 ID）
        if (buf.readableBytes() < MsgPacket.MIN_PACKET_LEN) {
            return null;
        }

        buf.markReaderIndex();

        // 读取 2 字节数据长度（大端序）
        int dataLen = buf.readShort() & 0xFFFF;

        if (dataLen > MsgPacket.MAX_PACKET_LEN) {
            throw new IllegalArgumentException("data length " + dataLen + " exceeds max " + MsgPacket.MAX_PACKET_LEN);
        }

        // 读取 1 字节消息 ID
        byte msgId = buf.readByte();

        // 检查数据是否完整
        if (buf.readableBytes() < dataLen) {
            buf.resetReaderIndex();
            return null;
        }

        // 读取数据
        byte[] data = new byte[dataLen];
        buf.readBytes(data);

        return MsgPacket.newInstance(msgId, data);
    }
}
