package com.tzw.network;

/**
 * ============================================================
 *  数据包接口 — 网络层最基础的抽象
 * ============================================================
 *
 * 【作用】
 * 所有通过网络传输的数据都必须实现此接口。
 * {@link #serialize()} 将数据包编码为字节数组，写入网络。
 *
 * 【两种实现】
 * - {@link DefaultPacket}：4 字节大端序长度前缀 + 正文（TCP 备用协议）
 * - {@link com.tzw.packet.MsgPacket}：2 字节长度 + 1 字节消息 ID + protobuf 正文（实际使用的协议）
 *
 * 【镜像 Go】
 * Go 的 {@code network.Packet} 接口：{@code Serialize() []byte}
 */
public interface Packet {

    /**
     * 序列化为字节数组，用于写入网络
     *
     * @return 序列化后的字节数组
     */
    byte[] serialize();
}
