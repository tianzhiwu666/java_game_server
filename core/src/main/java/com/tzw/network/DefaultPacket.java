package com.tzw.network;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * 默认数据包：4 字节大端序长度前缀 + 正文。
 *
 * <p>镜像 Go 参考实现中的 {@code network.DefaultPacket}。
 * 这是传输层的基本数据单元，定义了 TCP/UDP 流中的帧边界格式。
 *
 * <h3>数据包格式</h3>
 * <pre>
 * +------------------+------------------------------------+
 * | 4 字节长度前缀    | 正文（protobuf 消息）               |
 * | (大端序 uint32)   | (长度 = 前缀值)                     |
 * +------------------+------------------------------------+
 * </pre>
 *
 * <ul>
 *   <li><b>HEADER_SIZE = 4</b>：长度前缀固定 4 字节，使用 {@link ByteOrder#BIG_ENDIAN}（网络字节序）。</li>
 *   <li><b>MAX_BODY_SIZE = 1024</b>：正文最大 1024 字节。帧同步场景下，
 *       一条输入消息通常只有几十个字节，1024 上限足够容纳批量帧数据。</li>
 * </ul>
 *
 * <h3>设计决策</h3>
 * <p>采用长度前缀而非分隔符，是因为 protobuf 消息本身可能包含任意字节，
 * 无法使用固定分隔符。长度前缀是最简单可靠的帧定界方式。
 *
 * <h3>与帧同步的关系</h3>
 * <p>该类是传输层协议，不感知帧同步语义。{@link com.tzw.packet.MsgProtocol}
 * 在 {@link DefaultPacket} 的基础上进一步解析消息 ID 和 protobuf 正文，
 * 构成完整的协议栈：
 * <pre>
 * 传输层：DefaultProtocol → DefaultPacket（长度前缀 + 正文）
 * 应用层：MsgProtocol → MsgPacket（消息 ID + protobuf）
 * </pre>
 *
 * <h3>线程安全</h3>
 * <p>数据包是不可变对象（buff 数组为 final），因此是线程安全的。
 * 但注意 {@link #getBody()} 返回的是新数组，修改它不会影响原包。
 */
public class DefaultPacket implements Packet {

    /** 长度前缀字节数（4 字节大端序 uint32） */
    public static final int HEADER_SIZE = 4;

    /** 最大正文长度（1024 字节），超过此值视为恶意数据包 */
    public static final int MAX_BODY_SIZE = 1024;

    /** 完整数据：前 4 字节为长度前缀，其后为正文 */
    private final byte[] buff;

    private DefaultPacket(byte[] buff) {
        this.buff = buff;
    }

    /**
     * 获取正文（去掉 4 字节长度前缀后的纯 protobuf 数据）。
     *
     * <p>返回的是新数组，修改返回值不会影响原数据包。
     *
     * @return 正文字节数组
     */
    public byte[] getBody() {
        byte[] body = new byte[buff.length - HEADER_SIZE];
        System.arraycopy(buff, HEADER_SIZE, body, 0, body.length);
        return body;
    }

    /**
     * 序列化：返回完整的带长度前缀的字节数组，可直接写入网络。
     *
     * @return 完整数据包字节数组
     */
    @Override
    public byte[] serialize() {
        return buff;
    }

    /**
     * 构造一个带长度前缀的数据包。
     *
     * <p>将正文长度写入前 4 字节（大端序），然后追加正文内容。
     * 这是 {@link DefaultProtocol#decode} 的逆操作。
     *
     * @param body 正文字节数组
     * @return 封装好的数据包
     */
    public static DefaultPacket newInstance(byte[] body) {
        byte[] buff = new byte[HEADER_SIZE + body.length];
        ByteBuffer.wrap(buff).order(ByteOrder.BIG_ENDIAN).putInt(body.length);
        System.arraycopy(body, 0, buff, HEADER_SIZE, body.length);
        return new DefaultPacket(buff);
    }
}
