package com.tzw.packet;

import com.google.protobuf.Message;
import com.tzw.network.Packet;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * ============================================================
 *  消息数据包 — 帧同步服务器的核心协议格式
 * ============================================================
 *
 * 【数据包格式】（服务器↔客户端通用）
 * <pre>
 * ┌───────────────┬───────────────┬──────────────────────────┐
 * │ 数据长度(2B)  │  消息ID(1B)   │      protobuf 正文        │
 * │  uint16大端   │    uint8      │      (dataLen 字节)       │
 * └───────────────┴───────────────┴──────────────────────────┘
 * </pre>
 *
 * 【消息 ID 对应表】（proto 中定义）
 * <pre>
 *  ID  │ 消息类型          │ 方向    │ 说明
 * ─────┼───────────────────┼─────────┼─────────────────────
 *   1  │ MSG_Connect       │ C→S     │ 连接握手（第一个消息）
 *   2  │ MSG_Heartbeat     │ 双向    │ 心跳保活
 *  10  │ MSG_JoinRoom      │ C→S     │ 请求加入房间
 *  20  │ MSG_Progress      │ C→S     │ 加载进度 0~100
 *  30  │ MSG_Ready         │ C→S     │ 准备就绪
 *  40  │ MSG_Start         │ S→C     │ 游戏开始（含时间戳）
 *  50  │ MSG_Frame         │ S→C     │ 帧数据（核心！）
 *  60  │ MSG_Input         │ C→S     │ 玩家输入（核心！）
 *  70  │ MSG_Result        │ C→S     │ 战斗结果
 * 100  │ MSG_Close         │ S→C     │ 房间关闭，客户端强制退出
 * </pre>
 *
 * 【帧同步的核心消息】
 * - MSG_Input：客户端发送的玩家操作（如"第 N 帧按下方向键左"）
 * - MSG_Frame：服务器广播的帧数据（如"第 N 帧：玩家A左移，玩家B跳跃"）
 *
 * 【为什么用 protobuf？】
 * - 跨语言：Go 客户端和 Java 服务器可以互通
 * - 向后兼容：新增字段不影响旧版本
 * - 体积小：二进制编码，比 JSON 小很多
 *
 * 【镜像 Go】
 * Go 的 {@code pb_packet.Packet}：id uint8 + data []byte
 */
public class MsgPacket implements Packet {

    /** 数据长度字段字节数（uint16） */
    public static final int DATA_LEN_SIZE = 2;
    /** 消息 ID 字段字节数（uint8） */
    public static final int MSG_ID_SIZE = 1;
    /** 最小包长 = 2 + 1 */
    public static final int MIN_PACKET_LEN = DATA_LEN_SIZE + MSG_ID_SIZE;
    /** 最大包长（限制单包大小，防止内存溢出） */
    public static final int MAX_PACKET_LEN = 1024;

    private final byte id;
    private final byte[] data;

    private MsgPacket(byte id, byte[] data) {
        this.id = id;
        this.data = data;
    }

    public byte getMessageID() {
        return id;
    }

    public byte[] getData() {
        return data;
    }

    /**
     * 反序列化为 protobuf 消息
     *
     * <p>使用原型实例的 Parser 解析字节数据。
     * 例如：{@code msg.unmarshal(C2S_InputMsg.getDefaultInstance())}
     *
     * @param prototype 原型实例（用于获取 Parser）
     */
    public <T extends Message> T unmarshal(T prototype) throws com.google.protobuf.InvalidProtocolBufferException {
        @SuppressWarnings("unchecked")
        T result = (T) prototype.getParserForType().parseFrom(data);
        return result;
    }

    @Override
    public byte[] serialize() {
        int dataLen = data != null ? data.length : 0;
        ByteBuffer buf = ByteBuffer.allocate(MIN_PACKET_LEN + dataLen).order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) dataLen);
        buf.put(id);
        if (data != null) {
            buf.put(data);
        }
        return buf.array();
    }

    /**
     * 构造一个消息包
     *
     * @param id 消息 ID（对应 proto 中的 ID 枚举值）
     * @param msg protobuf 消息、byte[] 或 null
     * @return 数据包
     */
    public static MsgPacket newInstance(byte id, Object msg) {
        if (msg == null) {
            return new MsgPacket(id, new byte[0]);
        }
        if (msg instanceof byte[]) {
            return new MsgPacket(id, (byte[]) msg);
        }
        if (msg instanceof Message) {
            return new MsgPacket(id, ((Message) msg).toByteArray());
        }
        throw new IllegalArgumentException("unsupported message type: " + msg.getClass());
    }
}
