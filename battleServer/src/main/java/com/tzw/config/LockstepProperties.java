package com.tzw.config;


/**
 * 帧同步服务器配置类 —— 绑定 application.yml 中 {@code lockstep.*} 配置项。
 *
 * <p>镜像 Go 参考实现中的关键常量，集中管理以便移植时对照。
 * 所有配置都有默认值，与 Go 原版保持一致。
 *
 * <h3>配置结构</h3>
 * <pre>
 * lockstep:
 *   udp:
 *     address: ":10086"           # UDP 监听地址
 *   room:
 *     frequency: 30               # 房间时钟频率 (Hz)
 *     timeoutMinutes: 5           # 房间超时时间 (分钟)
 *   game:
 *     maxReadyTimeSeconds: 20     # 准备阶段最长时间 (秒)
 *     maxGameFrames: 5500         # 每局最大帧数
 *     broadcastOffsetFrames: 3    # 每隔多少帧广播一次
 *     maxFrameDataPerMsg: 60      # 每个消息包最多包含多少帧数据
 *     badNetworkThresholdSeconds: 2  # 心跳超时阈值 (秒)
 * </pre>
 *
 * <h3>Go 对照表</h3>
 * <table border="1">
 *   <tr><th>Java 配置项</th><th>Go 常量</th><th>默认值</th></tr>
 *   <tr><td>room.frequency</td><td>Frequency</td><td>30</td></tr>
 *   <tr><td>room.timeoutMinutes</td><td>TimeoutTime</td><td>5</td></tr>
 *   <tr><td>game.maxReadyTimeSeconds</td><td>MaxReadyTime</td><td>20</td></tr>
 *   <tr><td>game.maxGameFrames</td><td>MaxGameFrame</td><td>30*60*3+100</td></tr>
 *   <tr><td>game.broadcastOffsetFrames</td><td>BroadcastOffsetFrames</td><td>3</td></tr>
 *   <tr><td>game.maxFrameDataPerMsg</td><td>kMaxFrameDataPerMsg</td><td>60</td></tr>
 *   <tr><td>game.badNetworkThresholdSeconds</td><td>kBadNetworkThreshold</td><td>2</td></tr>
 * </table>
 *
 * <h3>线程安全</h3>
 * <p>配置对象在应用启动时创建，之后只被读取，不会被修改，因此是线程安全的。
 */
public class LockstepProperties {

    private Udp udp = new Udp();
    private Room room = new Room();
    private Game game = new Game();

    public Udp getUdp() { return udp; }
    public void setUdp(Udp udp) { this.udp = udp; }
    public Room getRoom() { return room; }
    public void setRoom(Room room) { this.room = room; }
    public Game getGame() { return game; }
    public void setGame(Game game) { this.game = game; }

    /**
     * KCP UDP 监听配置。
     *
     * <p>配置 UDP 服务器监听的地址和端口。
     */
    public static class Udp {
        /**
         * UDP 监听地址。
         *
         * <p>支持格式：
         * <ul>
         *   <li>{@code ":10086"} — 监听所有接口</li>
         *   <li>{@code "0.0.0.0:10086"} — 监听所有接口</li>
         *   <li>{@code "192.168.1.1:10086"} — 监听指定接口</li>
         * </ul>
         */
        private String address = ":10086";
        public String getAddress() { return address; }
        public void setAddress(String address) { this.address = address; }
    }

    /**
     * 房间时钟与超时配置。
     *
     * <p>控制房间事件循环的时钟频率和总超时时间。
     */
    public static class Room {
        /**
         * 房间时钟频率 (Hz)。
         *
         * <p>镜像 Go 的 {@code Frequency = 30}。
         * 30Hz 意味着每 33ms 推进一帧，是帧同步游戏的常见选择。
         * 频率越高，同步精度越高，但 CPU 和网络开销也越大。
         */
        private int frequency = 30;

        /**
         * 房间超时时间 (分钟)。
         *
         * <p>镜像 Go 的 {@code TimeoutTime = 5min}。
         * 这是房间的总运行时间上限，防止房间无限运行。
         * 5 分钟对大多数帧同步游戏足够。
         */
        private int timeoutMinutes = 5;

        public int getFrequency() { return frequency; }
        public void setFrequency(int frequency) { this.frequency = frequency; }
        public int getTimeoutMinutes() { return timeoutMinutes; }
        public void setTimeoutMinutes(int timeoutMinutes) { this.timeoutMinutes = timeoutMinutes; }

        /**
         * 每帧毫秒数 = 1000 / frequency。
         *
         * <p>用于计算 tick 间隔。例如 frequency=30 时，tickMs ≈ 33ms。
         *
         * @return 每帧毫秒数
         */
        public long getTickMs() { return 1000L / frequency; }

        /**
         * 超时毫秒数。
         *
         * <p>用于计算房间截止时间。
         *
         * @return 超时毫秒数
         */
        public long getTimeoutMs() { return timeoutMinutes * 60L * 1000L; }
    }

    /**
     * 游戏逻辑配置。
     *
     * <p>控制游戏流程的各项参数。
     */
    public static class Game {
        /**
         * 准备阶段最长时间 (秒)。
         *
         * <p>镜像 Go 的 {@code MaxReadyTime = 20}。
         * 如果超过此时间仍有玩家未准备，但有在线玩家，则强制开始游戏。
         * 如果没有任何在线玩家，则直接结束游戏。
         */
        private int maxReadyTimeSeconds = 20;

        /**
         * 每局最大帧数。
         *
         * <p>镜像 Go 的 {@code MaxGameFrame = 30*60*3 + 100 = 5500}。
         * 约 3 分钟的游戏时长。超过此帧数则强制结束游戏。
         * 这是游戏的安全网，防止无限运行。
         */
        private long maxGameFrames = 30L * 60 * 3 + 100;

        /**
         * 每隔多少帧广播一次。
         *
         * <p>镜像 Go 的 {@code BroadcastOffsetFrames = 3}。
         * 帧广播的节流机制：当帧差超过此值时触发广播。
         * 较小的值降低延迟但增加带宽，较大的值节省带宽但增加延迟。
         */
        private int broadcastOffsetFrames = 3;

        /**
         * 每个消息包最多包含多少帧数据。
         *
         * <p>镜像 Go 的 {@code kMaxFrameDataPerMsg = 60}。
         * 限制单个 UDP 数据包的大小，避免 IP 分片。
         * 在断线重连时尤为重要：需要批量发送大量历史帧。
         */
        private int maxFrameDataPerMsg = 60;

        /**
         * 心跳超时阈值 (秒)。
         *
         * <p>镜像 Go 的 {@code kBadNetworkThreshold = 2}。
         * 如果玩家超过此时间未发送心跳，视为网络不佳，暂停发送帧数据。
         * 这是网络拥塞控制机制，避免向网络不佳的玩家发送大量数据。
         */
        private int badNetworkThresholdSeconds = 2;

        public int getMaxReadyTimeSeconds() { return maxReadyTimeSeconds; }
        public void setMaxReadyTimeSeconds(int v) { this.maxReadyTimeSeconds = v; }
        public long getMaxGameFrames() { return maxGameFrames; }
        public void setMaxGameFrames(long v) { this.maxGameFrames = v; }
        public int getBroadcastOffsetFrames() { return broadcastOffsetFrames; }
        public void setBroadcastOffsetFrames(int v) { this.broadcastOffsetFrames = v; }
        public int getMaxFrameDataPerMsg() { return maxFrameDataPerMsg; }
        public void setMaxFrameDataPerMsg(int v) { this.maxFrameDataPerMsg = v; }
        public int getBadNetworkThresholdSeconds() { return badNetworkThresholdSeconds; }
        public void setBadNetworkThresholdSeconds(int v) { this.badNetworkThresholdSeconds = v; }
    }
}
