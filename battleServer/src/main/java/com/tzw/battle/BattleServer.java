package com.tzw.battle;

import com.tzw.config.LockstepProperties;
import com.tzw.logic.RoomManager;
import com.tzw.logic.match.MatchService;
import com.tzw.mq.MqProducer;
import com.tzw.mq.RedisStreamMqAdapter;
import com.tzw.mq.TypedMqConsumer;
import com.tzw.network.Server;
import com.tzw.network.ServerConfig;
import com.tzw.packet.MsgProtocol;
import com.tzw.server.LockStepServer;
import com.tzw.server.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 战斗服启动入口。
 *
 * <p>负责帧同步对战逻辑：
 * <ul>
 *   <li>KCP 可靠 UDP 连接（默认端口 10086）</li>
 *   <li>房间管理（每房间单线程 Actor）</li>
 *   <li>帧同步（30Hz tick）</li>
 *   <li>战斗结果计算</li>
 * </ul>
 *
 * <p>无 Spring Boot，手动装配依赖。
 */
public final class BattleServer {

    private static final Logger log = LoggerFactory.getLogger(BattleServer.class);

    /** 默认 KCP/UDP 端口（与 target1 保持一致） */
    private static final int DEFAULT_PORT = 10086;

    private Server udpServer;
    private RoomManager roomManager;
    private MatchService matchService;
    private RedisStreamMqAdapter mqAdapter;
    private int port;

    private BattleServer() {}

    /**
     * 启动战斗服务
     *
     * @param port KCP/UDP 监听端口
     */
    public void start(int port) {
        this.port = port;
        log.info("[battleServer] starting on port {} (Java {})", port, Runtime.version());

        // 1. 创建配置（默认值与 target1 一致）
        LockstepProperties properties = new LockstepProperties();

        // 2. 创建 MQ 组件（真实 MQ：Redis Stream，跨进程通信）
        RedisStreamMqAdapter mqAdapter = RedisStreamMqAdapter.fromEnv();
        log.info("[battleServer] Redis MQ url: {}", mqAdapter.redisUri());
        MqProducer mqProducer = mqAdapter;
        TypedMqConsumer mqConsumer = mqAdapter;
        this.mqAdapter = mqAdapter;

        // 3. 创建房间管理器
        roomManager = new RoomManager(properties);

        // 4. 创建顶层服务器（持有 RoomManager 和连接计数）
        AtomicLong totalConn = new AtomicLong(0);
        LockStepServer lockStepServer = new LockStepServer(roomManager, totalConn);

        // 5. 创建路由器（第一层消息分发：Connect 握手 + 心跳）
        Router router = new Router(lockStepServer);

        // 6. 创建匹配服务（订阅 match.create → 建房间 → 回报 match.ready / match.result）
        matchService = new MatchService(roomManager, mqProducer, mqConsumer, "127.0.0.1", port);

        // 7. 创建并启动 KCP/UDP 服务器
        ServerConfig udpConfig = new ServerConfig();
        udpConfig.setPacketSendChanLimit(1024);
        udpConfig.setPacketReceiveChanLimit(1024);
        udpServer = new Server(udpConfig, router, new MsgProtocol());
        udpServer.start(new InetSocketAddress(port));

        log.info("[battleServer] started successfully on port {}", port);
    }

    /**
     * 停止战斗服务
     */
    public void stop() {
        log.info("[battleServer] stopping...");
        if (roomManager != null) {
            roomManager.stop();
        }
        if (matchService != null) {
            matchService.close();
        }
        if (mqAdapter != null) {
            mqAdapter.close();
        }
        if (udpServer != null) {
            udpServer.stop();
        }
        log.info("[battleServer] stopped");
    }

    public static void main(String[] args) {
        int port = DEFAULT_PORT;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port: " + args[0] + ", using default " + DEFAULT_PORT);
            }
        }

        BattleServer server = new BattleServer();
        server.start(port);

        // 注册关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "battle-shutdown"));
    }
}
