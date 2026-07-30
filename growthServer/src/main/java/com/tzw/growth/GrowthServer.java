package com.tzw.growth;

import com.tzw.mq.EventBus;
import com.tzw.mq.InMemoryMqAdapter;
import com.tzw.mq.MqProducer;
import com.tzw.mq.TypedMqConsumer;
import com.tzw.network.ServerConfig;
import com.tzw.server.GrowthRouter;
import com.tzw.logic.growth.GrowthSessionManager;
import com.tzw.network.TcpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * 养成逻辑服启动入口。
 *
 * <p>负责玩家养成逻辑：
 * <ul>
 *   <li>TCP 长连接管理（默认端口 10087）</li>
 *   <li>玩家数据（等级/经验/金币/背包）</li>
 *   <li>业务逻辑（升级/装备/抽卡）</li>
 *   <li>匹配请求（发布 match.create 事件）</li>
 * </ul>
 *
 * <p>无 Spring Boot，手动装配依赖。
 */
public final class GrowthServer {

    private static final Logger log = LoggerFactory.getLogger(GrowthServer.class);

    /** 默认 TCP 端口（与 target1 保持一致） */
    private static final int DEFAULT_PORT = 10087;

    private TcpServer tcpServer;
    private GrowthSessionManager sessionManager;

    private GrowthServer() {}

    /**
     * 启动养成服务
     *
     * @param port TCP 监听端口
     */
    public void start(int port) {
        log.info("[growthServer] starting on port {} (Java {})", port, Runtime.version());

        // 1. 创建 MQ 组件（进程内实现，可替换为 Redis）
        InMemoryMqAdapter mqAdapter = new InMemoryMqAdapter();
        MqProducer mqProducer = mqAdapter;
        TypedMqConsumer mqConsumer = mqAdapter;
        EventBus eventBus = new EventBus();

        // 2. 创建网络配置
        ServerConfig config = new ServerConfig();

        // 3. 创建会话管理器
        sessionManager = new GrowthSessionManager(mqProducer);

        // 4. 创建路由器（第一层消息分发：鉴权 + 心跳）
        GrowthRouter router = new GrowthRouter(sessionManager, eventBus, mqConsumer, mqProducer);

        // 5. 创建并启动 TCP 服务器
        tcpServer = new TcpServer(config, router, new com.tzw.packet.MsgProtocol());
        tcpServer.start(new InetSocketAddress(port));

        log.info("[growthServer] started successfully on port {}", port);
    }

    /**
     * 停止养成服务
     */
    public void stop() {
        log.info("[growthServer] stopping...");
        if (sessionManager != null) {
            sessionManager.stop();
        }
        if (tcpServer != null) {
            tcpServer.stop();
        }
        log.info("[growthServer] stopped");
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

        GrowthServer server = new GrowthServer();
        server.start(port);

        // 注册关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "growth-shutdown"));
    }
}
