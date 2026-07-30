# CLAUDE.md

本文件为 Claude Code (claude.ai/code) 提供本仓库的工作指南。

## 构建命令

**重要**：本项目使用 Java 25，但系统 `JAVA_HOME` 默认指向 JDK 21。构建时必须指定 JDK 25：

```bash
# 编译全模块
JAVA_HOME="C:/Program Files/Java/jdk-25" mvn compile

# 安装到本地仓库（运行测试前必须先 install）
JAVA_HOME="C:/Program Files/Java/jdk-25" mvn install -DskipTests

# 打包（产出可执行 fat jar）
JAVA_HOME="C:/Program Files/Java/jdk-25" mvn package

# 运行指定模块的测试
JAVA_HOME="C:/Program Files/Java/jdk-25" mvn test -pl battleServer

# 运行单个测试类
JAVA_HOME="C:/Program Files/Java/jdk-25" mvn test -pl battleServer -Dtest=LockstepTest
```

## 项目架构

### 模块结构（4 模块 Maven 工程）

```
com.tzw:game-server (父 POM, packaging=pom)
├── core          # 网络/协议/MQ接口/Protobuf（无业务逻辑）
├── dao           # 数据访问层（当前为空壳，Redis/MySQL 待实现）
├── growthServer  # 养成逻辑服（TCP :10087）
└── battleServer  # 战斗服（KCP/UDP :10086）
```

**依赖方向**：`core ← dao ← growthServer / battleServer`。core 是叶子模块，两个业务模块互不依赖。

### 技术栈

- **Java 25 (LTS)** + 纯 Netty + KCP（可靠 UDP）+ Protobuf
- **无 Spring Boot**：手动依赖注入，入口类显式 `new` 所有组件
- **构建插件**：`protobuf-maven-plugin`（编译 proto）+ `maven-shade-plugin`（打可执行 fat jar）

### 核心设计模式

1. **Actor 模型**：每个玩家（GrowthActor）和每个房间（Room）都是单线程事件循环，通过 `BlockingQueue` 与外部通信，无锁保证状态一致性
2. **两层路由**：
   - 第一层 Router（Netty 线程）：处理鉴权/心跳，鉴权后切换回调
   - 第二层 Actor/Room（业务单线程）：处理所有游戏逻辑
3. **帧同步**（battleServer）：30Hz tick，Lockstep 稀疏帧映射存储输入历史，支持断线重连批量重放

### 入口类与服务启动

| 服务 | 入口类 | 默认端口 | 协议 |
|------|--------|----------|------|
| 养成服 | `com.tzw.growth.GrowthServer` | 10087 | TCP 长连接 |
| 战斗服 | `com.tzw.battle.BattleServer` | 10086 | KCP 可靠 UDP |

运行方式：
```bash
java -jar growthServer/target/growthServer-1.0.0-SNAPSHOT.jar [端口]
java -jar battleServer/target/battleServer-1.0.0-SNAPSHOT.jar [端口]
```

### 模块职责

**core** — 被所有模块依赖
- `network/`：TCP/UDP 网络层（`Server` KCP、`TcpServer` TCP、`Conn` 连接抽象）
- `packet/`：应用层协议拆包（`MsgProtocol`：`[2B dataLen][1B msgID][protobuf body]`）
- `mq/`：MQ 接口（`MqProducer`/`MqConsumer`/`TypedMqConsumer`）+ 事件类 + `InMemoryMqAdapter`（进程内实现）
- `proto/`：`message.proto` → 生成 `com.tzw.pb.Message`

**growthServer** — 养成业务
- `GrowthActor`：每玩家单线程 Actor，处理升级/装备/抽卡/匹配
- `GrowthSessionManager`：管理所有玩家 Actor 的生命周期
- `GrowthRouter`：TCP 第一层分发（GrowthAuth 鉴权 + 心跳）

**battleServer** — 战斗业务
- `Room` / `RoomManager`：房间 Actor，30Hz tick 驱动帧同步
- `Game`：状态机 `READY→GAMING→OVER→STOP`，帧广播
- `Lockstep`：稀疏帧映射，重复输入检测
- `Router`：KCP 第一层分发（Connect 握手 + 心跳）
- `MatchService`：匹配逻辑（已迁移但未接入入口，待接线）

**dao** — 数据访问（空壳）
- 仅有 `Placeholder.java`，Redis/MySQL 实现待迁入

### 通信协议

- Protobuf 消息 ID：战斗 1~100，养成 200~242（详见 `core/src/main/proto/message.proto`）
- 包格式：`[2B dataLen][1B msgID][protobuf body]`，最大 1024 字节
- MQ Topic：`match.create`、`match.ready`、`match.result`

### 测试

- 框架：JUnit 5（`junit-jupiter`）
- 当前覆盖：`battleServer/src/test/java/com/tzw/logic/game/LockstepTest.java`（9 个用例，覆盖 Lockstep 帧缓冲核心逻辑）
- 运行测试前需先 `mvn install -DskipTests` 安装依赖模块到本地仓库

## 注意事项

- **不要恢复 Spring Boot**：本项目刻意去除了 Spring，所有依赖在入口类手动装配
- **target1/ 目录**：原始的 Java 21 Spring Boot 单模块实现（参考用），不参与新工程构建，已被 `.gitignore` 忽略
- **Netty Unsafe 警告**：Java 25 上 Netty 4.1.x 使用 `sun.misc.Unsafe` 会产生弃用警告，不影响功能，升级 Netty 4.2+ 可解决
