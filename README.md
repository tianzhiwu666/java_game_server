# java_game_server

Java 25 帧同步游戏服务器 — 养成 + 战斗双服务架构（Netty + KCP + Protobuf）

## 项目简介

本项目是一个基于 **Java 25 (LTS)** 的多模块游戏服务器，采用**纯 Netty + KCP + Protobuf** 技术栈（无 Spring Boot），支持养成系统和帧同步战斗两大核心玩法。

### 技术选型

| 组件 | 技术 | 说明 |
|------|------|------|
| 语言 | Java 25 (LTS) | 虚拟线程、模式匹配等新特性 |
| 网络 | Netty 4.1.x | TCP 长连接 + KCP 可靠 UDP |
| 序列化 | Protobuf 3.25 | 客户端/服务端跨语言通信 |
| 构建 | Maven 多模块 | core / dao / growthServer / battleServer |
| 架构 | Actor 模型 | 每玩家/每房间单线程事件循环，无锁设计 |

## 模块结构

```
com.tzw:game-server (父 POM)
├── core          # 网络层 / 协议层 / MQ接口 / Protobuf
├── dao           # 数据访问层（待实现 Redis + MySQL）
├── growthServer  # 养成逻辑服（TCP :10087）
└── battleServer  # 战斗服（KCP/UDP :10086）
```

**依赖方向**：`core ← dao ← growthServer / battleServer`

## 架构设计

### 双连接设计

客户端同时持有两条连接，各司其职：

```
客户端
  ├── TCP 长连接 ──→ growthServer (:10087)
  │     养成操作：升级 / 装备 / 抽卡 / 匹配请求
  │     特点：可靠、低频、长生命周期
  │
  └── KCP 直连 ────→ battleServer (:10086)
        帧同步对战：帧输入 / 帧广播 / 战斗结果
        特点：低延迟、高频、单局生命周期
```

**为什么双连接？**
- 养成系统：低频、可靠、需要数据库交互 → TCP
- 战斗系统：高频（30Hz）、低延迟、可容忍偶丢 → KCP（可靠 UDP）

### 线程模型

```
Netty EventLoop (KCP/UDP)                Netty EventLoop (TCP)
  │ channelRead()                           │ channelRead()
  │ ↓ 拆包 ([2B len][1B id][pb body])      │ ↓ 拆包
  ▼                                         ▼
Router.onMessage() (Netty 线程)           GrowthRouter.onMessage() (Netty 线程)
  │ 处理 Connect 握手 / 心跳                │ 处理 GrowthAuth 鉴权 / 心跳
  │ 鉴权通过后切换回调                      │ 鉴权通过后切换回调
  ▼                                         ▼
Room.msgQ (BlockingQueue, 2048)           GrowthActor.msgQ (BlockingQueue, 512)
  │ offer() 入队                            │ offer() 入队
  ▼                                         ▼
每房间单线程 Executor (room-{id})         每玩家单线程 Executor (growth-{id})
  │ Room.run()                              │ GrowthActor.run()
  │ ↓ 30Hz tick + 消息消费                  │ ↓ 纯消息驱动（无 tick）
  │ Game.processMsg()                        │ processMsg()
  │ ↓ 状态机 READY→GAMING→OVER→STOP         │ ↓ 升级/装备/抽卡/匹配
  ▼                                         ▼
broadcastFrameData()                        send()
  ↓ conn.asyncWrite()                       ↓ conn.asyncWrite()
  ↓ 发送队列 → 发送线程每 10ms 刷新          ↓ 发送队列 → 发送线程每 10ms 刷新
```

**核心设计原则**：
- Netty 线程只做鉴权和元数据检查，**不修改游戏状态**
- 所有状态修改在 Actor 单线程中完成，**无需加锁**
- BlockingQueue 是跨线程通信的**唯一桥梁**

### 两层路由架构

每个服务都有两层消息分发：

| 层级 | 战斗服 | 养成服 |
|------|--------|--------|
| 第一层（Netty 线程） | `Router`：MSG_Connect 握手 + 心跳 | `GrowthRouter`：MSG_GrowthAuth 鉴权 + 心跳 |
| 第二层（业务单线程） | `Room` → `Game`：游戏逻辑 | `GrowthActor`：养成业务 |

### Actor 模型

```
GrowthActor (每玩家一个)                  Room (每房间一个)
  ├── playerId                            ├── roomId
  ├── msgQ (消息队列, 512)                ├── msgQ (消息队列, 2048)
  ├── inChan (新连接, 8)                  ├── inChan (新连接, 8)
  ├── outChan (断连, 8)                   ├── outChan (断连, 8)
  ├── eventQ (系统事件, 64)               ├── game (Game 逻辑对象)
  ├── state (PlayerGrowthState)            └── 30Hz tick 驱动
  └── mqProducer
```

### 通信协议

**数据包格式**（客户端↔服务端通用）：

```
┌───────────────┬───────────────┬──────────────────────────┐
│  数据长度(2B)  │  消息ID(1B)   │      protobuf 正文        │
│  uint16大端   │    uint8      │      (dataLen 字节)       │
└───────────────┴───────────────┴──────────────────────────┘
```

**消息 ID 分配**：

| 范围 | 用途 |
|------|------|
| 1~100 | 战斗消息（Connect/Heartbeat/JoinRoom/Progress/Ready/Start/Frame/Input/Result/Close） |
| 200~242 | 养成消息（GrowthAuth/PlayerData/UpgradeLevel/EquipItem/UnequipItem/Gacha/Inventory/EnterMatch/MatchReady/MatchResult） |
| 255 | MSG_END |

**MQ Topic**：
- `match.create` — 养成服 → 战斗服（匹配请求）
- `match.ready` — 战斗服 → 养成服（匹配成功）
- `match.result` — 战斗服 → 养成服（对战结果）

## 客户端-服务器生命周期

### 1. 登录与养成阶段

```
客户端                                    growthServer
  │                                          │
  │──── TCP 连接 ──────────────────────────►│
  │                                          │
  │──── MSG_GrowthAuth (playerID, token) ──►│
  │                                          │ 验证 token
  │                                          │ 创建 GrowthActor
  │                                          │ 切换回调
  │◄─── S2C_PlayerDataMsg (养成数据) ──────│
  │                                          │
  │──── MSG_UpgradeLevel (目标等级) ───────►│
  │◄─── S2C_UpgradeLevelMsg (成功/失败) ───│
  │                                          │
  │──── MSG_Gacha (times=1或10) ──────────►│
  │◄─── S2C_GachaMsg (抽到的物品) ─────────│
  │                                          │
  │──── MSG_EquipItem (itemID) ────────────►│
  │◄─── S2C_EquipItemMsg (新攻击力) ───────│
```

### 2. 匹配阶段

```
客户端            growthServer                MQ                  battleServer
  │                  │                        │                        │
  │──EnterMatch────►│                        │                        │
  │                  │──match.create────────►│                        │
  │                  │                        │──match.create────────►│
  │                  │                        │                        │ 创建房间
  │                  │                        │                        │ 生成 token
  │                  │                        │◄──match.ready─────────│
  │                  │◄──match.ready─────────│                        │
  │                  │                        │                        │
  │◄─S2C_MatchReady─│                        │                        │
  │  (roomHost,      │                        │                        │
  │   roomPort,      │                        │                        │
  │   token)         │                        │                        │
```

### 3. 战斗阶段

```
客户端                                        battleServer
  │                                              │
  │════ KCP/UDP 连接 ══════════════════════════►│
  │                                              │
  │════ MSG_Connect (playerID, roomID, token) ═►│
  │                                              │ 校验房间存在
  │                                              │ 校验玩家归属
  │                                              │ 校验 token
  │                                              │ 切换回调 → Room
  │                                              │
  │◄═══ S2C_JoinRoomMsg (座位/种子/玩家列表) ══│
  │                                              │
  │════ MSG_Progress (加载进度 0~100) ════════►│
  │◄═══ S2C_ProgressMsg (广播给其他玩家) ══════│
  │                                              │
  │════ MSG_Ready (准备就绪) ═════════════════►│
  │                                              │ 全员准备 → doStart()
  │◄═══ S2C_StartMsg (时间戳) ═════════════════│
  │                                              │
  │    ┌────── 30Hz 帧同步循环 ──────┐           │
  │    │                              │           │
  │════ MSG_Input (玩家操作) ═══════►│           │
  │                              │           │ lockstep.pushCmd()
  │                              │           │ Game.tick()
  │◄═══ S2C_FrameMsg (帧广播) ═════│           │ broadcastFrameData()
  │    │                              │           │
  │    └──────────────────────────────┘           │
  │                                              │
  │════ MSG_Result (winnerID) ════════════════►│
  │                                              │ 全员提交 → OVER
  │◄═══ S2C_MatchResultMsg (胜负/奖励) ════════│
  │                                              │
  │════ 断开 KCP ══════════════════════════════│
```

### 4. 战斗结束与奖励发放

```
客户端            growthServer                MQ                  battleServer
  │                  │                        │                        │
  │                  │                        │                        │ 房间结束
  │                  │                        │◄──match.result────────│
  │                  │◄──match.result────────│                        │
  │                  │                        │                        │
  │                  │ 发放奖励:               │                        │
  │                  │ state.addExp()         │                        │
  │                  │ state.addGold()        │                        │
  │                  │ state.tryLevelUp()     │                        │
  │                  │                        │                        │
  │◄─S2C_MatchResult─│                        │                        │
  │  (胜负/经验/金币) │                        │                        │
  │                  │                        │                        │
  │  (TCP 保持，继续养成)                       │                        │
```

### 5. 断线重连

```
客户端                                    battleServer
  │                                          │
  │════ KCP 连接断开 ═══════════════════════│
  │                                          │ Room.outChan 感知
  │                                          │ Game.leaveGame()
  │                                          │
  │  (TCP 连接保持，玩家可继续养成)           │
  │                                          │
  │════ 重新 KCP 连接 ═════════════════════►│
  │════ MSG_Connect (playerID, roomID) ════►│
  │                                          │ room.hasPlayer() → true
  │════ MSG_Ready ════════════════════════►│
  │                                          │ GAMING 状态 → doReconnect()
  │◄═══ S2C_StartMsg ══════════════════════│
  │◄═══ S2C_FrameMsg (批量历史帧 0~N) ═════│
  │                                          │ 追赶到当前帧，继续对战
```

## 构建与运行

### 构建

```bash
# 编译（必须指定 JDK 25）
JAVA_HOME="C:/Program Files/Java/jdk-25" mvn compile

# 打包为可执行 fat jar
JAVA_HOME="C:/Program Files/Java/jdk-25" mvn package

# 运行测试（需先 install）
JAVA_HOME="C:/Program Files/Java/jdk-25" mvn install -DskipTests
JAVA_HOME="C:/Program Files/Java/jdk-25" mvn test -pl battleServer
```

### 启动服务

```bash
# 养成服（TCP :10087）
java -jar growthServer/target/growthServer-1.0.0-SNAPSHOT.jar

# 战斗服（KCP/UDP :10086）
java -jar battleServer/target/battleServer-1.0.0-SNAPSHOT.jar

# 自定义端口
java -jar growthServer/target/growthServer-1.0.0-SNAPSHOT.jar 10087
java -jar battleServer/target/battleServer-1.0.0-SNAPSHOT.jar 10086
```

## 配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| 养成服端口 | 10087 | TCP 长连接 |
| 战斗服端口 | 10086 | KCP 可靠 UDP |
| 房间时钟 | 30 Hz | 帧同步频率 |
| 房间超时 | 5 分钟 | 单局最大时长 |
| 准备超时 | 20 秒 | 等待玩家准备的最大时间 |
| 最大帧数 | 5500 | 约 3 分钟游戏时长 |
| 帧广播节流 | 3 帧 | 帧差超过此值触发广播 |
