# 行为保持重构实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 按规格 `docs/superpowers/specs/2026-10-14-refactor-design.md` 完成行为保持的结构清理：先建特征化测试锁行为，再拆 GrowthActor、提取 FrameBatchPacker、统一 MQ 常量。

**Architecture:** 测试先行（同包探针捕获 Conn 发送字节）；Handler 逐行搬移不改写；每任务独立编译+测试+提交。

**Tech Stack:** Java 25、Maven 多模块、JUnit 5、Netty、Protobuf。

## Global Constraints

- 构建必须 `JAVA_HOME="C:/Program Files/Java/jdk-25"`（系统默认 JDK 21）
- 运行模块测试前先 `JAVA_HOME=... mvn install -DskipTests -q`
- 客户端字节（msgID+protobuf 字段，含 `ERR_RoomState`）、MQ topic 字符串、状态机时序 **逐字节不变**
- 不新增依赖、不动 core 网络层、不实现持久化（`saveData()` TODO 原样保留）
- Handler 搬移纪律：逻辑逐行复制，含 try/catch、日志措辞、错误码

---

### Task 1: 行为基线 —— SendQueueProbe（battleServer）+ GameProtocolTest

**Files:**
- Create: `battleServer/src/test/java/com/tzw/network/SendQueueProbe.java`
- Create: `battleServer/src/test/java/com/tzw/logic/game/GameProtocolTest.java`

**Interfaces:**
- Produces: `SendQueueProbe.create(): SendQueueProbe`、`conn(): Conn`、`drain(): List<Captured>`、`record Captured(int msgID, byte[] body)`（Task 2 在 growthServer 复制同款）

**原理**：`Conn.flushSendQueue()` 是 package-private，探针与 `Conn` 同包名（classpath 模式合法，无 JPMS）。writer 回调收集 `packet.serialize()` 字节，`MsgProtocol.decode(Unpooled.wrappedBuffer(bytes))` 反解回 `(msgID, body)`。

- [ ] **Step 1: 确认 ConnCallback 精确签名后写探针**（读 `core/src/main/java/com/tzw/network/ConnCallback.java`，Noop 实现按其方法签名）
- [ ] **Step 2: 写 GameProtocolTest**，7 个用例锁定旧 Game 行为（`broadcastOffsetFrames` 保持默认 3，靠 dirty/帧差触发）：
  1. `joinRoomRepliesSeatAndSeed`：JoinRoom 回包 roomseatid=1、randomSeed、others=[P2]、pros=[0]
  2. `progressExcludesSender`：P1 Progress(50) → P1 无包、P2 收 S2C_ProgressMsg(id=P1,pro=50)；GAMING 状态后 Progress 被忽略
  3. `readyBothStartsGame`：双 Ready（各收 MSG_Ready 回执）→ tick → Start 广播 + listener `start` 事件 + State.GAMING
  4. `inputDirtyBroadcastsFrame`：Start 后 P1 Input → tick → 双方各收 MSG_Frame{frame0 含 P1 InputData(sid,x,y,roomseatid)}
  5. `reconnectBatchesHistoryFrames`：`maxFrameDataPerMsg=2`，推 5 帧（每帧 P1 输入+tick），P2 在 GAMING 发 Ready → P2 收 Start + 3 个 Frame 包（帧数 2/2/1）+ 游标=5
  6. `gameOverAfterAllResults`：双 Result（各收 MSG_Result 回执）→ tick(OVER) → tick(doGameOver 事件) → tick 返回 false
  7. `joinRejectedAfterOver`：结束后 join → ConnectMsg 带 ERR_RoomState
- [ ] **Step 3: 运行** `JAVA_HOME=... mvn install -DskipTests -q && JAVA_HOME=... mvn test -pl battleServer -Dtest=GameProtocolTest`。期望 PASS（对旧代码建立基线；失败说明我对代码理解有误，先修正测试再对照生产代码查证，禁止改生产代码）
- [ ] **Step 4: 回归** `-Dtest=GameProtocolTest,LockstepTest` 全绿后提交：`git commit -m "test: 锁定 Game 帧同步协议行为基线"`

---

### Task 2: 行为基线 —— SendQueueProbe（growthServer）+ GrowthActorProtocolTest

**Files:**
- Create: `growthServer/src/test/java/com/tzw/network/SendQueueProbe.java`（与 Task 1 相同，头注释互相引用）
- Create: `growthServer/src/test/java/com/tzw/logic/growth/GrowthActorProtocolTest.java`

**Interfaces:**
- Consumes: `GrowthActor(long, MqProducer)`、`actor.tell(MsgPacket)`、`actor.tellEvent(GrowthEvent)`、`actor.onConnect(Conn)`、`actor.getState()`、`actor.stop()`；`GrowthRouter.GrowthEvent(String,Object)` record；fake `MqProducer`
- Produces: 行为基线，Task 4/5 拆分后必须同样通过

**时序安全**：所有断言前用轮询等待（10ms 间隔，2s 上限）收集 `probe.drain()`；禁止固定 sleep 猜时长。

- [ ] **Step 1: 复制探针** 到 growthServer 测试目录
- [ ] **Step 2: 写测试**，setUp 启线程 + `onConnect` 后等待首包 PlayerData；8 个用例：
  1. `upgradeSuccessThenInsufficientGold`：目标级 2（成功 gold900 lvl2 atk15）→ 3（success gold500 lvl3 atk20）→ 4（cost900>500，`ERR_RoomState` success=false 回显 lvl3/500）→ 6（跨度非法，同错误码）
  2. `gachaInvalidTimes`：times=2 → `ERR_RoomState` 空 items gold1000
  3. `gachaCostMatrix`：十连成功 gold100；再十连不足 `ERR_RoomState`；单抽成功 gold0；背包件数合计=10、bonus ∈ {5,15,30,50}（随机性用不变式断言）
  4. `equipFlow`：单抽 → 从 `actor.getState().getInventory()` 取 itemId（响应先于断言，addItem 在回包前完成）→ Equip 成功 newAttack=10+bonus → Unequip 成功 atk10 → 重复 Unequip `ERR_RoomState`
  5. `heartbeatAndPushes`：Heartbeat/PlayerData/Inventory 回包
  6. `enterMatchPublishesMq`：fake Mq 收到 `("match.create", MatchCreateEvent(playerId, 1))`
  7. `matchReadyEventPushesClient`：`GrowthEvent("match.ready", MatchReadyEvent(55, 7, "10.0.0.2", 10086, "tk"))` → S2C_MatchReadyMsg 四字段一致
  8. `matchResultAutoLevelUp`：`("match.result", MatchResultEvent(55, 7, true, 100, 50, 0))` → S2C_MatchResultMsg newLevel=2，state gold=1050 level=2
- [ ] **Step 3: 运行** `JAVA_HOME=... mvn test -pl growthServer -Dtest=GrowthActorProtocolTest`，对旧 GrowthActor PASS；tearDown `actor.stop(); thread.join(1000)`
- [ ] **Step 4: 提交** `git commit -m "test: 锁定 GrowthActor 养成协议行为基线"`

---

### Task 3: FrameBatchPacker 提取（Game 去重）

**Files:**
- Create: `battleServer/src/main/java/com/tzw/logic/game/FrameBatchPacker.java`
- Create: `battleServer/src/test/java/com/tzw/logic/game/FrameBatchPackerTest.java`
- Modify: `battleServer/src/main/java/com/tzw/logic/game/Game.java:587-631`（doReconnect）、`Game.java:654-717`（broadcastFrameData 内层循环）

**Interfaces:**
- Produces: `static int pack(Lockstep lockstep, long from, long to, int maxPerMsg, Consumer<Message.S2C_FrameMsg> sender)`（包私有 final 类）

- [ ] **Step 1: 写 FrameBatchPackerTest（先失败）**：3 用例——空区间发 0 包；稀疏帧（输入@1，to=3）发 1 包含帧{1,2}（末帧空也发）；连续 5 帧输入 max=2 → 3 包（2/2/1）
- [ ] **Step 2: 运行验证失败**（类不存在，编译错误即预期 FAIL）
- [ ] **Step 3: 实现 packer**：builder 写法（统一两处旧差异：doReconnect 的空壳 fd 变量丢弃）；`Game.doReconnect` 与 `Game.broadcastFrameData` 改为调用 `pack(...)`，游标设置保留在原方法
- [ ] **Step 4: 运行** FrameBatchPackerTest、GameProtocolTest、LockstepTest、MatchServiceTest 全绿（GameProtocolTest 不改动即是去重正确性的证明）
- [ ] **Step 5: 提交** `git commit -m "refactor: 提取 FrameBatchPacker 消除帧打包重复"`

---

### Task 4: GrowthMessenger + Upgrade/Equip Handler（第一批拆分）

**Files:**
- Create: `growthServer/src/main/java/com/tzw/logic/growth/GrowthMessenger.java`
- Create: `growthServer/src/main/java/com/tzw/logic/growth/handler/UpgradeHandler.java`
- Create: `growthServer/src/main/java/com/tzw/logic/growth/handler/EquipHandler.java`
- Modify: `growthServer/src/main/java/com/tzw/logic/growth/GrowthActor.java`（删除搬走的私有方法与 conn 字段，processMsg 改委托）

**Interfaces:**
- Produces: `GrowthMessenger(long playerId, PlayerGrowthState state)`、`bind(Conn)/unbind(Conn)/send(MsgPacket)/pushPlayerData()/pushInventory()`；`UpgradeHandler(PlayerGrowthState, GrowthMessenger).handle(MsgPacket)`；`EquipHandler(...).handleEquip(MsgPacket)/.handleUnequip(MsgPacket)`
- Consumes: Task 2 的行为基线测试

- [ ] **Step 1: GrowthMessenger**：`volatile Conn conn` + send/pushPlayerData/pushInventory（逐行搬 `GrowthActor.java:622-646`）
- [ ] **Step 2: UpgradeHandler/EquipHandler**：逐行搬 `GrowthActor.java:334-451`，`send(...)`→`messenger.send(...)`，日志前缀 `[GrowthActor-{id}]` 保留
- [ ] **Step 3: GrowthActor 接线**：构造 new 两 Handler + messenger；`run()` 中 in/outChan 处理改 `messenger.bind/unbind`；删除原方法。GachaHandler/MatchHandler 暂缺——剩余业务方法保留在 Actor
- [ ] **Step 4: 运行** `mvn install -DskipTests -q && mvn test -pl growthServer -Dtest=GrowthActorProtocolTest` 全绿
- [ ] **Step 5: 提交** `git commit -m "refactor: 拆出 GrowthMessenger 与 Upgrade/Equip Handler"`

---

### Task 5: Gacha/Match Handler（第二批拆分，Actor 到位）

**Files:**
- Create: `growthServer/src/main/java/com/tzw/logic/growth/handler/GachaHandler.java`（`RarityTier` record + `static final RarityTier[]` 概率表；`Random` 构造注入默认 `new Random()`）
- Create: `growthServer/src/main/java/com/tzw/logic/growth/handler/MatchHandler.java`（handleEnterMatch/handleMatchReady/handleMatchResult，注入 `MqProducer`）
- Modify: `GrowthActor.java`（删剩余业务方法；processMsg/processEvent 全委托；目标 ≤250 行）

- [ ] **Step 1: GachaHandler**：搬 `GrowthActor.java:456-544`；概率阈值语义严格对应旧 if-else 链（<5 传说50 / <20 史诗30 / <50 稀有15 / 其余普通5）
- [ ] **Step 2: MatchHandler**：搬 `GrowthActor.java:316-327, 551-562, 576-598`
- [ ] **Step 3: 接线**：processMsg `MSG_Gacha → gachaHandler.handle(msg)` 等；processEvent 保留 Actor（分发职责），委托 MatchHandler
- [ ] **Step 4: 运行** GrowthActorProtocolTest + 全模块测试绿；`wc -l GrowthActor.java` ≤ ~260
- [ ] **Step 5: 提交** `git commit -m "refactor: 拆出 GachaHandler 与 MatchHandler，GrowthActor 瘦身为事件循环"`

---

### Task 6: MqTopics 常量 + import 清理

**Files:**
- Create: `core/src/main/java/com/tzw/mq/MqTopics.java`（`MATCH_CREATE/MATCH_READY/MATCH_RESULT`，String 常量可作 switch 标签）
- Modify: `growthServer/.../GrowthRouter.java:82-92`（4 处）、`growthServer/.../GrowthActor.java`（processEvent switch + MatchHandler 的 3 处）、`battleServer/.../MatchService.java:100,170,196`
- Modify: 全限定名改 import：`Room.java:351`（`java.util.concurrent.TimeUnit`）、新拆文件中残留 FQN

- [ ] **Step 1: MqTopics**（生产代码 10 处替换；**测试文件中 5 处字面量保留**做黑盒验证；`"conn.close"` 是事件类型不是 topic，不动）
- [ ] **Step 2: import 清理**：GrowthActor 及其 Handler 的 `java.util.Collections/ArrayList/List/TimeUnit`、Room 的 TimeUnit
- [ ] **Step 3: 运行** 全模块编译 + `mvn test -pl battleServer,growthServer`（dao 测试若依赖外部 Redis 则跳过说明，检查 `dao/src/test` 是否需要运行环境）
- [ ] **Step 4: 提交** `git commit -m "refactor: MQ topic 常量化与 import 风格统一"`

---

### Task 7: CLAUDE.md 增量更新 + 终验

**Files:**
- Modify: `CLAUDE.md`（模块职责段：growthServer handler 结构、`MqTopics`、`FrameBatchPacker` 各一行）

- [ ] **Step 1: 更新文档段落**
- [ ] **Step 2: 终验** `JAVA_HOME=... mvn install -DskipTests -q && JAVA_HOME=... mvn test`（能跑的模块全绿；行为基线测试全绿 = 字节不变达成）
- [ ] **Step 3: 提交** `git commit -m "docs: 同步重构后的模块结构说明"`

---

## Self-Review 记录

- 规格 §3 探针/两组特征化测试 → Task 1/2 ✓；§5 packer → Task 3 ✓；§4 拆分 → Task 4/5 ✓；§6 全局小项 → Task 6 ✓；§1 CLAUDE.md → Task 7 ✓；§7 顺序一致 ✓
- 探针 API 与规格 §3.1 修正后签名一致（captureConn→实例 `conn()` 方法，构造即捕获）
- Task 4→5 的 GrowthActor 中间态允许业务方法暂存 Actor，最终态在 Task 5 收敛
