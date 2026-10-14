# 重构优化设计：行为保持的代码结构清理

日期：2026-10-14
状态：已批准（用户确认方案 A）

## 1. 目标与范围

对 `java_game_server` 进行**行为保持**的代码结构清理（方案 A：类级拆分 + 提取共享逻辑 + 全局小项）。

**范围内**：
- 拆分 `GrowthActor.java`（661 行，事件循环与业务处理器混杂）
- 提取 `Game.java` 中 `doReconnect` 与 `broadcastFrameData` 重复的帧打包逻辑
- 全局小项：MQ topic 常量、内联全限定名、新增类的日志风格、CLAUDE.md 增量更新

**范围外**（明确不做）：
- 不动 core 网络层（`Server.java`/`TcpServer.java` 的 TCP/UDP 重复不抽象）
- 不修事件循环"每轮只处理一条消息"的模式（另立任务）
- 不实现持久化（`saveData()` 的空 TODO 与 `PersistScheduler` 引用保持原样）
- 不修正 `ERR_RoomState` 错误码语义（客户端字节不变式优先）
- 不做全面文档修订（CLAUDE.md 只补充本次新增结构）

## 2. 行为契约（不变式）

以下内容**逐字节不变**：
1. 客户端收到的包序列：msgID + protobuf 字段（包括语义不当的 `ERR_RoomState` 响应）
2. MQ topic 字符串（`match.create`/`match.ready`/`match.result`）与事件载荷
3. 状态机（READY→GAMING→OVER→STOP）转换时序与帧广播触发条件

允许变化的只有：日志文本、类/方法组织、内部命名、Javadoc/注释位置。

**验收标准**：
1. `JAVA_HOME="C:/Program Files/Java/jdk-25" mvn compile` 通过
2. 全模块 `mvn test` 绿色（含新增特征化测试）
3. 特征化测试对重构前代码跑绿（建立行为基线），对重构后代码同样跑绿
4. `GrowthActor.java` 降至 ~250 行以内；`Game.java` 中帧打包逻辑仅存一处实现

## 3. 测试安全网（先写测试，后重构）

### 3.1 发送捕获：同包探针

业务出口是 `Player.sendMessage`/`GrowthActor.send` → `Conn.asyncWrite` → 私有 `sendQueue`，
真实刷出依赖 Server 发送线程调用 package-private 的 `Conn.flushSendQueue()`。

**做法**：在测试目录创建与 `Conn` 同包名的探针类，零生产代码改动：
- `battleServer/src/test/java/com/tzw/network/SendQueueProbe.java`
- `growthServer/src/test/java/com/tzw/network/SendQueueProbe.java`

两份 ~30 行拷贝（跨模块 test-jar 不值当，接受重复，注释互相引用）。

探针 API：
```java
// 构造 fake Conn 并收集刷出的字节
static FakeConn captureConn()            // writer 收集 byte[]，可 flushSendQueue() 排空
static List<Captured> drain(Conn conn)   // flush 后用 MsgProtocol 反解为 (msgID, protobuf body)
record Captured(int msgID, byte[] body)
```

注意：同包名跨模块访问 package-private 成员在 classpath 模式下合法（项目无 JPMS 模块声明）。

### 3.2 GameProtocolTest（battleServer）

驱动真实 `Game` 的公开接口（`joinGame`/`processMsg`/`tick`），锁定：
- JoinRoom 响应字段（座位号、随机种子、others/props 列表）
- Progress 广播排除发送者
- Ready → 全员就绪 → Start 消息 → GAMING 帧广播序列
- Input 推帧 + dirty 强制广播
- `maxFrameDataPerMsg` 打包边界（构造超过单包容量的帧数验证分包）
- 断线重连重放（GAMING 状态收到 Ready → Start + 历史帧批量 + 游标推进）
- Result 全员提交 → OVER → 下一 tick doGameOver

### 3.3 GrowthActorProtocolTest（growthServer）

`tell()` 投递 + 启动 `run()` 线程 + 探针收集响应，锁定：
- 升级：成功（金币扣除、等级+1、攻击+5）/ 目标等级非法 / 金币不足（响应含 `ERR_RoomState`）
- 装备/卸下：成功与失败路径
- 抽卡：1 次（花费 100）/ 10 次（花费 900）/ 次数非法 / 金币不足；随机性用分布不变式断言（背包 +N 件、attackBonus ∈ {5,15,30,50}），不做逐字节断言
- 心跳回包、PlayerData/Inventory 推送
- `MSG_EnterMatch` → fake `MqProducer` 断言发出 `match.create` + `MatchCreateEvent(playerId, 1)`
- `match.ready`/`match.result` 事件 → 对应 S2C 消息与奖励入账（含自动升级循环）

线程时序：用轮询 + 超时收敛（如 `Awaitility` 风格手写循环，上限 2s），不用固定 sleep 猜时长。

### 3.4 既有测试

`LockstepTest`（9 用例）、`MatchServiceTest`、dao 的 Redis 测试保持不动，作为回归的一部分。

## 4. GrowthActor 拆分

```
growthServer/src/main/java/com/tzw/logic/growth/
├── GrowthActor.java            # 事件循环、三队列+eventQ、ConnCallback、processMsg/processEvent 分发、stop/saveData（目标 ~250 行）
├── GrowthMessenger.java        # volatile conn 持有者；send(MsgPacket)、bind(conn)/unbind(conn)、pushPlayerData()、pushInventory()
├── PlayerGrowthState.java      # 不动
└── handler/
    ├── UpgradeHandler.java     # handleUpgrade + sendUpgradeResult
    ├── EquipHandler.java       # handleEquip + handleUnequip + 两个 send 响应
    ├── GachaHandler.java       # handleGacha + rollGacha；概率表 static final RarityTier[]（record: threshold/itemId/name/attackBonus）数据驱动；Random 构造注入（默认 new Random()）
    └── MatchHandler.java       # handleEnterMatch（MqProducer 注入）+ handleMatchReady + handleMatchResult
```

**规则**：
- Handler 构造注入 `(PlayerGrowthState, GrowthMessenger)`（GachaHandler 额外 `Random`，MatchHandler 额外 `MqProducer`）
- Handler 方法签名 `void handle(MsgPacket msg)`，业务逻辑**逐行搬移不改写**（含 try/catch 结构、日志措辞、错误码）
- `GrowthActor` 构造函数手动 new 全部 Handler（延续无框架、入口显式装配风格）
- `processMsg` 的 switch 每个 case 变为一行委托
- `processEvent` 保留在 Actor（事件分发是循环职责），payload 类型 switch 后委托 MatchHandler
- `saveData()` 空 TODO 保留在 Actor 原位，不实现

## 5. Game 帧打包去重

新增 `battleServer/src/main/java/com/tzw/logic/game/FrameBatchPacker.java`：

```java
final class FrameBatchPacker {
    /**
     * 从 from（含）到 to（不含）逐帧打包 S2C_FrameMsg：
     * 跳过无输入的空帧（to-1 帧除外）；累计 maxPerMsg 帧或到达末帧时通过 sender 发出一包。
     * @return 实际发出的包数
     */
    static int pack(Lockstep lockstep, long from, long to, int maxPerMsg,
                    Consumer<Message.S2C_FrameMsg> sender)
}
```

调用点改造：
- `broadcastFrameData()`：每玩家 `pack(lockstep, p.getSendFrameCount(), framesCount, maxPerMsg, m -> p.sendMessage(...))`，循环后 `p.setSendFrameCount(framesCount)`
- `doReconnect(p)`：`pack(lockstep, 0, clientFrameCount, maxPerMsg, ...)`，随后 `p.setSendFrameCount(clientFrameCount)`

**已知差异归并**：`doReconnect` 中先建空 `FrameData` 再在 `frameData != null` 时整体重建（空壳 `fd` 变量实际未被使用）；`broadcastFrameData` 用 builder 按需填 input。两者产出的字节序列相同，统一采用 builder 写法，由特征化测试的重连场景断言锁定。

`Game.java` 保留：状态机 tick、消息处理、广播辅助、状态检查。

## 6. 全局小项

| 项 | 做法 |
|---|---|
| MQ topic 硬编码 | 新增 `core/src/main/java/com/tzw/mq/MqTopics.java`：`public static final String MATCH_CREATE/MATCH_READY/MATCH_RESULT`；替换 growthServer/battleServer **生产代码**中的 14 处字面量；测试与 proto 注释中的字面量保留（黑盒验证） |
| 内联全限定名 | `GrowthActor` 的 `java.util.Collections`/`ArrayList`/`List`/`java.util.concurrent.TimeUnit`、`Room.run` 的 `java.util.concurrent.TimeUnit` 等统一改为 import |
| 日志风格 | 新 Handler 类沿用既有前缀（`[GrowthActor-{id}]`）；旧日志文本不动 |
| CLAUDE.md | 模块职责段补充：growthServer 的 handler 拆分结构、`MqTopics`、`FrameBatchPacker`；不改动其余过时内容 |

## 7. 实施顺序

```
① 行为基线：SendQueueProbe ×2 + GameProtocolTest + GrowthActorProtocolTest（对旧代码跑绿）
② FrameBatchPacker 提取，Game 两处改造（②后跑测试）
③ GrowthActor → GrowthMessenger + 4 个 Handler（每拆一个跑一次测试）
④ MqTopics 常量 + import 清理（编译 + 全量测试）
⑤ CLAUDE.md 增量更新
```

每步独立可编译可测试，失败回滚面为单步。

## 8. 风险与对策

| 风险 | 对策 |
|---|---|
| 特征化测试对 Actor `run()` 线程时序敏感 | 轮询 + 超时收敛（2s 上限），断言前先等待预期包数 |
| Handler 搬移时行为漂移（复制漏改） | 逐行搬移纪律 + ③ 步每拆一个 Handler 即跑全量测试 |
| 跨模块同包探针在打包环境失效 | 仅测试作用域使用；若 shade/模块化引发问题，降级为反射读取 sendQueue |
| doReconnect/broadcast 归并遗漏差异 | 特征化测试分别锁定两处旧行为，归并后两个用例都必须绿 |
