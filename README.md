# calmcut-studio · 分镜风险增量分析服务

科普短视频分镜（storyboard）的**风险增量分析后台**。接收带版本号的分镜批量导入与增删改事件，
维护一条连续、无重叠的时间轴，并计算五类**可解释**的编辑风险。单段修改后只重算受影响窗口，
结果与全量重算**逐项一致**。

> 说明：本服务只描述**剪辑结构层面**的风险，输出永不诊断"成瘾"、永不预测"脑损伤"或任何临床/神经学结论。
> 相关约束由 `RiskRulesTest` 中的用语校验测试强制保证。

## 架构

```
写入 API ──append──▶ event_log ─┐(同一事务)                Redpanda/Kafka
             (乐观锁+完整性校验)  └─▶ outbox ──OutboxPublisher──▶ storyboard.events
                                                                      │
                                                          AnalysisConsumer(幂等/乱序缓冲)
                                                                      │
                                              IdempotentProcessor ── 投影表 + 增量分析
                                                       │  └─失败重试─▶ dead_letter(可重放)
                                                       └──▶ storyboard.analysis.results
```

- **事件日志（event sourcing）**：所有变更以带版本号的 `StoryboardEvent` 追加写入 `storyboard_event`。
- **投影表（projection）**：`segment_projection` 保存连续时间轴，`analysis_projection` 保存最新分析结果。
- **异步分析 worker**：从 Kafka 消费事件，折叠投影并做增量分析。
- **Outbox**：写 API 在同一事务里写入 `event_log` 与 `outbox`，由 `OutboxPublisher` 最终发布，保证事件不丢。
- **幂等 + 乱序缓冲**：`IdempotentProcessor` 通过事件 id 去重；`VersionBuffer` 按版本严格排序释放，丢弃过期版本。
- **死信重放**：多次失败的事件进入 `dead_letter`，可通过 `/dlq` 查看并重放。
- **从零重建投影**：`IdempotentProcessor.rebuildFromZero` 重放事件日志重建投影。

## 五类风险规则（`analysis/rules`）

| 代码 | 含义 | 触发条件 |
|------|------|----------|
| `REVERSAL_DENSITY` | 强反转过密 | 任意 60 秒窗口内强反转 **> 6** 次 |
| `CONSECUTIVE_INTENSITY` | 连续高刺激 | **连续 ≥ 3** 段刺激强度为 **5** |
| `SHORT_AVERAGE_SHOT` | 平均镜头过短 | 平均镜头时长 **< 3 秒** |
| `DECLINE_INDUCEMENT` | 继续下滑诱导 | 出现标记为"继续下滑诱导"的镜头 |
| `MISSING_KNOWLEDGE_BUFFER` | 缺少缓冲段 | 两个不同知识点之间缺少低刺激缓冲段 |

每条命中返回：`ruleVersion`（规则版本）、`hitSegmentIds`（命中段）、`evidence`（证据）、`suggestion`（建议）。

### 增量 = 全量

每条规则都是时间轴的纯函数，并实现 `affectedWindow(timeline, changed)`：把局部改动扩展成
"其命中集合可能变化的最小窗口"。位于窗口之外、锚点仍存在的旧命中被原样保留，窗口内重算，
合并后与全量重算**逐项相等**。该性质由 `IncrementalEquivalenceTest` 用 3000 次随机
增/删/改场景验证。

## 构建与测试

```bash
./gradlew test          # 40 个单元测试，无需外部依赖（用内存 fake）
./gradlew build         # 编译 + 测试 + 生成可运行 jar
./gradlew shadowJar     # 生成 build/libs/calmcut-studio-all.jar
```

## 本地运行

```bash
docker compose up -d           # 启动 PostgreSQL + Redpanda
./gradlew run                  # 启动 Ktor 服务（默认 :8080）
```

主要环境变量：`DB_JDBC_URL` / `DB_USERNAME` / `DB_PASSWORD` / `KAFKA_BOOTSTRAP_SERVERS`。

## HTTP 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/storyboards/batch-import` | 批量导入（携带 `expectedVersion` 做乐观锁）|
| POST | `/storyboards/segments` | 新增单段 |
| POST | `/storyboards/segments/update` | 修改单段 |
| POST | `/storyboards/segments/delete` | 删除单段 |
| GET  | `/storyboards/{id}/analysis` | 查询最新分析结果 |
| POST | `/imports` | 启动流式导入（百万分镜，分块 + 检查点）|
| GET  | `/imports/{jobId}` | 查询导入进度 |
| POST | `/imports/{jobId}/cancel` | 取消导入 |
| POST | `/imports/{jobId}/resume` | 从检查点恢复导入 |
| GET  | `/dlq` | 列出死信事件 |

乐观锁冲突返回 `409`，知识点完整性校验失败返回 `422`。

## 测试覆盖

- `RiskRulesTest` — 五类规则触发/不触发边界 + 禁用临床用语校验
- `IncrementalEquivalenceTest` — 增量 vs 全量等价（3000 次随机 property test）
- `DriftDetectionTest` — 投影漂移检测 + 从零重建等价
- `VersionBufferTest` — 乱序缓冲、按序释放、过期丢弃
- `WorkerCrashAndIdempotencyTest` — worker 崩溃重试、重复消息幂等、乱序、死信重放
- `TimelineValidationTest` — 连续无重叠时间轴 + 原版/修订版知识点完整性
- `StreamingImportTest` — 百万级流式分块、取消、断点恢复

## 技术栈

Kotlin 2.1 · Ktor 3 · Exposed + PostgreSQL · Kafka client（Redpanda 兼容）· kotlinx.coroutines/serialization
