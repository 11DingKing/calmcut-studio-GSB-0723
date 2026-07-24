# 分镜风险增量分析服务 (Storyboard Risk Analyzer)

科普内容团队短视频分镜风险分析后端服务。基于事件溯源 (Event Sourcing) + CQRS 架构，支持实时增量风险评估、百万级分镜流式导入、以及与全量重算严格一致的增量投影。

## 技术栈

| 组件 | 技术 |
|---|---|
| 语言 | Kotlin 2.0 (JVM 17) |
| Web 框架 | Ktor 2.3 (Netty) |
| 数据库 | PostgreSQL 16 (Exposed ORM + Flyway) |
| 消息队列 | Redpanda / Kafka (kafka-clients 3.7) |
| 序列化 | kotlinx.serialization |
| 数据库连接池 | HikariCP |
| 测试 | JUnit 5 + Testcontainers (PostgreSQL + Redpanda) |

## 架构概览

```
┌──────────────┐     ┌─────────────────────────────────────────────────┐
│   HTTP API   │────▶│  StoryboardCommandService (Write Side)          │
│  (Ktor)      │     │  ┌─────────────────────────────────────────┐    │
│              │     │  │  AtomicWriteRepository (Unit of Work)   │    │
└──────────────┘     │  │  ┌─────────┬──────────┬──────────────┐  │    │
                     │  │  │Version │Event Log │   Outbox     │  │    │
                     │  │  │ Bump   │ Append   │   Insert     │  │    │
                     │  │  └─────────┴──────────┴──────────────┘  │    │
                     │  │  ONE PostgreSQL TRANSACTION (原子)       │    │
                     │  └─────────────────────────────────────────┘    │
                     └─────────────────────────────────────────────────┘
                                          │
                                          ▼
                              ┌─────────────────────┐
                              │  Outbox Publisher   │───▶ Redpanda/Kafka
                              │  (事务性发件箱模式)    │    (storyboard-events)
                              └─────────────────────┘
                                          │
                                          ▼
                     ┌─────────────────────────────────────────────────┐
                     │  EventConsumer (Read Side)                      │
                     │  ┌──────────────────────────────────────────┐   │
                     │  │ 1. Persistent offset dedup (PostgreSQL)   │   │
                     │  │ 2. Out-of-order buffering (sorted map)   │   │
                     │  │ 3. Idempotent version tracking (DB)      │   │
                     │  │ 4. Dead letter queue w/ exponential retry │   │
                     │  └──────────────────────────────────────────┘   │
                     └─────────────────────────────────────────────────┘
                                          │
                                          ▼
                     ┌─────────────────────────────────────────────────┐
                     │  EventProcessor → RiskAnalysisEngine            │
                     │  ┌──────────────────────────────────────────┐   │
                     │  │  Full analysis: all rules × all segments  │   │
                     │  │  Incremental: dirty window only (±60s)    │   │
                     │  │  Drift detector: periodic incremental vs  │   │
                     │  │  full comparison with auto-repair         │   │
                     │  └──────────────────────────────────────────┘   │
                     └─────────────────────────────────────────────────┘
                                          │
                                          ▼
                              ┌─────────────────────┐
                              │ Risk Projections    │
                              │ (PostgreSQL)        │
                              └─────────────────────┘
```

## 五类风险规则

| 规则 ID | 检测内容 | 默认阈值 | 严重程度 |
|---|---|---|---|
| `EXCESSIVE_REVERSALS_IN_WINDOW` | 任意 60 秒滑动窗口内强反转次数过多 | > 6 次 | MEDIUM/HIGH |
| `CONSECUTIVE_HIGH_STIMULUS` | 连续 N 段刺激强度达到最高值（5） | ≥ 3 段 | MEDIUM/HIGH |
| `SHOT_DURATION_TOO_SHORT` | 窗口内平均镜头时长过短 | < 3.0 秒 | MEDIUM/HIGH |
| `SCROLL_INDUCEMENT_PRESENT` | 存在继续下滑诱导元素 | 标记即触发 | LOW |
| `MISSING_BUFFER_BETWEEN_KNOWLEDGE_POINTS` | 两个知识点之间缺少低刺激缓冲段 | 需 ≤ 强度 2 且 ≥ 1s | MEDIUM |

> ⚠️ **重要声明**：所有风险建议仅涉及视频节奏和内容结构优化，不做任何成瘾性诊断或脑损伤预测。

## 关键设计特性

### 1. 原子事务写入（All-or-Nothing）

所有写操作（版本号更新 + 事件日志追加 + Outbox 写入）在**单个 PostgreSQL 事务**内完成，任何一步失败都会导致完整回滚，绝不会留下部分状态。

```kotlin
// AtomicWriteRepository.appendEventAndOutboxAtomically()
// 1. 检查 optimistic lock (current_version == expectedVersion)
// 2. UPDATE storyboards SET current_version = newVersion
// 3. INSERT INTO event_log (...)
// 4. INSERT INTO outbox_events (...)
// 全部在同一个 newSuspendedTransaction {} 中
```

### 2. 持久化幂等去重

消费者重启或 rebalance 后不会重复处理消息，两层持久化去重：
- **Offset 级别**：`consumer_processed_offsets` 表记录 `(consumer_group, topic, partition, offset)`
- **版本级别**：`consumer_storyboard_versions` 表记录每个 storyboard 已处理的最高版本号

```kotlin
// EventConsumer.processRecord():
// 1. 检查 atomicWriteRepo.isOffsetProcessed(group, topic, partition, offset)
// 2. 检查 atomicWriteRepo.getProcessedVersion(group, storyboardId) >= event.version
// 3. 处理成功后 saveProcessedOffset() + saveProcessedVersion()
```

### 3. 时间轴连续无重叠校验

写入层在插入新片段前强制校验时间轴：
```kotlin
// AtomicWriteRepository.validateTimelineContinuous()
// 查询现有片段，检测：
// - 重叠 (next.startTime < current.endTime) → 拒绝
// - 间隙（允许，不强制连续）
```

### 4. 增量分析与全量等价

增量算法保证结果与全量重算**逐项一致**：

1. 确定变更影响范围 `dirtyRange = [changeStart - 60s, changeEnd + 60s]`
2. 加载**全部**片段（滑动窗口需要边界上下文）
3. 对 dirtyRange 范围内的窗口运行所有规则
4. 从旧投影中保留完全在 dirtyRange 外的 findings
5. 合并并去重：`result = old[outside dirtyRange] + new[inside dirtyRange]`

**正确性证明**：
- dirtyRange 外的片段未发生变化 → 相关 findings 不受影响
- dirtyRange 内的所有窗口被全量重算 → 等同于全量分析在该区域的结果
- 周期性 drift checker 自动比对增量投影与全量结果，发现漂移自动修复

### 5. 事件乱序缓冲

使用基于版本号的 TreeMap 缓冲乱序事件：
- 收到 v3 但 v1 未到 → v3 进入 buffer
- v1 到达后按序释放 v1, v2（如已在 buffer）, v3
- 在 buffer 中的版本超过 1000 个时告警（可能有消息丢失）

### 6. 死信重放

处理失败的消息进入 `dead_letters` 表：
- 指数退避重试：30s → 60s → 120s（最大 3 次）
- 超过重试上限后标记 resolved = false
- 可通过管理接口手动重放到 Kafka

### 7. 百万级流式导入

- 基于 Kotlin Flow 的背压流式处理
- 批次大小可配置（默认 1000 条/批）
- 检查点持久化（`import_jobs.checkpoint`）
- 支持中途取消和断点恢复

## API 端点

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/v1/storyboards` | 创建分镜（含原版知识点） |
| GET | `/api/v1/storyboards/{id}` | 查询分镜元信息 |
| GET | `/api/v1/storyboards/{id}/segments` | 查询所有片段 |
| POST | `/api/v1/storyboards/{id}/segments` | 添加片段（需 expectedVersion） |
| PUT | `/api/v1/storyboards/{id}/segments/{segId}` | 更新片段 |
| DELETE | `/api/v1/storyboards/{id}/segments/{segId}` | 删除片段 |
| POST | `/api/v1/storyboards/{id}/segments:reorder` | 重排序 |
| POST | `/api/v1/storyboards/{id}/segments:batchImport` | 批量导入 |
| POST | `/api/v1/storyboards/{id}/import:stream` | 百万级流式导入 |
| POST | `/api/v1/storyboards/{id}/import/{jobId}:cancel` | 取消导入任务 |
| GET | `/api/v1/storyboards/{id}/import/{jobId}` | 查询导入进度 |
| GET | `/api/v1/storyboards/{id}/risks` | 查询风险分析结果 |
| POST | `/api/v1/storyboards/{id}/risks:rebuild` | 请求增量重建 |
| POST | `/api/v1/storyboards/{id}/risks:rebuildFull` | 从零重建投影 |
| POST | `/api/v1/storyboards/{id}/risks:verify` | 增量/全量等价性校验（漂移检测） |
| GET | `/api/v1/storyboards/{id}/knowledge-points:validate` | 知识点完整性校验 |
| GET | `/health` | 健康检查 |

### 请求示例

```bash
# 创建分镜
curl -X POST http://localhost:8080/api/v1/storyboards \
  -H "Content-Type: application/json" \
  -d '{"externalId": "video-001", "title": "科普视频", "originalKnowledgePoints": ["kp1", "kp2"]}'

# 添加片段（乐观锁：expectedVersion = 1）
curl -X POST http://localhost:8080/api/v1/storyboards/{id}/segments \
  -H "Content-Type: application/json" \
  -d '{
    "expectedVersion": 1,
    "segment": {
      "order": 0, "startTimeMs": 0, "endTimeMs": 3000,
      "stimulusIntensity": 4, "hasReversal": true,
      "isKnowledgePoint": false
    }
  }'

# 查询风险分析
curl http://localhost:8080/api/v1/storyboards/{id}/risks
```

### 风险分析响应示例

```json
{
  "storyboardId": "...",
  "ruleVersion": "1.0.0",
  "projectionVersion": 15,
  "computedAt": 1721800000000,
  "totalRiskScore": 22.0,
  "findingCount": 4,
  "findings": [
    {
      "ruleId": "EXCESSIVE_REVERSALS_IN_WINDOW",
      "ruleName": "窗口内强反转过多",
      "severity": "MEDIUM",
      "windowStartMs": 0,
      "windowEndMs": 60000,
      "affectedSegmentIds": ["..."],
      "evidence": {
        "description": "在 60 秒窗口内检测到 7 次强反转，超过阈值 6 次",
        "metrics": {"reversalCount": 7.0, "threshold": 6.0},
        "segmentDetails": [...]
      },
      "suggestion": "建议在该时间段内减少强反转次数至 6 次以内..."
    }
  ]
}
```

## 快速开始

### 使用 Docker Compose

```bash
# 启动 PostgreSQL + Redpanda + App
docker-compose up -d

# 等待服务就绪
docker-compose logs -f app

# API 在 http://localhost:8080
# Redpanda Console 在 http://localhost:8081
```

### 本地开发

**前置条件**：
- JDK 17+
- Docker（运行集成测试需要）

```bash
# 编译
export JAVA_HOME=/path/to/jdk17
gradle build -x test

# 运行单元测试（无 Docker 依赖）
gradle test --tests "com.calmcut.domain.rules.*" --tests "com.calmcut.service.*" -i

# 运行集成测试（需要 Docker）
gradle test --tests "com.calmcut.integration.*" -i

# 启动服务（需先启动 PostgreSQL 和 Redpanda）
gradle run
```

### 数据库迁移

Flyway 在应用启动时自动执行：
- `V1__initial_schema.sql`：初始表结构
- `V2__consumer_dedup.sql`：消费者幂等去重表

## 配置

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `database.jdbcUrl` | - | PostgreSQL JDBC URL |
| `database.username` | - | 数据库用户名 |
| `database.password` | - | 数据库密码 |
| `database.maximumPoolSize` | 20 | HikariCP 连接池大小 |
| `redpanda.bootstrapServers` | localhost:9092 | Kafka/Redpanda 地址 |
| `redpanda.consumerGroupId` | storyboard-risk-worker | 消费者组 ID |
| `redpanda.topicEvents` | storyboard-events | 事件主题名 |
| `analysis.windowSizeSeconds` | 60 | 滑动窗口大小（秒） |
| `analysis.maxReversalsPerWindow` | 6 | 单窗口最大反转次数 |
| `analysis.consecutiveHighStimulus` | 3 | 连续高刺激阈值 |
| `analysis.highStimulusThreshold` | 5 | 高刺激强度阈值 |
| `analysis.minAverageShotSeconds` | 3.0 | 最低平均镜头时长（秒） |
| `analysis.bufferStimulusThreshold` | 2 | 缓冲段最高刺激强度 |
| `analysis.driftCheckIntervalSeconds` | 3600 | 漂移检测周期（秒） |
| `import.batchSize` | 1000 | 每批导入片段数 |

## 测试覆盖

### 单元测试（38 tests）

| 测试类 | 覆盖内容 |
|---|---|
| `RiskRulesTest` | 五类规则的正/反向检测、边界条件、空输入、医学术语屏蔽 |
| `IncrementalEquivalenceTest` | 增量/全量等价、乱序缓冲、重复事件去重、顺序事件直通 |
| `WorkerRecoveryTest` | 幂等性、确定性重建、死信重试/放弃、检查点恢复、乐观锁、知识点校验、时间轴验证 |
| `LargeScaleImportTest` | 10 万段性能、流式取消/恢复、证据完整性 |

### 集成测试（10 tests，Testcontainers）

| 测试 | 验证内容 |
|---|---|
| 原子事务 | version + event_log + outbox 在同一事务中全成功 |
| 乐观锁 | 并发修改被正确拒绝（409） |
| 时间轴校验 | 重叠片段被写入层拒绝 |
| 幂等处理 | 重复事件不产生重复数据 |
| 持久化去重 | 重启安全（版本持久化到 DB） |
| 投影重建 | 从零重建产生确定性结果 |
| 漂移检测 | 30 次增量变更后与全量一致 |
| 知识点完整性 | 缺失/多余知识点检测 |
| 风险规则检测 | 全规则触发 + 证据 + 无医学诊断 |
| 死信队列 | 失败事件可查询 |

## 目录结构

```
src/main/kotlin/com/calmcut/
├── domain/
│   ├── Models.kt                    # 领域模型
│   ├── events/Events.kt             # 事件定义
│   └── rules/RiskRules.kt           # 五类风险规则引擎
├── infrastructure/
│   ├── db/
│   │   ├── Tables.kt                # Exposed 表定义
│   │   └── DatabaseFactory.kt      # HikariCP + Flyway
│   ├── messaging/
│   │   ├── KafkaProducer.kt         # Outbox 发布器
│   │   ├── KafkaConsumer.kt         # 消费者（乱序缓冲+幂等+DLQ）
│   │   ├── DeadLetterReplayer.kt    # 死信重放
│   │   └── ProjectionDriftChecker.kt # 漂移检测
│   └── repository/
│       ├── AtomicWriteRepository.kt # 原子工作单元（核心）
│       ├── StoryboardRepository.kt  # 分镜/片段 CRUD
│       ├── EventLogRepository.kt    # 事件日志 + Outbox 查询
│       ├── RiskProjectionRepository.kt
│       ├── ImportRepository.kt
│       └── DeadLetterRepository.kt
├── service/
│   ├── StoryboardCommandService.kt  # 写入命令服务
│   ├── StoryboardQueryService.kt    # 查询服务
│   ├── RiskAnalysisEngine.kt        # 增量/全量分析 + 等价性验证
│   ├── EventProcessor.kt            # 事件投影处理器
│   ├── KnowledgePointValidator.kt   # 知识点完整性校验
│   └── StreamingImportService.kt    # 百万级流式导入
├── api/
│   ├── Routes.kt                    # HTTP 路由
│   └── dto/Dtos.kt                  # 请求/响应 DTO
└── Application.kt                   # Ktor 启动入口 + 依赖装配
```
