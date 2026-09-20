# TaskForge · 异步任务优先级队列与死信重试引擎

一套可独立运行的任务编排服务：外部通过 HTTP 提交任务，引擎按五级优先级 +
权重公平 + Critical/High 抢占调度，交给 Worker 池执行，失败按指数退避 / 固定
间隔 / Cron 重试，重试耗尽进入死信区，并支持 DAG 依赖编排。队列深度、吞吐、
Worker 状态、死信积压通过 Nuxt 3 管理面板实时可见（SSE）。

## 一条命令启动

```bash
docker compose up --build
```

启动后：

| 服务 | 地址 |
| --- | --- |
| 管理面板（前端） | http://localhost:3000 |
| 后端 API | http://localhost:8080/api |
| 健康检查 | http://localhost:8080/api/health |
| PostgreSQL | localhost:5432 (taskforge/taskforge) |
| Redis | localhost:6379 |

首次启动后端会自动执行数据库迁移（`internal/store/schema.sql`）。

## 快速制造流量

```bash
# 普通成功任务（内置 echo/sleep/compute 处理器，每个类型注册了两个处理器做负载均衡）
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
  -d '{"type":"compute","payload":{"a":6,"b":7,"op":"mul"},"priority":"high"}'

# 延迟 5 秒执行
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
  -d '{"type":"echo","payload":{"x":1},"priority":"normal","delay_seconds":5}'

# 固定间隔重试、最多 2 次（flaky 前两次失败）→ 观察重试后成功
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
  -d '{"type":"flaky","payload":{"fail_first_n":2},"max_retries":3,
       "retry_policy":{"kind":"fixed","base_interval_seconds":1,"max_retries":3}}'

# 必失败任务，重试耗尽进入死信区
curl -X POST localhost:8080/api/tasks -H 'Content-Type: application/json' \
  -d '{"type":"always-fail","payload":{},"max_retries":2,
       "retry_policy":{"kind":"exponential","base_interval_seconds":1,"max_retries":2}}'
```

更多示例见 `scripts/seed.sh`。

## 调度语义（已被测试锁定）

- **五级严格优先级**：critical → high → normal → low → bulk，各自独立就绪队列。
- **权重公平，不饿死低优先级**：连续消费 `FAIRNESS_THRESHOLD`（默认 5）次
  critical/high 后，强制插入一次 normal/low/bulk 的消费机会；低优先级为空时
  立即放行高优先级并重置计数。公平窗口获得执行的低优先级任务受保护，不会被
  随后到来的高优先级任务无限抢占。
- **抢占**：critical/high 等待且槽位全满时，可打断正在运行的 normal 及以下
  任务；被打断任务回到其队列**队首**，槽位立即释放给高优先级任务，任务不
  丢失、稍后会被重新取到。正在运行的 critical/high 永不被抢占。
- **秒级延迟**：延迟任务在 Redis ZSET 中按到期时间排序，默认每 250ms 扫描，
  到点转入对应优先级就绪队列。
- **原子状态流转**：PostgreSQL 是唯一事实源，所有 ready→running→终态转换都在
  事务内做 `state` 条件更新（CAS）；Redis 仅存调度态。晚到的执行结果若发现
  任务已不在 running（被抢占/回收）会被丢弃，杜绝重复执行与状态覆盖。
- **Worker 心跳与租约**：Worker 周期性心跳，超过 `WORKER_TIMEOUT` 判失联，
  其在执行任务由 PostgreSQL 直接读取并按队首重入队；执行期间续租
  （`LEASE_TTL`），续租失败按超时回收。优雅关闭后不再接新任务、等待手中
  任务跑完。

## 重试与死信

- 三种策略：`exponential`（base × 2^已重试次数）、`fixed`、`cron`
  （robfig/cron 标准五段式），均有最大次数上限。
- 死信区：查看详情（含每次尝试的错误类型/信息与完整执行时间线）、按错误
  类型聚合统计、单条或批量重试（队首重入队）、批量丢弃。

## DAG 编排

- 定义节点与 `depends_on` 前驱集合，例如 `A → (B,C) → D`；根节点先执行，
  前驱全部成功（或按策略跳过）后解锁后继。
- 节点失败三种处置：终止整个 DAG、跳过失败节点继续、重试失败节点（节点级
  最大次数）。
- 提交前做三染色 DFS 环检测，含环直接 422 拒绝且不持久化、不入队。
- 面板按拓扑层级渲染前驱/后继并按节点状态着色。

## 监控

`GET /api/metrics` 返回各队列实时深度、延迟区大小、每秒完成数、各优先级成功
/失败率、平均执行与调度延迟、Worker 利用率、死信积压及 5 分钟趋势；`GET
/api/events` 通过 SSE 每秒推送同一快照供大盘实时刷新。所有状态变更写
`audit_events`，可在任务详情中查看。

## 目录结构

```
backend/
  cmd/server/            程序入口（依赖等待、迁移、启动、优雅关闭）
  internal/
    config/              环境变量配置
    clock/               时钟抽象（真实时钟 + 测试用 FakeClock）
    domain/              核心领域模型
    queue/               队列抽象：redis 实现 + memory 实现（测试）
    store/               持久化抽象：PostgreSQL（事实源）+ memory（测试）
    processor/           处理器注册表（同类型多处理器轮询负载均衡）
    workerpool/          Worker 池：槽位预留、抢占、优雅关闭
    retry/               三种重试策略的下次执行时间计算
    dag/                 环检测、拓扑分层、运行时编排
    metrics/             秒级环形桶指标采集
    engine/              调度器/延迟/抢占/执行/续租/回收/死信/DAG 钩子
    httpapi/             Fiber 路由、DTO、REST + SSE
    demo/                内置示例处理器
frontend/
  lib/api/               接口封装（client / endpoints / 类型）
  stores/                状态管理（metrics SSE、toast）
  components/            徽章、指标卡、折线图、任务详情、提交/DAG 编辑器等
  pages/                 总览 / 任务 / 死信 / DAG / Worker
docker-compose.yml       postgres + redis + backend + frontend
```

## 自动化测试

```bash
cd backend
go test ./...
```

关键行为测试（`internal/engine/engine_behavior_test.go`）：

1. `TestFairnessLowPriorityNotStarved` — 高优先级持续到来时低优先级仍按配置
   节奏被消费（断言实际派发序列 `hi hi hi lo …`）。
2. `TestCriticalPreemptsNormalAndRequeuesAtHead` — Critical 抢占 normal，被打断
   任务回队首、不丢失、稍后重跑成功。
3. `TestDelayedTaskPromotedSecondLevel` — 延迟任务到点秒级入队。
4. `TestWorkerLossRequeuesInflightTask` — Worker 失联后在执行任务被回收重入
   队并由存活 Worker 跑完。
5. `TestRetriesExhaustedGoesDead` — 固定间隔重试耗尽进死信，保留每次尝试与
   错误类型。
6. `TestDeadBatchRetryReenqueues` — 死信批量重试重新入队并成功。
7. `TestDAGDependencyOrder` / `TestDAGCycleRejected` — A→(B,C)→D 顺序正确、
   环被拒绝；另含 `internal/dag` 与 `internal/retry` 单元测试。
8. `TestConcurrentSubmitNoLossNoDuplication` — 200 个并发提交、五优先级混合，
   不丢不重且全部成功。

## 配置

所有参数见根目录 `.env.example`（compose 中直接以环境变量给出），包括 Worker
数、每 Worker 槽位、公平阈值、调度/延迟扫描/心跳间隔、Worker 失联时限、租约
TTL 与优雅关闭超时。
