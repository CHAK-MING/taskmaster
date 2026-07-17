# 可观测性

DAGForge 将可观测性分为三个互补平面：Prometheus Metrics 用于趋势、SLO、容量和告警；Run Snapshot、Failure Report、Evidence 与 Artifact 用于单次执行审计；结构化日志和 TraceContext 用于跨服务关联。Metrics 不承担单次 Run 调试，Evidence 也不承担跨 Run 聚合。

该设计遵循 Prometheus 的命名、基础单位和标签基数规范，并参考 OpenTelemetry 对稳定 `error.type`、错误记录一致性和高基数属性的约束。Kubernetes 探针按 liveness 与 readiness 的不同职责拆分，避免把暂时过载或外部依赖故障误判为需要重启进程。

## 快速部署

Prometheus 告警规则位于 [`templates/prometheus-rules.yml`](templates/prometheus-rules.yml)，Grafana Dashboard 位于 [`templates/grafana-dashboard.json`](templates/grafana-dashboard.json)，Kubernetes 探针示例位于 [`templates/kubernetes-probes.yaml`](templates/kubernetes-probes.yaml)。所有资产由 `python3 scripts/check-observability-assets.py` 校验，确保查询只引用实际导出的指标并阻止高基数身份进入 Prometheus 标签。

Prometheus 抓取地址：

```text
GET /metrics
```

默认 Grafana Dashboard 的数据源变量名为 `DS_PROMETHEUS`。导入时选择抓取 DAGForge 的 Prometheus 数据源即可。

## 健康探针

`GET /api/health` 是 liveness。只要服务进程和 HTTP 路由仍可响应就返回 `200`，单个 Workflow 失败、重试、外部 HTTP 依赖故障或容量暂时饱和不会让 liveness 失败。

`GET /api/ready` 是 readiness。只有 Runtime 已启动、Workflow Runtime 可以接受新 Run、配置要求的持久化目录锁仍由当前实例持有且 API 已进入服务状态时才返回 `200`；否则返回 `503`。示例响应：

```json
{
  "status": "ready",
  "components": {
    "runtime": "ready",
    "workflow": "ready",
    "storage": "ready",
    "api": "ready"
  }
}
```

`dagforge_ready` 与 readiness 使用同一判定，适合告警和部署状态面板。

## 标签基数契约

Prometheus 中的每个标签组合都会创建独立时序。DAGForge 指标只使用稳定枚举标签：

- `result`：`succeeded`、`failed`、`cancelled`、Task/Attempt 的其他有限终态。
- `error_type`：DAGForge `Error` 域中的稳定值，例如 `timeout`、`protocol_error`、`persistence_error`。成功样本不携带该标签。
- `executor_class`：`command`、`http`、`other`。自定义 executor 不直接成为标签值。
- `store`：`checkpoint`、`evidence`、`artifact`。
- `operation`：`write`、`append`。
- `decision`：`reused`、`invalidated`。

以下身份和自由文本禁止作为默认指标标签：

```text
run_id workflow_id plan_id node_id attempt_id artifact_id
trace_id span_id principal idempotency_key error_message
```

这些字段保留在 Run API、Evidence、Failure Report、Artifact、结构化日志或 Trace 中。这样可以从聚合异常下钻到具体 Run，同时避免 Prometheus 时序数量随业务运行量无界增长。

`StartRunRequest.trace.trace_id` 和 `trace.parent_span_id` 会传递给 executor，并写入 `trigger_received` Evidence。调用方可以使用同一 trace identity 关联上游请求、DAGForge Run 和下游服务，而无需把 trace identity 放进指标标签。

## 指标目录

### 服务和 API

| 指标 | 类型 | 含义 |
|---|---|---|
| `dagforge_ready` | Gauge | 实例是否可以安全接收流量 |
| `dagforge_runtime_running` | Gauge | Runtime 是否运行 |
| `dagforge_runtime_shards` | Gauge | Runtime shard 数量 |
| `dagforge_http_active_requests` | Gauge | 当前活动 HTTP 请求 |
| `dagforge_http_requests_total{method,endpoint,status}` | Counter | HTTP 请求终态数量 |
| `dagforge_http_request_duration_seconds{endpoint}` | Histogram | HTTP 请求耗时 |

### Run

| 指标 | 类型 | 标签 |
|---|---|---|
| `dagforge_workflow_runs_total` | Counter | `result`、失败时的 `error_type` |
| `dagforge_workflow_run_duration_seconds` | Histogram | `result`、失败时的 `error_type` |
| `dagforge_workflow_runs_active` | Gauge | 无 |
| `dagforge_workflow_runs_paused` | Gauge | 无 |
| `dagforge_workflow_runs_stopping` | Gauge | 无 |

`dagforge_workflow_active_runs` 是兼容旧客户端的弃用别名，新查询应使用 `dagforge_workflow_runs_active`。

### Task 与 Attempt

| 指标 | 类型 | 标签 |
|---|---|---|
| `dagforge_workflow_tasks_total` | Counter | `executor_class`、`result`、失败时的 `error_type` |
| `dagforge_workflow_task_duration_seconds` | Histogram | 同上 |
| `dagforge_workflow_task_queue_duration_seconds` | Histogram | `executor_class` |
| `dagforge_workflow_tasks_active` | Gauge | `executor_class` |
| `dagforge_workflow_tasks_ready` | Gauge | 无 |
| `dagforge_workflow_tasks_retry_waiting` | Gauge | 无 |
| `dagforge_workflow_attempts_total` | Counter | `executor_class`、`result`、失败时的 `error_type` |
| `dagforge_workflow_attempt_duration_seconds` | Histogram | 同上 |
| `dagforge_workflow_attempts_active` | Gauge | `executor_class` |
| `dagforge_workflow_retries_total` | Counter | `executor_class` |

Task queue duration 从 Task 首次 Ready 到对应 Attempt 开始执行。重试等待属于同一个 Task 的执行历史，不会伪造额外 Task queue 样本；重试压力通过 `dagforge_workflow_retries_total` 和 `dagforge_workflow_tasks_retry_waiting` 展示。

### Repair Run

| 指标 | 类型 | 标签 |
|---|---|---|
| `dagforge_workflow_repair_runs_total` | Counter | `result`、失败时的 `error_type` |
| `dagforge_workflow_repair_run_duration_seconds` | Histogram | 同上 |
| `dagforge_workflow_repair_nodes_total` | Counter | `decision` |

具体 Node 的复用或失效原因仍通过 Repair Run 响应及 `task_reused`、`task_invalidated` Evidence 查询。

### 持久化完整性

| 指标 | 类型 | 标签 |
|---|---|---|
| `dagforge_workflow_persistence_operations_total` | Counter | `store`、`operation`、`result`、失败时的 `error_type` |
| `dagforge_workflow_persistence_operation_duration_seconds` | Histogram | 同上 |
| `dagforge_workflow_durability_deferred_total` | Counter | `store`、`operation` |

`result="deferred"` 表示逻辑内容已经可见，但父目录同步没有确认崩溃持久性。它与普通失败不同，但仍应告警并检查底层文件系统。

## 告警策略

默认规则优先告警用户可感知症状，而不是每个内部瞬态：目标不可抓取、实例长期不 Ready、Run 失败率、尾延迟、重试风暴、Task queue 堵塞、Attempt 超时、持久化失败、durability deferred 和长期 stopping。失败率和重试比例包含最小样本门槛，避免低流量环境中单次失败造成噪声。

规则中的延迟阈值是可运行默认值，不是所有业务的最终 SLO。生产部署应根据 Workflow 的业务时限调整，并优先使用 p95/p99 分布而不是平均值。

## 问题下钻

推荐排查顺序：

1. 从 Dashboard 或告警确认异常属于可用性、错误率、延迟、饱和度还是持久化完整性。
2. 通过 `/api/status` 判断 Runtime 和活动 Run 总量，通过 Run 列表或触发方保存的 Run ID 找到受影响执行。
3. 查询 Run Snapshot、`/failures` 和 `/evidence`，定位失败 Task、Attempt 次数、稳定 `failure.code`、下游跳过原因和已保留输出。
4. 大型失败详情或业务审计包通过 Artifact API 下载。
5. 修正 Plan 或外部依赖后创建 Repair Run，并核对 `task_reused`、`task_invalidated` Evidence 与 Repair 指标。

## 技术成功与业务成功

Run `succeeded` 表示 Workflow 按声明完成，不一定表示订单、授信或其他业务结果成功。例如补偿路径可能正确执行并让 Run 成功，但业务结果是 `compensated`。业务 Plan 应发布稳定的 `business_outcome` 和 `reason_code`，由业务监控系统聚合；不要把自由变化的业务实体或原因直接塞进 DAGForge 基础指标标签。

## 规范来源

- [Prometheus metric and label naming](https://prometheus.io/docs/practices/naming/)
- [OpenTelemetry metrics semantic conventions](https://opentelemetry.io/docs/specs/semconv/general/metrics/)
- [OpenTelemetry recording errors](https://opentelemetry.io/docs/specs/semconv/general/recording-errors/)
- [Kubernetes liveness, readiness, and startup probes](https://kubernetes.io/docs/concepts/configuration/liveness-readiness-startup-probes/)
