# TaskMaster

[English](README.md)

基于现代 C++23 构建的高性能 DAG 工作流编排器，设计灵感来自 Apache Airflow。

![TaskMaster UI](image/web-ui.png)

## 特性

- **DAG 工作流** - 使用 YAML 定义任务依赖
- **触发规则** - Airflow 风格的条件执行（all_success、all_failed、one_success 等）
- **传感器** - 轮询等待条件满足后继续（文件存在、HTTP 端点、命令）
- **分支执行** - 基于 XCom 值的条件任务执行
- **回填** - 自动补齐错过的计划运行
- **XCom** - 跨任务通信，支持模板变量
- **调度** - 基于单调时钟的防漂移 Cron 调度
- **执行器** - Shell、Docker、Sensor 三种执行模式
- **Web UI** - React Flow 实时 DAG 可视化
- **REST API** - 完整的控制和监控接口
- **持久化** - SQLite 存储，支持基于水位线的崩溃恢复（至少一次语义）

## 快速开始

### 编译

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

### 运行

```bash
./build/bin/taskmaster serve -c system_config.yaml
# Web UI: http://localhost:8888
```

### 构建 Web UI（可选）

```bash
cd web-ui && npm install && npm run build
```

## DAG 配置

DAG 文件放在 `dags/` 目录。文件名即为 DAG ID（如 `etl.yaml` → `etl`）。

### 基本结构

```yaml
name: ETL 数据管道
description: 每日数据处理
cron: "0 2 * * *"
start_date: "2024-01-01"
catchup: true

tasks:
  - id: extract
    command: ./extract.sh --date {{ds}}
    
  - id: transform
    command: ./transform.sh
    dependencies: [extract]
    trigger_rule: all_success
    
  - id: load
    command: ./load.sh
    dependencies: [transform]
```

### 调度字段

| 字段 | 说明 |
|------|------|
| `cron` | Cron 表达式，启用自动调度 |
| `start_date` | 最早调度日期（YYYY-MM-DD） |
| `end_date` | 最晚调度日期（YYYY-MM-DD） |
| `catchup` | 若为 true，自动回填 start_date 以来的所有错过运行 |

### 任务字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | string | 任务唯一标识 |
| `command` | string | 执行命令（支持模板变量） |
| `executor` | string | `shell`（默认）、`docker` 或 `sensor` |
| `dependencies` | array | 依赖的任务 ID 列表 |
| `trigger_rule` | string | 基于上游状态决定何时运行 |
| `is_branch` | bool | 若为 true，XCom 输出决定执行哪些下游任务 |
| `timeout` | int | 超时秒数（默认 300） |
| `max_retries` | int | 失败重试次数（默认 3） |

### 触发规则

| 规则 | 说明 |
|------|------|
| `all_success` | 所有上游任务成功时运行（默认） |
| `all_failed` | 所有上游任务失败时运行 |
| `all_done` | 所有上游任务完成时运行（无论成功或失败） |
| `one_success` | 至少一个上游任务成功时运行 |
| `one_failed` | 至少一个上游任务失败时运行 |
| `none_failed` | 没有上游任务失败时运行（允许跳过） |
| `none_skipped` | 没有上游任务被跳过时运行 |

### 模板变量

可在 `command` 字段中使用：

| 变量 | 示例 | 说明 |
|------|------|------|
| `{{ds}}` | 2024-01-15 | 执行日期（YYYY-MM-DD） |
| `{{ds_nodash}}` | 20240115 | 无横线的执行日期 |
| `{{execution_date}}` | 2024-01-15T00:00:00Z | 完整 ISO 时间戳 |
| `{{dag_id}}` | etl_pipeline | DAG 标识 |
| `{{task_id}}` | extract | 当前任务标识 |
| `{{run_id}}` | etl_pipeline_20240115 | DAG 运行标识 |

### 分支执行

分支任务返回要执行的任务 ID 的 JSON 数组：

```yaml
tasks:
  - id: check_data
    command: |
      if [ -f /data/large.csv ]; then
        echo '["process_large"]'
      else
        echo '["process_small"]'
      fi
    is_branch: true
    xcom_push:
      - key: branch
        source: json

  - id: process_large
    command: ./process_large.sh
    dependencies: [check_data]

  - id: process_small
    command: ./process_small.sh
    dependencies: [check_data]
```

### 传感器

传感器轮询直到条件满足：

```yaml
tasks:
  - id: wait_for_file
    executor: sensor
    sensor_type: file
    sensor_path: /data/input.csv
    sensor_interval: 30
    timeout: 3600

  - id: wait_for_api
    executor: sensor
    sensor_type: http
    sensor_url: http://api.example.com/ready
    sensor_expected_status: 200

  - id: wait_for_db
    executor: sensor
    sensor_type: command
    command: pg_isready -h localhost
```

| 传感器类型 | 必填字段 |
|------------|----------|
| `file` | `sensor_path` |
| `http` | `sensor_url`、`sensor_expected_status` |
| `command` | `command`（退出码 0 表示就绪） |

### XCom

任务间传递数据：

```yaml
tasks:
  - id: producer
    command: echo '{"count": 42, "status": "ok"}'
    xcom_push:
      - key: result
        source: json

  - id: consumer
    command: echo "处理 $COUNT 条记录"
    dependencies: [producer]
    xcom_pull:
      - key: result
        from: producer
        env: COUNT
        json_path: count
```

**推送来源：** `stdout`、`stderr`、`exit_code`、`json`

### Docker 执行器

```yaml
tasks:
  - id: python_task
    executor: docker
    docker_image: python:3.11
    command: python -c "print('Hello from Docker')"
```

## 系统配置

`system_config.yaml`：

```yaml
scheduler:
  log_level: info
  tick_interval_ms: 1000
  max_concurrency: 10

storage:
  db_file: taskmaster.db

api:
  enabled: true
  host: 127.0.0.1
  port: 8888

dag_source:
  directory: ./dags
  scan_interval_sec: 60
```

## CLI 命令

```bash
taskmaster serve -c config.yaml    # 启动服务
taskmaster run --db db.sqlite dag  # 检查 DAG 是否存在
taskmaster list --db db.sqlite     # 列出所有 DAG
taskmaster validate -c config.yaml # 验证 DAG 文件
taskmaster status --db db.sqlite dag [--run id]  # 显示历史
```

## REST API

| 方法 | 端点 | 说明 |
|------|------|------|
| GET | `/api/health` | 健康检查 |
| GET | `/api/status` | 系统状态 |
| GET | `/api/dags` | DAG 列表 |
| GET | `/api/dags/:id` | DAG 详情 |
| POST | `/api/dags/:id/trigger` | 触发运行 |
| GET | `/api/history` | 运行历史 |
| GET | `/api/runs/:runId/tasks/:taskId/logs` | 任务日志 |

## 环境要求

- C++23（GCC 13+ / Clang 17+）
- CMake 3.25+
- Linux + io_uring

## 许可证

MIT
