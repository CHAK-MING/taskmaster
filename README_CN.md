# TaskMaster

[English](README.md)

基于现代 C++23 的高性能 DAG 任务调度器。

![TaskMaster UI](image/web-ui.png)

## 特性

- DAG 任务依赖管理
- 标准 Cron 表达式定时调度
- **XCom（跨任务通信）** - 在任务间传递数据
- Web UI 可视化管理（React Flow）
- REST API 监控与控制
- SQLite 持久化，支持崩溃恢复

## 快速开始

### 编译

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

### 运行

```bash
# 启动服务（含调度器和 Web UI）
./build/bin/taskmaster serve -c system_config.yaml

# 访问 http://localhost:8888
```

### 构建 Web UI（可选）

```bash
cd web-ui
npm install && npm run build
```

## CLI 命令参考

### 命令

#### `serve` - 启动 TaskMaster 服务

```bash
taskmaster serve -c system_config.yaml
```

启动调度器和 API 服务器。需要 `-c, --config` 指定系统配置文件。

#### `run` - 触发 DAG 运行

```bash
taskmaster run --db taskmaster.db <dag_id>
```

检查 DAG 是否存在于数据库中。实际触发请使用 API 端点。

#### `list` - 列出所有 DAG

```bash
taskmaster list --db taskmaster.db
```

列出数据库中存储的所有 DAG。

#### `validate` - 验证 DAG 文件

```bash
taskmaster validate -c system_config.yaml
```

验证配置目录中的 DAG YAML 文件。

#### `status` - 显示 DAG 运行历史

```bash
taskmaster status --db taskmaster.db <dag_id>
taskmaster status --db taskmaster.db <dag_id> --run <run_id>
```

显示 DAG 的运行历史或特定运行的详情。

## 项目结构

```
TaskMaster/
├── system_config.yaml    # 系统配置文件
├── dags/                 # DAG 定义文件目录
│   ├── simple.yaml       # 简单示例 DAG
│   ├── monitoring.yaml   # 系统监控 DAG
│   ├── xcom_example.yaml # XCom 数据管道示例
│   └── docker_example.yaml # Docker 执行器示例
├── build/bin/taskmaster  # 编译后的二进制文件
└── web-ui/               # React 前端
```

## 系统配置

`system_config.yaml` 配置调度器、存储和 API：

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

### 配置字段

| 字段 | 说明 |
|------|------|
| `scheduler.log_level` | 日志级别（debug, info, warn, error） |
| `scheduler.tick_interval_ms` | 调度器检查间隔（毫秒） |
| `scheduler.max_concurrency` | 最大并发任务数 |
| `storage.db_file` | SQLite 数据库路径 |
| `api.enabled` | 启用/禁用 REST API 和 Web UI |
| `api.host` | API 绑定地址 |
| `api.port` | API 端口 |
| `dag_source.directory` | DAG 文件目录 |
| `dag_source.scan_interval_sec` | DAG 文件热重载间隔（秒） |

## DAG 配置

DAG 文件放置在 `dags/` 目录。**文件名即为 DAG ID**（如 `simple.yaml` → DAG ID: `simple`）。

### DAG 文件结构

```yaml
name: 我的 DAG
description: 可选描述
cron: "0 2 * * *"  # 可选：启用自动调度
tasks:
  - id: task1
    name: 任务一
    command: echo '{"status": "ok", "count": 42}'
    xcom_push:
      - key: result
        source: json
  - id: task2
    name: 任务二
    command: echo "收到 $DATA"
    dependencies: [task1]
    xcom_pull:
      - key: result
        from: task1
        env: DATA
```

### DAG 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `name` | string | 显示名称 |
| `description` | string | 描述信息（可选） |
| `cron` | string | Cron 表达式（可选，有值则启用自动调度） |
| `tasks` | array | 任务列表（必填） |

**注意：** 有 `cron` 字段的 DAG 会按计划自动运行。无 `cron` 的 DAG 只能手动触发（通过 API 或 CLI）。

### 任务字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | string | 任务 ID（必填） |
| `name` | string | 显示名称（可选） |
| `command` | string | Shell 命令（必填） |
| `executor` | string | 执行器类型：`shell`（默认）或 `docker` |
| `docker_image` | string | Docker 镜像（executor 为 `docker` 时必填） |
| `docker_socket` | string | Docker socket 路径（默认 `/var/run/docker.sock`） |
| `dependencies` | array | 依赖的任务 ID 列表 |
| `timeout` | int | 超时秒数（默认 300） |
| `max_retries` | int | 最大重试次数（默认 3） |
| `xcom_push` | array | 从任务输出提取的 XCom 值 |
| `xcom_pull` | array | 注入为环境变量的 XCom 值 |

### DAG 示例

参见 `dags/` 目录：

- **simple.yaml** - 简单的两任务 DAG，每分钟运行一次
- **monitoring.yaml** - 系统健康检查（磁盘、API、数据库）及指标采集
- **xcom_example.yaml** - 演示 XCom 跨任务数据传递的数据管道
- **docker_example.yaml** - Docker 执行器示例，混合 shell/容器任务

## XCom（跨任务通信）

XCom 实现 DAG 中任务间的数据传递。常见场景：

- **ETL 管道**：在抽取/转换/加载阶段传递记录数、文件路径或批次 ID
- **动态工作流**：上游任务决定下游任务的参数
- **结果汇总**：收集多个任务的输出用于最终报告

生产者任务从输出中提取值并推送；消费者任务将其作为环境变量拉取。

### 推送配置（`xcom_push`）

从任务输出提取值：

```yaml
xcom_push:
  - key: result           # 值的键名
    source: stdout        # stdout, stderr, exit_code 或 json
    json_path: data.id    # 可选：从 JSON 提取（如 "items[0].name"）
    regex: "ID: (\\d+)"   # 可选：通过正则提取
    regex_group: 1        # 使用哪个捕获组（默认 0）
```

| 来源 | 说明 |
|------|------|
| `stdout` | 捕获标准输出为字符串（默认） |
| `stderr` | 捕获标准错误为字符串 |
| `exit_code` | 捕获退出码为整数 |
| `json` | 将标准输出解析为 JSON |

### 拉取配置（`xcom_pull`）

将值注入为环境变量：

```yaml
xcom_pull:
  - key: result           # 要获取的键
    from: producer_task   # 源任务 ID
    env: MY_DATA          # 环境变量名
```

### 示例

```yaml
tasks:
  - id: producer
    command: echo '{"count": 42}'
    xcom_push:
      - key: data
        source: json

  - id: consumer
    command: echo "收到 $COUNT 条记录"
    dependencies: [producer]
    xcom_pull:
      - key: data
        from: producer
        env: COUNT
```

## REST API

| 方法 | 端点 | 说明 |
|------|------|------|
| GET | `/api/health` | 健康检查 |
| GET | `/api/status` | 系统状态（DAG 数量、活跃运行） |
| GET | `/api/dags` | 获取所有 DAG |
| GET | `/api/dags/:id` | 获取 DAG 详情 |
| GET | `/api/dags/:id/tasks/:taskId` | 获取任务详情 |
| POST | `/api/dags/:id/trigger` | 触发 DAG 运行 |
| GET | `/api/history` | 获取运行历史 |
| GET | `/api/runs/:runId/tasks/:taskId/logs` | 获取任务日志 |

## 环境要求

- C++23（GCC 13+ / Clang 17+）
- CMake 3.25+
- Linux + io_uring

## 许可证

MIT
