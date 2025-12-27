# TaskMaster

基于现代 C++23 的高性能 DAG 任务调度器。

![TaskMaster UI](image/web-ui.png)

## 特性

- DAG 任务依赖管理
- 标准 Cron 表达式定时调度
- Web UI 可视化管理（React Flow）
- REST API + WebSocket 实时日志
- SQLite 持久化，支持崩溃恢复

## 快速开始

### 编译

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

### 运行

```bash
# 启动服务（含 Web UI）
./build/bin/taskmaster --server --port 8080

# 访问 http://localhost:8080
```

### 构建 Web UI（可选）

```bash
cd web-ui
npm install && npm run build
```

## 任务配置

| 字段          | 类型   | 说明                       |
| ------------- | ------ | -------------------------- |
| `id`          | string | 唯一标识（必填）           |
| `name`        | string | 显示名称                   |
| `command`     | string | Shell 命令（必填）         |
| `cron`        | string | Cron 表达式                |
| `deps`        | array  | 依赖任务 ID 列表           |
| `timeout`     | int    | 超时秒数（默认 300）       |
| `max_retries` | int    | 最大重试次数（默认 3）     |

## REST API

| 方法      | 端点                          | 说明         |
| --------- | ----------------------------- | ------------ |
| GET       | `/api/dags`                   | 获取 DAG 列表 |
| POST      | `/api/dags`                   | 创建 DAG     |
| GET       | `/api/dags/:id`               | 获取 DAG     |
| PUT       | `/api/dags/:id`               | 更新 DAG     |
| DELETE    | `/api/dags/:id`               | 删除 DAG     |
| POST      | `/api/dags/:id/trigger`       | 触发运行     |
| GET       | `/api/dags/:id/tasks`         | 获取任务列表 |
| POST      | `/api/dags/:id/tasks`         | 添加任务     |
| PUT       | `/api/dags/:id/tasks/:taskId` | 更新任务     |
| DELETE    | `/api/dags/:id/tasks/:taskId` | 删除任务     |
| WebSocket | `/ws/logs`                    | 实时日志     |

## 环境要求

- C++23（GCC 13+ / Clang 17+）
- CMake 3.25+
- Linux + io_uring

## 许可证

MIT
