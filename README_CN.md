# TaskMaster

一个基于现代 C++23 的高性能 DAG 任务调度器。

## 特性

- **DAG 依赖管理** - 支持复杂的有向无环图任务工作流
- **Cron 定时调度** - 标准 cron 表达式支持
- **双运行模式** - CLI 守护进程模式，Server 模式带 Web UI
- **REST API + WebSocket** - 完整的管理 API 和实时日志推送
- **SQLite 持久化** - 支持崩溃恢复和运行历史记录
- **现代化 Web UI** - 基于 React 的控制台，含 DAG 可视化（React Flow）
- **YAML/JSON 配置** - 灵活的配置格式

## 快速开始

### 编译

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

### CLI 模式（守护进程）

作为后台守护进程运行，根据 cron 表达式自动执行任务：

```bash
# 前台运行（用于测试）
./build/bin/taskmaster -c config.yaml

# 后台守护进程运行
./build/bin/taskmaster -c config.yaml -d

# CLI 模式同时启用 API
./build/bin/taskmaster -c config.yaml --port 8080

# 触发指定 DAG 后退出
./build/bin/taskmaster -c config.yaml -t my_dag

# 列出所有任务后退出
./build/bin/taskmaster -c config.yaml -l
```

### Server 模式（API + Web UI）

启动 REST API 和 Web UI，支持动态管理 DAG：

```bash
# 在 8080 端口启动服务
./build/bin/taskmaster --server --port 8080

# 后台守护进程运行
./build/bin/taskmaster --server --port 8080 -d

# 指定数据库路径
./build/bin/taskmaster --server --port 8080 --db /var/lib/taskmaster/data.db
```

访问 Web UI：`http://localhost:8080`

### 命令行选项

```
选项:
  -c, --config <file>   配置文件（YAML 或 JSON）
  --server              Server 模式（仅 API，无需配置文件）
  --port <port>         API 服务端口（默认: 8080）
  --host <host>         API 服务地址（默认: 127.0.0.1）
  --db <file>           数据库文件（默认: taskmaster.db）
  -d, --daemon          后台守护进程运行
  -l, --list            列出所有任务后退出
  -t, --trigger <dag>   触发指定 DAG 后退出
  -v, --version         显示版本
  -h, --help            显示帮助
```

## 配置示例

### ETL 数据流水线

典型的数据仓库 ETL 工作流：

```yaml
scheduler:
  log_level: info

tasks:
  - id: extract_mysql
    name: 从 MySQL 抽取
    command: |
      mysqldump -h db.example.com -u etl_user -p$MYSQL_PASSWORD \
        --single-transaction sales_db orders customers \
        > /data/staging/mysql_$(date +%Y%m%d_%H).sql
    cron: "0 * * * *"  # 每小时执行
    timeout: 1800

  - id: extract_api
    name: 从 API 抽取
    command: |
      curl -s -H "Authorization: Bearer $API_TOKEN" \
        "https://api.example.com/v1/transactions" \
        > /data/staging/api_$(date +%Y%m%d_%H).json
    cron: "0 * * * *"
    timeout: 600

  - id: transform
    name: 数据转换
    command: python3 /opt/etl/transform.py --input /data/staging --output /data/processed
    deps: [extract_mysql, extract_api]
    timeout: 3600

  - id: load_warehouse
    name: 加载到数据仓库
    command: psql -h warehouse.example.com -U etl_user -d analytics -f /opt/etl/load.sql
    deps: [transform]
    timeout: 1800
```

### 数据库备份

每日备份并上传云存储：

```yaml
scheduler:
  log_level: info

tasks:
  - id: backup_postgres
    name: 备份 PostgreSQL
    command: |
      pg_dump -h localhost -U postgres -d production | gzip > /backup/daily_$(date +%Y%m%d).sql.gz
    cron: "0 2 * * *"  # 每天凌晨 2 点
    timeout: 7200

  - id: upload_s3
    name: 上传到 S3
    command: aws s3 cp /backup/ s3://company-backups/ --recursive --exclude "*.tmp"
    deps: [backup_postgres]
    timeout: 3600

  - id: cleanup
    name: 清理旧备份
    command: find /backup -type f -mtime +7 -delete
    deps: [upload_s3]
    timeout: 300
```

### 系统监控

定期健康检查：

```yaml
scheduler:
  log_level: info

tasks:
  - id: check_disk
    name: 检查磁盘使用率
    command: |
      USAGE=$(df -h / | awk 'NR==2 {print $5}' | tr -d '%')
      [ "$USAGE" -gt 85 ] && echo "警告: 磁盘使用率 ${USAGE}%" && exit 1
      echo "正常: 磁盘使用率 ${USAGE}%"
    cron: "*/5 * * * *"  # 每 5 分钟
    timeout: 30

  - id: check_api
    name: 检查 API 健康状态
    command: |
      curl -sf http://localhost:8080/health || exit 1
    cron: "* * * * *"
    timeout: 30
    max_retries: 2
```

### 任务配置项

| 字段          | 类型   | 说明                         |
| ------------- | ------ | ---------------------------- |
| `id`          | string | 任务唯一标识（必填）         |
| `name`        | string | 显示名称                     |
| `command`     | string | 执行的 Shell 命令（必填）    |
| `cron`        | string | Cron 表达式                  |
| `deps`        | array  | 依赖的任务 ID 列表           |
| `timeout`     | int    | 超时时间，单位秒（默认 300） |
| `max_retries` | int    | 最大重试次数（默认 3）       |
| `enabled`     | bool   | 是否启用（默认 true）        |

## REST API

| 方法      | 端点                          | 说明                |
| --------- | ----------------------------- | ------------------- |
| GET       | `/api/health`                 | 健康检查            |
| GET       | `/api/status`                 | 系统状态            |
| GET       | `/api/dags`                   | 获取所有 DAG        |
| POST      | `/api/dags`                   | 创建 DAG            |
| GET       | `/api/dags/:id`               | 获取 DAG 详情       |
| PUT       | `/api/dags/:id`               | 更新 DAG            |
| DELETE    | `/api/dags/:id`               | 删除 DAG            |
| POST      | `/api/dags/:id/trigger`       | 触发 DAG 运行       |
| GET       | `/api/dags/:id/tasks`         | 获取 DAG 内任务列表 |
| POST      | `/api/dags/:id/tasks`         | 添加任务到 DAG      |
| PUT       | `/api/dags/:id/tasks/:taskId` | 更新任务            |
| DELETE    | `/api/dags/:id/tasks/:taskId` | 删除任务            |
| WebSocket | `/ws/logs`                    | 实时日志流          |

## Web UI

Web UI 提供以下功能：

- 系统概览仪表盘
- DAG 管理和可视化（React Flow）
- 实时日志查看器（支持过滤和搜索）
- 任务配置编辑器

### 开发模式

```bash
cd web-ui
npm install
npm run dev
```

### 生产构建

```bash
cd web-ui
npm run build
# 输出目录：web-ui/dist/
```

## 环境要求

- C++23 编译器（GCC 13+ 或 Clang 17+）
- CMake 3.25+
- Linux（需要 liburing 支持 io_uring）

### 依赖库（CMake 自动获取）

- nlohmann/json
- yaml-cpp
- SQLite3
- Crow（HTTP/WebSocket 服务器）

## 项目结构

```
TaskMaster/
├── include/taskmaster/    # 头文件
├── src/taskmaster/        # 源文件
│   └── api/               # REST API 和 WebSocket
├── web-ui/                # React 前端
├── examples/              # 配置示例
│   ├── etl_pipeline.yaml  # ETL 工作流示例
│   ├── backup_rotation.yaml # 备份示例
│   └── monitoring.yaml    # 监控示例
├── tests/                 # 单元测试
└── docs/                  # 文档
```

## 路线图

- [ ] 更多执行器（Docker、Kubernetes、SSH）

## 许可证

MIT