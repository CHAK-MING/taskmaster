# 企业订单履约 DAG 演示

这个演示用本地 HTTP 服务模拟订单、定价、反欺诈、库存、物流、支付和发货系统，真实运行 DAGForge 的 HTTP executor、持久化、Evidence、Artifact、重试、条件分支和 Repair Run。模拟只替代外部企业系统，不绕过 Workflow Runtime；所有 Node、Task、Attempt、输出、失败和修复都由 DAGForge 实际执行与记录。

## 运行

先完成正常构建和 Minijail 安装，然后执行：

```bash
python3 scripts/demo-order-fulfillment.py --report /tmp/dagforge-order-report.json
```

默认使用 `~/.local/share/build2-configs/dagforge-gcc/dagforge/bin/dagforge`。其他二进制可通过 `--binary` 指定，基准重复次数可通过 `--benchmark-runs` 调整。

该场景已经接入 `bash scripts/test.sh e2e`，门禁使用一次完整成功运行以控制耗时；手工观察速度时默认执行三次并报告 median 与 p95。

## 验证内容

- 成功履约：校验订单后，定价、反欺诈、库存预览和物流报价并行执行，随后完成决策、支付授权、库存预占、发货和订单确认。
- 中间产物：演示逐个读取十个 Node 的输出，检查金额、风险决策、库存、物流、支付和发货结果。
- Artifact：成功路径生成大型履约审计包，Runtime 将其外部化为 Artifact，演示随后下载并校验其中 1800 条审计事件。
- 速度：脚本比较 fan-out 各分支耗时总和与实际墙钟时间，并要求至少达到 2 倍并行加速；同时报告完整 Run 的 median 和 p95。
- 业务补偿：库存预占返回正常业务结果 `reservation_failed` 时，DAG 跳过发货链路，执行支付撤销和订单补偿，Run 仍然成功。
- 系统失败：物流网关连续返回 503，Task 按策略执行两次 Attempt 后失败，独立分支的成功输出仍可查询，失败报告和 `task_failed` Evidence 必须存在。
- Repair Run：修正版 Plan 只改变 `shipping_quote` 的执行契约。DAGForge 必须复用订单校验、定价、反欺诈和库存输出，只重跑物流及其后代，并且修复耗时应低于完整运行 median。

可检查的 Workflow Plan 模板位于 `dags/order_fulfillment.json`。模板中的端口、订单输入和故障模式由演示脚本在临时目录中物化，不修改仓库文件。
