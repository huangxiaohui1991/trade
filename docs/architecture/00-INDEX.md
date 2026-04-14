# Hermes 终局架构蓝图

## 定版

```
SQLite 事件内核 + 6 个粗粒度业务 Context + 可重建 Projection 的模块化单体
```

CLI / Hermes Agent (MCP) 优先。FastAPI / worker / 消息队列全部降级为未来扩展点。

## 文档结构

| 编号 | 文档 | 内容 |
|------|------|------|
| 01 | [第一性原理](./01-FIRST_PRINCIPLES.md) | 事实/观察/规则/投影四分法，核心不变量 |
| 02 | [架构总览](./02-ARCHITECTURE.md) | 终局架构图、6 个 Context、运行链路 |
| 03 | [数据模型](./03-DATA_MODEL.md) | SQLite schema、事件类型、投影表 |
| 04 | [Context 详解](./04-CONTEXTS.md) | 每个 Context 的职责、接口、文件结构 |
| 05 | [迁移计划](./05-MIGRATION.md) | 从 V1 到终局的分阶段路径 |

## 需求任务

| 编号 | 文档 | Phase |
|------|------|-------|
| T01 | [事件内核 + platform](./tasks/T01-EVENT_KERNEL.md) | Phase 1 |
| T02 | [strategy + risk 领域内核](./tasks/T02-DOMAIN_KERNEL.md) | Phase 2 |
| T03 | [market 数据层](./tasks/T03-MARKET_CONTEXT.md) | Phase 3 |
| T04 | [execution + portfolio](./tasks/T04-EXECUTION.md) | Phase 4 |
| T05 | [reporting 投影层](./tasks/T05-REPORTING.md) | Phase 5 |
| T06 | [MCP Server + Agent 融合](./tasks/T06-MCP_AGENT.md) | Phase 6 |
