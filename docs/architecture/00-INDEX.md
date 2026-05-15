# A-Stock Trading 终局架构

```
MySQL 事件内核 + 6 个粗粒度业务 Context + 可重建 Projection 的模块化单体
```

CLI / MCP Server 优先。Runtime 数据库由 `ASTOCK_DATABASE_URL=mysql+pymysql://...` 提供。SQLite 只作为测试替身和一次性历史迁移源。

## 文档

| 文档 | 内容 |
|------|------|
| [架构总览](./ARCHITECTURE.md) | 终局架构图、6 个 Context、运行链路、目录结构 |
| [数据模型](./DATA_MODEL.md) | SQLAlchemy/MySQL schema、事件类型、投影表 |
| [运维手册](../operations/RUNBOOK.md) | 健康检查、备份、launchd 模板 |
