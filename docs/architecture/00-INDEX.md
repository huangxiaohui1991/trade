# Hermes 终局架构

```
SQLite 事件内核 + 6 个粗粒度业务 Context + 可重建 Projection 的模块化单体
```

CLI / MCP Server 优先。132 个测试覆盖全部 context。

## 文档

| 文档 | 内容 |
|------|------|
| [架构总览](./ARCHITECTURE.md) | 终局架构图、6 个 Context、运行链路、目录结构 |
| [数据模型](./DATA_MODEL.md) | SQLite schema、事件类型、投影表 |
