# T01 — 事件内核 + platform

> Phase 1 | 预估 1-2 周 | 优先级：P0 | 无依赖

## 目标

建立 SQLite 事件内核：event_log、config_versions、run_log。
这是整个架构的地基，后续所有 context 都依赖这层。

## 子任务

### T01.1 SQLite schema + migration

- [ ] 创建 `src/hermes/platform/db.py`
- [ ] 建表：`event_log`, `config_versions`, `run_log`
- [ ] 建表：所有 `projection_*` 表（先建空表，后续 Phase 填充）
- [ ] 建表：`market_observations`, `market_bars`
- [ ] 建表：`report_artifacts`
- [ ] WAL 模式启用
- [ ] 简单 migration 机制（版本号 + 增量 SQL）
- [ ] 金额字段全部用 `_cents` 整数

### T01.2 EventStore

- [ ] 创建 `src/hermes/platform/events.py`
- [ ] `append(stream, stream_type, event_type, payload, metadata) -> event_id`
  - 自动生成 event_id (UUID)
  - 自动递增 stream_version
  - payload 和 metadata 序列化为 JSON
- [ ] `query(stream, event_type, since, until, limit) -> list[dict]`
- [ ] `get_stream(stream) -> list[dict]`（按 version 排序）
- [ ] `count(event_type, since) -> int`

### T01.3 ConfigRegistry

- [ ] 创建 `src/hermes/platform/config.py`
- [ ] `load_and_validate(profile)` — 加载 YAML + JSON Schema 校验
- [ ] `freeze() -> ConfigSnapshot` — deep copy + SHA256 hash + 写入 config_versions
- [ ] `get_version(config_version) -> dict` — 按版本号加载历史配置
- [ ] `ConfigSnapshot` dataclass：version, hash, data (frozen dict)
- [ ] 支持 profile overlay（base + conservative/aggressive）

### T01.4 RunJournal

- [ ] 创建 `src/hermes/platform/runs.py`
- [ ] `start_run(run_type, config_version, scope) -> run_id`
- [ ] `complete_run(run_id, artifacts) -> None`
- [ ] `fail_run(run_id, error) -> None`
- [ ] `is_completed_today(run_type) -> bool`（幂等检查）
- [ ] `get_last_run(run_type, date) -> Optional[dict]`
- [ ] `get_failed_runs(days) -> list[dict]`

### T01.5 CLI 骨架

- [ ] 创建 `src/hermes/platform/cli.py`（typer）
- [ ] `hermes db init` — 初始化数据库
- [ ] `hermes db migrate` — 运行 migration
- [ ] `hermes config freeze` — 冻结当前配置
- [ ] `hermes config history` — 查看配置版本历史
- [ ] `hermes runs list` — 查看运行记录
- [ ] `hermes runs failed` — 查看失败记录
- [ ] `hermes events query --type <type> --since <date>` — 查询事件

### T01.6 测试

- [ ] EventStore 单元测试：append → query → 验证 stream_version 递增
- [ ] EventStore 幂等测试：同一 stream+version 重复写入应报错
- [ ] ConfigRegistry 测试：freeze → get_version → 内容一致
- [ ] RunJournal 测试：start → complete → is_completed_today = True
- [ ] RunJournal 幂等测试：已完成的 run_type 不重复执行

## 验收标准

- [ ] `hermes db init` 创建所有表
- [ ] 可以 append 事件并按 stream/type/时间查询
- [ ] config freeze 生成版本号和 hash，可按版本号还原
- [ ] run journal 支持幂等检查
- [ ] 所有金额字段为整数（分）
