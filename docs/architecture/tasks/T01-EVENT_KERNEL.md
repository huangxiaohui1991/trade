# T01 — 事件内核 + platform

> Phase 1 | 预估 1-2 周 | 优先级：P0 | 无依赖
> **状态：✅ 已完成** | 19 个测试通过

## 目标

建立 SQLite 事件内核：event_log、config_versions、run_log。
这是整个架构的地基，后续所有 context 都依赖这层。

## 子任务

### T01.1 SQLite schema + migration

- [x] 创建 `src/hermes/platform/db.py`
- [x] 建表：`event_log`, `config_versions`, `run_log`
- [x] 建表：所有 `projection_*` 表（先建空表，后续 Phase 填充）
- [x] 建表：`market_observations`, `market_bars`
- [x] 建表：`report_artifacts`
- [x] WAL 模式启用
- [x] 简单 migration 机制（版本号 + 增量 SQL）
- [x] 金额字段全部用 `_cents` 整数

### T01.2 EventStore

- [x] 创建 `src/hermes/platform/events.py`
- [x] `append(stream, stream_type, event_type, payload, metadata) -> event_id`
  - 自动生成 event_id (UUID)
  - 自动递增 stream_version
  - payload 和 metadata 序列化为 JSON
- [x] `query(stream, event_type, since, until, limit) -> list[dict]`
- [x] `get_stream(stream) -> list[dict]`（按 version 排序）
- [x] `count(event_type, since) -> int`

### T01.3 ConfigRegistry

- [x] 创建 `src/hermes/platform/config.py`
- [ ] `load_and_validate(profile)` — 加载 YAML + JSON Schema 校验（YAML 加载已实现，JSON Schema 校验待补充）
- [x] `freeze() -> ConfigSnapshot` — deep copy + SHA256 hash + 写入 config_versions
- [x] `get_version(config_version) -> dict` — 按版本号加载历史配置
- [x] `ConfigSnapshot` dataclass：version, hash, data (frozen dict)
- [x] 支持 profile overlay（base + conservative/aggressive）

### T01.4 RunJournal

- [x] 创建 `src/hermes/platform/runs.py`
- [x] `start_run(run_type, config_version, scope) -> run_id`
- [x] `complete_run(run_id, artifacts) -> None`
- [x] `fail_run(run_id, error) -> None`
- [x] `is_completed_today(run_type) -> bool`（幂等检查）
- [x] `get_last_run(run_type, date) -> Optional[dict]`
- [x] `get_failed_runs(days) -> list[dict]`

### T01.5 CLI 骨架

- [x] 创建 `src/hermes/platform/cli.py`（typer）
- [x] `hermes db init` — 初始化数据库
- [ ] `hermes db migrate` — 运行 migration（当前只有 v1，migrate 逻辑待后续 schema 变更时补充）
- [x] `hermes config freeze` — 冻结当前配置
- [x] `hermes config history` — 查看配置版本历史
- [x] `hermes runs list` — 查看运行记录
- [x] `hermes runs failed` — 查看失败记录
- [x] `hermes events query --type <type> --since <date>` — 查询事件

### T01.6 测试

- [x] EventStore 单元测试：append → query → 验证 stream_version 递增
- [x] EventStore 幂等测试：同一 stream+version 重复写入应报错
- [x] ConfigRegistry 测试：freeze → get_version → 内容一致
- [x] RunJournal 测试：start → complete → is_completed_today = True
- [x] RunJournal 幂等测试：已完成的 run_type 不重复执行

## 验收标准

- [x] `hermes db init` 创建所有表
- [x] 可以 append 事件并按 stream/type/时间查询
- [x] config freeze 生成版本号和 hash，可按版本号还原
- [x] run journal 支持幂等检查
- [x] 所有金额字段为整数（分）
