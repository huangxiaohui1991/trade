# T05 — reporting 投影层

> Phase 5 | 预估 1 周 | 优先级：P1 | 依赖：T01, T04

## 目标

报告和通知全部从事实和投影消费生成。Obsidian 降级为纯投影层，不再当事实源。

## 核心约束

- reporting/ **不反写任何业务表**
- 所有产出物可删可重建
- Obsidian 只是生成物或人工注释层

## 子任务

### T05.1 ProjectionUpdater

- [ ] 创建 `src/hermes/reporting/projectors.py`
- [ ] `sync_all(since)` — 从 event_log 增量同步所有 projection 表
- [ ] `rebuild_all()` — 删除所有 projection 数据，从 event_log 完全重建
- [ ] `sync_positions()` — 同步 projection_positions
- [ ] `sync_orders()` — 同步 projection_orders
- [ ] `sync_balances()` — 同步 projection_balances
- [ ] `sync_candidate_pool()` — 同步 projection_candidate_pool
- [ ] `sync_market_state()` — 同步 projection_market_state

### T05.2 报告生成

- [ ] 创建 `src/hermes/reporting/reports.py`
- [ ] `generate_morning_report(run_id) -> str` — 盘前摘要
- [ ] `generate_evening_report(run_id) -> str` — 收盘报告
- [ ] `generate_scoring_report(run_id) -> str` — 评分报告
- [ ] `generate_weekly_report(week) -> str` — 周报
- [ ] 所有报告从 event_log + projection 表消费数据
- [ ] 报告写入 report_artifacts 表

### T05.3 Obsidian 投影

- [ ] 创建 `src/hermes/reporting/obsidian.py`
- [ ] `write_daily_log(run_id, report)` — 写入今日日志
- [ ] `write_scoring_report(run_id, scores)` — 写入评分报告
- [ ] `write_portfolio_status()` — 写入持仓状态页
- [ ] `write_pool_status()` — 写入池子状态页
- [ ] 所有写入都是从投影表生成，可删可重建

**迁移映射：**
| V1 | V2 |
|----|-----|
| `utils/obsidian.py::ObsidianVault.write()` | `reporting/obsidian.py` |
| pipeline 中散落的 vault 写入逻辑 | 统一到 reporting/obsidian.py |

### T05.4 Discord 格式化

- [ ] 创建 `src/hermes/reporting/discord.py`
- [ ] `format_morning_embed(report) -> dict` — 盘前摘要 embed
- [ ] `format_evening_embed(report) -> dict` — 收盘报告 embed
- [ ] `format_scoring_embed(report) -> dict` — 评分报告 embed
- [ ] `format_stop_alert_embed(signal) -> dict` — 止损告警 embed
- [ ] 注意：实际发送由 Hermes Agent Gateway 处理，这里只负责格式化

**迁移映射：**
| V1 | V2 |
|----|-----|
| `utils/discord_push.py::_build_morning_embeds()` | `reporting/discord.py::format_morning_embed()` |
| `utils/discord_push.py::_build_evening_embeds()` | `reporting/discord.py::format_evening_embed()` |
| `utils/discord_push.py` (700+ 行) | `reporting/discord.py` (格式化) + Agent Gateway (发送) |

### T05.5 测试

- [ ] rebuild_all 测试：删除所有 projection → rebuild → 数据一致
- [ ] 报告生成测试：固定事件数据 → 生成报告 → 验证内容
- [ ] Obsidian 写入测试：生成 → 删除 → 重新生成 → 内容一致

## 验收标准

- [ ] reporting/ 不 import 任何业务 service（只读 event_log + projection）
- [ ] Obsidian 页面可删可重建
- [ ] 报告内容与现有系统一致
- [ ] projection 表可从 event_log 完全重建
