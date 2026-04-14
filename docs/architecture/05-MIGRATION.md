# 05 — 迁移计划

## 原则

- 渐进式迁移，每个 Phase 结束后系统可正常运行
- 新旧代码通过 adapter 桥接共存
- 现有 crontab + pipeline 在迁移期间继续运行
- 每个 Phase 有对比测试：新旧代码同一输入，输出一致

## Phase 总览

```
Phase 1 ──▶ Phase 2 ──▶ Phase 3 ──▶ Phase 4 ──▶ Phase 5 ──▶ Phase 6
事件内核     领域内核     市场数据     执行+持仓    报告投影     MCP+Agent
platform    strategy    market      execution   reporting   interfaces
            + risk
(1-2 周)    (1-2 周)    (1-2 周)    (1 周)      (1 周)      (1 周)
```

---

## Phase 1: 事件内核 + platform（1-2 周）

地基。没有这层，后面所有 context 无法运作。

**做什么：**
1. SQLite schema 建表（event_log, config_versions, run_log, projection 表）
2. EventStore 实现（append, query, get_stream）
3. ConfigRegistry 实现（load, validate, freeze, version）
4. RunJournal 实现（start, complete, fail, 幂等检查）
5. DB migration 机制

**验收：**
- 可以 append 事件、查询事件
- 可以 freeze config 并查询历史版本
- 可以创建 run 并检查幂等

---

## Phase 2: strategy + risk 领域内核（1-2 周）

核心资产。纯函数评分/决策/风控，回测和实盘共用。

**做什么：**
1. 定义 domain models（StockSnapshot, ScoreResult, DecisionIntent, ExitSignal, ...）
2. 从现有 scorer.py 抽取纯函数 Scorer
3. 从现有 composite.py 抽取纯函数 Decider
4. 从现有 risk_model.py 抽取纯函数 risk rules + sizing
5. 从现有 stock_classifier.py 抽取纯函数 classifier
6. StrategyService + RiskService（编排 + 事件追加）
7. 现有 pipeline 加 adapter 调用新纯函数
8. 单元测试（固定输入 → 验证输出）

**验收：**
- strategy/ 和 risk/ 下无 IO import
- 单元测试全过，无需 mock 网络
- 现有 pipeline 行为不变

---

## Phase 3: market 数据层（1-2 周）

**做什么：**
1. 定义 Protocol 接口（MarketDataProvider, FinancialDataProvider, ...）
2. 实现 AkShare / MX / Sina adapter
3. MarketService（fallback chain + 缓存 + semaphore 限流）
4. market_observations / market_bars 存储
5. Pipeline 改为调用 MarketService
6. 批量接口优先（如 stock_zh_a_spot_em）

**验收：**
- 评分耗时从 ~60-90s 降至 ~15-25s
- 数据源可插拔
- 市场观察追加到 market_observations

---

## Phase 4: execution + portfolio（1 周）

**做什么：**
1. 定义 Order, Position, Balance models
2. 订单事件化（order.created → order.filled → position.opened）
3. PositionProjector（从 event_log 重建持仓）
4. 从现有 state/service.py 迁移持仓/订单逻辑
5. 模拟盘 adapter（兼容现有 shadow_trade）

**验收：**
- 持仓状态可从 event_log 完全重建
- 删除 projection_positions 后重建，数据一致

---

## Phase 5: reporting 投影层（1 周）

**做什么：**
1. ProjectionUpdater（event → projection 表同步）
2. 日报/周报/月报生成（从事实和投影消费）
3. Obsidian 写入（只是投影，不是事实源）
4. Discord 消息格式化
5. report_artifacts 存储

**验收：**
- Obsidian 页面可删可重建
- 报告内容与现有系统一致
- reporting/ 不反写任何业务表

---

## Phase 6: MCP Server + Agent 融合（1 周）

**做什么：**
1. MCP Server 实现（~12 个 tools，薄壳调用 service 层）
2. Hermes Agent Skills（盘前/收盘/评分/选股/分析）
3. Hermes Agent Cron Jobs（替代 crontab）
4. CLI 保留（typer，调试用）
5. 端到端验证

**验收：**
- Agent 通过 MCP 调用所有交易能力
- Cron 自动执行盘前/收盘流程
- 旧 crontab 可安全停用

---

## 回测迁移（贯穿 Phase 2-4）

回测不是单独一个 Phase，而是随着领域内核的建立逐步迁移：

- Phase 2 完成后：strategy replay 可以用新的 Scorer/Decider（从 K 线重跑）
- Phase 3 完成后：fixture builder 可以用 MarketService 的 baostock adapter
- Phase 4 完成后：event replay 可以用新的 execution 模拟器

三种回测模式：
1. **Event Replay** — 基于历史评分样本，只跑 Decider + Risk（参数扫描用）
2. **Strategy Replay** — 从 K 线出发完整重跑（策略验证用）
3. **Analysis** — 绩效分析、回撤、归因（消费回测产出的事件）

---

## 过渡期安排

- Phase 1-5 期间，现有 crontab + discord_push.py 继续运行
- 新旧系统通过 adapter 桥接，逐步切换
- Phase 6 完成后切换到 Agent Cron，停用旧 crontab
- state/service.py 在 Phase 4 逐步被 execution context 替代，不急着删

## 风险与回退

| 风险 | 缓解措施 |
|------|----------|
| 重构引入 bug | 每个 Phase 写对比测试 |
| event_log 设计不合理 | 前期事件类型保持粗粒度，后续可细化 |
| SQLite 性能不够 | market_bars 体量膨胀时外溢到 Parquet/DuckDB |
| Agent 推理不稳定 | Skill 写详细，约束 Agent 行为 |
| state/service.py 迁移复杂 | 先做薄 adapter，不急着重构内部 |
