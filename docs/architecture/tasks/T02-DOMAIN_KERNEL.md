# T02 — strategy + risk 领域内核

> Phase 2 | 预估 1-2 周 | 优先级：P0 | 依赖：T01
> **状态：🟡 部分完成** | 31 个测试通过

## 目标

将评分、决策、风控、风格判定抽取为纯函数内核。
回测和实盘共用同一套代码，只替换数据源和执行器。

## 核心约束

- strategy/ 和 risk/ 下的 domain 代码**不能 import** HTTP、SQL、YAML、文件系统
- 所有函数输入是 dataclass/dict，输出是 dataclass/dict
- 结果必须带 run_id + config_version + data_snapshot_ref

## 子任务

### T02.1 strategy models

- [x] 创建 `src/hermes/strategy/models.py`
- [x] `ScoringWeights` — 四维权重
- [x] `ScoreResult` — 评分结果（total, dimensions[], veto_signals[], style, data_quality）
- [x] `DecisionIntent` — 交易意图（action, confidence, position_pct, stop_loss, take_profit）
- [ ] `StyleResult` — 风格判定结果（当前内联在 Scorer 中，未独立 dataclass）
- [x] `MarketState` — 大盘状态（signal, detail, multiplier）
- [ ] `EntrySignal` — 入场信号（当前作为 ScoreResult.entry_signal bool 字段）

### T02.2 Scorer（纯函数）

- [x] 创建 `src/hermes/strategy/scorer.py`
- [x] `Scorer.__init__(weights, veto_rules)`
- [x] `score(snapshot: StockSnapshot) -> ScoreResult`
- [x] `_score_technical(snapshot)` — 金叉/量比/RSI/均线排列/动量
- [x] `_score_fundamental(snapshot)` — ROE/营收增长/现金流
- [x] `_score_flow(snapshot)` — 资金流入/主力/北向
- [x] `_score_sentiment(snapshot)` — 舆情评分
- [x] `_check_veto(snapshot, dimensions)` — 一票否决
- [x] `_check_entry(snapshot, tech_score)` — 入场信号
- [x] `score_batch(snapshots) -> list[ScoreResult]`

### T02.3 Decider（纯函数）

- [x] 创建 `src/hermes/strategy/decider.py`
- [x] `Decider.__init__(thresholds, position_limits)`
- [x] `decide(score_result, market_state, portfolio_summary) -> DecisionIntent`
- [x] 不再内部实例化 MarketTimer，接收 MarketState 参数

### T02.4 Classifier + Timer（纯函数）

- [ ] 创建 `src/hermes/strategy/classifier.py`（风格判定当前内联在 Scorer._classify_style 中，未独立模块）
- [ ] `classify_style(closes, rsi, config) -> StyleResult`
- [ ] `check_style_switch(style, daily_change, rsi, rsi_history) -> SwitchResult`
- [ ] 创建 `src/hermes/strategy/timer.py`
- [ ] `compute_market_signal(index_data, config) -> MarketState`

### T02.5 risk models

- [x] 创建 `src/hermes/risk/models.py`
- [x] `ExitSignal` — 离场信号（signal_type, trigger_price, urgency）
- [ ] `RiskAssessment` — 风控评估结果（待 T02.8 service 层实现时补充）
- [x] `PositionSize` — 仓位计算结果（shares, amount, pct）
- [x] `RiskParams` — 风格对应的风控参数
- [ ] `PortfolioLimits` — 组合风控阈值（当前用 dict 传入）
- [x] `RiskBreach` — 组合风控触发

### T02.6 risk rules（纯函数）

- [x] 创建 `src/hermes/risk/rules.py`
- [x] `check_exit_signals(position, snapshot, risk_params) -> list[ExitSignal]`
- [x] `_check_stop_loss()` — 固定止损
- [x] `_check_trailing_stop()` — 移动止盈
- [x] `_check_time_stop()` — 时间止损
- [x] `_check_ma_exit()` — MA 跌破离场
- [x] `check_portfolio_risk(portfolio, limits) -> list[RiskBreach]`

### T02.7 risk sizing（纯函数）

- [x] 创建 `src/hermes/risk/sizing.py`
- [x] `calc_position_size(decision, portfolio, market_multiplier, limits) -> PositionSize`

### T02.8 StrategyService + RiskService

- [ ] 创建 `src/hermes/strategy/service.py`
- [ ] `evaluate(snapshots, market_state, run_id, config_version) -> list[DecisionIntent]`
- [ ] 评分结果追加到 event_log（score.calculated）
- [ ] 决策结果追加到 event_log（decision.suggested）

- [ ] 创建 `src/hermes/risk/service.py`
- [ ] `assess(positions, snapshots, market_state, run_id) -> list[RiskAssessment]`
- [ ] 风控结果追加到 event_log

### T02.9 桥接层

- [ ] 现有 `scripts/engine/scorer.py` 加 adapter 调用新 Scorer
- [ ] 现有 `scripts/engine/composite.py` 加 adapter 调用新 Decider
- [ ] 现有 pipeline 行为不变

### T02.10 单元测试

- [x] `test_scorer.py` — 固定 StockSnapshot → 验证评分（10 个测试）
- [x] `test_decider.py` — 固定 ScoreResult → 验证决策（8 个测试）
- [x] `test_rules.py` — 固定 Position → 验证止损/止盈（9 个测试）
- [x] `test_sizing.py` — 固定 Decision → 验证仓位（含在 test_risk.py 中，4 个测试）
- [ ] `test_classifier.py` — 固定 K 线 → 验证风格（风格判定内联在 scorer 测试中）
- [ ] 对比测试：新旧 scorer 同一输入产生相同输出

## 验收标准

- [x] strategy/ 和 risk/ 下无 `import requests/akshare/httpx/sqlite3/yaml/os.path`
- [x] 单元测试全过，无需 mock
- [ ] 现有 pipeline 行为不变（桥接层待实现）
- [ ] 评分结果写入 event_log 并带 run_id + config_version（service 层待实现）
