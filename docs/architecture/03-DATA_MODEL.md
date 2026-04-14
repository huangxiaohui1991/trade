# 03 — 数据模型

## SQLite Schema 设计原则

- 按未来 PG 迁移去设计，不按 SQLite 习惯随手建表
- 金额存最小货币单位整数（分），避免浮点坑
- JSON 全部显式放 `*_json` 字段，不混杂隐式结构
- 任何 `projection_*` 表都必须能从 `event_log` + `market_observations` 重建
- 每次策略运行必须冻结 `config_version` + `run_id` + `data_cutoff`

---

## 核心表

### event_log — 业务事实（append-only）

```sql
CREATE TABLE event_log (
    event_id        TEXT PRIMARY KEY,           -- UUID
    stream          TEXT NOT NULL,              -- 聚合标识，如 "order:002138:20260414"
    stream_type     TEXT NOT NULL,              -- "order" | "position" | "strategy" | "risk" | "pool"
    stream_version  INTEGER NOT NULL,           -- 同一 stream 内的序号
    event_type      TEXT NOT NULL,              -- 见下方事件类型表
    payload_json    TEXT NOT NULL,              -- 事件数据 JSON
    metadata_json   TEXT NOT NULL DEFAULT '{}', -- run_id, config_version, data_snapshot_ref, ...
    occurred_at     TEXT NOT NULL,              -- ISO 8601
    UNIQUE(stream, stream_version)
);

CREATE INDEX idx_event_log_type ON event_log(event_type);
CREATE INDEX idx_event_log_stream ON event_log(stream);
CREATE INDEX idx_event_log_occurred ON event_log(occurred_at);
```

### config_versions — 规则版本

```sql
CREATE TABLE config_versions (
    config_version  TEXT PRIMARY KEY,           -- "v20260414_153500"
    config_hash     TEXT NOT NULL UNIQUE,       -- SHA256 of config_json
    config_json     TEXT NOT NULL,              -- 完整 strategy.yaml + stocks.yaml 快照
    created_at      TEXT NOT NULL,
    activated_at    TEXT                        -- 首次被 run 使用的时间
);
```

### run_log — 运行记录

```sql
CREATE TABLE run_log (
    run_id          TEXT PRIMARY KEY,           -- "run_20260414_153500_abc123"
    run_type        TEXT NOT NULL,              -- "morning" | "evening" | "scoring" | "screener" | "backtest"
    scope           TEXT NOT NULL DEFAULT 'cn_a', -- "cn_a" | "hk" | "paper"
    config_version  TEXT NOT NULL REFERENCES config_versions(config_version),
    data_cutoff     TEXT,                       -- 数据截止时间
    status          TEXT NOT NULL DEFAULT 'running', -- "running" | "completed" | "failed"
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    error_message   TEXT,
    artifacts_json  TEXT DEFAULT '{}',          -- 产出物引用
    FOREIGN KEY (config_version) REFERENCES config_versions(config_version)
);

CREATE INDEX idx_run_log_type_date ON run_log(run_type, started_at);
```

---

## 市场观察表

### market_observations — 标准化观测

```sql
CREATE TABLE market_observations (
    observation_id  TEXT PRIMARY KEY,
    source          TEXT NOT NULL,              -- "akshare" | "mx" | "sina" | "baostock"
    kind            TEXT NOT NULL,              -- "quote" | "financial" | "flow" | "sentiment" | "index"
    symbol          TEXT NOT NULL,              -- "002138" | "sh000001"
    observed_at     TEXT NOT NULL,              -- 观测时间
    run_id          TEXT,                       -- 关联的 run（可选）
    payload_json    TEXT NOT NULL,
    UNIQUE(source, kind, symbol, observed_at)
);

CREATE INDEX idx_market_obs_symbol ON market_observations(symbol, kind, observed_at);
```

### market_bars — K 线时序（高频查询）

```sql
CREATE TABLE market_bars (
    symbol          TEXT NOT NULL,
    bar_date        TEXT NOT NULL,              -- "2026-04-14"
    period          TEXT NOT NULL DEFAULT 'daily', -- "daily" | "weekly"
    open_cents      INTEGER NOT NULL,           -- 最小货币单位（分）
    high_cents      INTEGER NOT NULL,
    low_cents       INTEGER NOT NULL,
    close_cents     INTEGER NOT NULL,
    volume          INTEGER NOT NULL,
    amount_cents    INTEGER NOT NULL,
    source          TEXT NOT NULL,
    fetched_at      TEXT NOT NULL,
    PRIMARY KEY (symbol, bar_date, period)
);
```

---

## 投影表（全部可删可重建）

### projection_positions — 当前持仓

```sql
CREATE TABLE projection_positions (
    code            TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    style           TEXT NOT NULL,              -- "slow_bull" | "momentum"
    shares          INTEGER NOT NULL,
    avg_cost_cents  INTEGER NOT NULL,
    entry_date      TEXT NOT NULL,
    entry_day_low_cents INTEGER,
    stop_loss_cents INTEGER,
    take_profit_cents INTEGER,
    highest_since_entry_cents INTEGER,
    current_price_cents INTEGER,
    unrealized_pnl_cents INTEGER,
    updated_at      TEXT NOT NULL
);
```

### projection_orders — 订单状态

```sql
CREATE TABLE projection_orders (
    order_id        TEXT PRIMARY KEY,
    code            TEXT NOT NULL,
    side            TEXT NOT NULL,              -- "buy" | "sell"
    shares          INTEGER NOT NULL,
    price_cents     INTEGER NOT NULL,
    status          TEXT NOT NULL,              -- "pending" | "filled" | "cancelled"
    broker          TEXT,
    created_at      TEXT NOT NULL,
    filled_at       TEXT,
    updated_at      TEXT NOT NULL
);
```

### projection_balances — 资金状态

```sql
CREATE TABLE projection_balances (
    scope           TEXT PRIMARY KEY,           -- "real" | "paper"
    cash_cents      INTEGER NOT NULL,
    total_asset_cents INTEGER NOT NULL,
    weekly_buy_count INTEGER NOT NULL DEFAULT 0,
    daily_pnl_cents INTEGER NOT NULL DEFAULT 0,
    consecutive_loss_days INTEGER NOT NULL DEFAULT 0,
    updated_at      TEXT NOT NULL
);
```

### projection_candidate_pool — 核心池/观察池

```sql
CREATE TABLE projection_candidate_pool (
    code            TEXT NOT NULL,
    pool_tier       TEXT NOT NULL,              -- "core" | "watch"
    name            TEXT,
    score           REAL,
    added_at        TEXT NOT NULL,
    last_scored_at  TEXT,
    streak_days     INTEGER DEFAULT 0,
    note            TEXT,
    PRIMARY KEY (code, pool_tier)
);
```

### projection_market_state — 大盘状态

```sql
CREATE TABLE projection_market_state (
    index_symbol    TEXT PRIMARY KEY,           -- "sh000001" | "sz399001" | "sz399006"
    name            TEXT NOT NULL,
    signal          TEXT,                       -- "GREEN" | "YELLOW" | "RED" | "CLEAR"
    price_cents     INTEGER,
    change_pct      REAL,
    ma20_pct        REAL,
    ma60_pct        REAL,
    updated_at      TEXT NOT NULL
);
```

### report_artifacts — 报告产出物

```sql
CREATE TABLE report_artifacts (
    artifact_id     TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL,
    report_type     TEXT NOT NULL,              -- "morning" | "evening" | "scoring" | "weekly"
    format          TEXT NOT NULL,              -- "markdown" | "discord_embed" | "json"
    content         TEXT NOT NULL,
    delivered_to    TEXT,                       -- "discord:盘前摘要" | "obsidian:02-运行/日志"
    created_at      TEXT NOT NULL
);
```

---

## 事件类型

### strategy 事件

| event_type | payload 关键字段 | 说明 |
|-----------|-----------------|------|
| `score.calculated` | code, name, total, dimensions[], veto_signals[], style, data_quality | 四维评分完成 |
| `entry_signal.detected` | code, signal_type, score, market_signal | 入场信号触发 |
| `decision.suggested` | code, action, confidence, position_pct, stop_loss, take_profit | 交易意图生成 |
| `pool.promoted` | code, from_tier, to_tier, reason | 池子晋升 |
| `pool.demoted` | code, from_tier, to_tier, reason | 池子降级 |
| `pool.removed` | code, tier, reason | 从池子移除 |

### risk 事件

| event_type | payload 关键字段 | 说明 |
|-----------|-----------------|------|
| `risk.stop_loss_triggered` | code, trigger_price, current_price, style | 止损触发 |
| `risk.trailing_stop_triggered` | code, highest, current, drawdown_pct | 移动止盈触发 |
| `risk.time_stop_triggered` | code, holding_days, limit_days | 时间止损触发 |
| `risk.ma_exit_triggered` | code, ma_period, ma_value, current_price | MA 跌破离场 |
| `risk.style_switched` | code, from_style, to_style, trigger | 风格切换 |
| `risk.portfolio_breach` | rule, current_value, limit_value | 组合风控触发 |
| `risk.position_sized` | code, shares, amount, pct, market_multiplier | 仓位计算结果 |
| `risk.blocked` | code, reason, details | 风控阻断 |

### execution 事件

| event_type | payload 关键字段 | 说明 |
|-----------|-----------------|------|
| `order.created` | order_id, code, side, shares, price, broker | 订单创建 |
| `order.filled` | order_id, code, side, shares, fill_price, fee | 订单成交 |
| `order.cancelled` | order_id, reason | 订单取消 |
| `position.opened` | code, shares, avg_cost, style | 开仓 |
| `position.increased` | code, added_shares, new_avg_cost | 加仓 |
| `position.reduced` | code, sold_shares, sell_price, realized_pnl | 减仓 |
| `position.closed` | code, shares, sell_price, realized_pnl, holding_days | 清仓 |

### platform 事件

| event_type | payload 关键字段 | 说明 |
|-----------|-----------------|------|
| `run.started` | run_id, run_type, config_version | 运行开始 |
| `run.completed` | run_id, duration_ms, summary | 运行完成 |
| `run.failed` | run_id, error, traceback | 运行失败 |
