# 数据模型

金额存分（_cents 整数），JSON 放 *_json 字段，projection_* 表可删可重建。

## 核心表

```sql
-- 业务事实 (append-only)
event_log (event_id PK, stream, stream_type, stream_version, event_type, payload_json, metadata_json, occurred_at)
  UNIQUE(stream, stream_version)

-- 规则版本
config_versions (config_version PK, config_hash UNIQUE, config_json, created_at, activated_at)

-- 运行记录
run_log (run_id PK, run_type, scope, config_version, status, started_at, finished_at, error_message, artifacts_json)
```

## 市场观察表

```sql
market_observations (observation_id PK, source, kind, symbol, observed_at, run_id, payload_json)
market_bars (symbol+bar_date+period PK, open/high/low/close_cents, volume, amount_cents, source, fetched_at)
```

## 投影表（全部可删可重建）

```sql
projection_positions (code PK, name, style, shares, avg_cost_cents, entry_date, ...)
projection_orders (order_id PK, code, side, shares, price_cents, status, broker, ...)
projection_balances (scope PK, cash_cents, total_asset_cents, weekly_buy_count, ...)
projection_candidate_pool (code+pool_tier PK, name, score, added_at, streak_days, note)
projection_market_state (index_symbol PK, name, signal, price_cents, change_pct, ...)
report_artifacts (artifact_id PK, run_id, report_type, format, content, delivered_to, created_at)
```

## 事件类型

### strategy
| event_type | 说明 |
|-----------|------|
| score.calculated | 四维评分完成 |
| decision.suggested | 交易意图生成 |
| pool.promoted / demoted / removed | 池子变动 |

### risk
| event_type | 说明 |
|-----------|------|
| risk.stop_loss_triggered | 止损触发 |
| risk.trailing_stop_triggered | 移动止盈触发 |
| risk.time_stop_triggered | 时间止损 |
| risk.ma_exit_triggered | MA 跌破离场 |
| risk.portfolio_breach | 组合风控触发 |
| risk.position_sized | 仓位计算结果 |

### execution
| event_type | 说明 |
|-----------|------|
| order.created / filled / cancelled | 订单生命周期 |
| position.opened / closed | 持仓生命周期 |

### platform
| event_type | 说明 |
|-----------|------|
| run.started / completed / failed | 运行生命周期 |
