# 终极版本架构设计

**日期：** 2026-04-13
**状态：** 已批准（v2，完整覆盖）

---

## 1. 目标

将现有 A 股交易系统演进为：
- **单一数据源** — PostgreSQL 替换 SQLite，消除 Obsidian 双写
- **Web Dashboard** — HTML 页面展示全部数据，手动刷新
- **REST API** — FastAPI 封装全部 Python engine 功能，Hermes 通过 HTTP 调用，不再读内部脚本
- **Docker 部署** — 一键启动 API + PostgreSQL + HTML
- **系统级 cron 保留** — `hermes_cron.sh` 继续在宿主机运行，调用 `bin/trade`

---

## 2. 系统架构

```
                    ┌─────────────────────────────────┐
                    │         用户视角                 │
                    └─────────────────────────────────┘
                              │              ▲
                              ▼              │
                    ┌──────────────────┐      │
                    │   HTML Web       │      │
                    │   Dashboard      │      │
                    └────────┬─────────┘      │
                             │ HTTP           │
                    ┌────────▼─────────┐      │
                    │   FastAPI         │      │
                    │   REST API        │◄─────┘ Hermes
                    └────────┬─────────┘        (HTTP calls)
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
    ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
    │  scripts/    │ │  scripts/    │ │  scripts/    │
    │  engine/     │ │  pipeline/   │ │  state/     │
    │  (核心逻辑)   │ │  (定时任务)   │ │  (状态管理)   │
    └──────────────┘ └──────────────┘ └──────────────┘
                             │
                    ┌────────▼─────────┐
                    │   PostgreSQL     │
                    │   (单一数据源)     │
                    └───────────────────┘
```

---

## 3. 组件职责

| 组件 | 职责 |
|------|------|
| **HTML Dashboard** | 展示所有数据（持仓/收益/信号/风控/池子/报告），手动刷新 |
| **FastAPI** | REST API 层，全部现有功能封装为端点，Hermes 和 Web 共用 |
| **Python Engine** | 现有逻辑全部保留（scorer/risk/data_engine/composite/pipeline） |
| **PostgreSQL** | 单一数据源，替换 SQLite + Obsidian |
| **hermes_cron.sh** | 保留在宿主机，继续调用 `bin/trade` |

---

## 4. 多账号 scope

系统支持三个独立的账号/组合：

| Scope | 说明 |
|-------|------|
| `cn_a_system` | A 股实盘 |
| `hk_legacy` | 港股遗留仓位 |
| `paper_mx` | 妙想模拟盘 |

所有 API 端点均接受 `X-Account-Scope` header 或 query 参数，默认 `cn_a_system`。

---

## 5. API 端点设计（完整覆盖）

### 5.1 系统状态

| Method | Endpoint | 说明 |
|--------|----------|------|
| GET | `/health` | 系统健康检查（同 `trade doctor`） |
| GET | `/status/today` | 今日状态汇总（同 `trade status today`） |

### 5.2 组合与持仓

| Method | Endpoint | 说明 |
|--------|----------|------|
| GET | `/portfolio/summary` | 组合总览：持仓/现金/总资产/今日盈亏 |
| GET | `/positions` | 当前持仓列表，含成本/现价/盈亏 |
| GET | `/positions/{stock_code}` | 单只持仓详情 |
| GET | `/orders` | 订单历史 |

### 5.3 信号与决策

| Method | Endpoint | 说明 |
|--------|----------|------|
| GET | `/signals/today` | 今日信号列表 |
| GET | `/池子/core` | 核心池状态及评分 |
| GET | `/池子/watch` | 观察池状态 |
| GET | `/market/timing` | 大盘择时信号（GREEN/YELLOW/RED/CLEAR） |

### 5.4 评分引擎

| Method | Endpoint | 说明 |
|--------|----------|------|
| POST | `/score/single` | 单只评分（body: `{"stock_code": "000001"}`）|
| POST | `/score/batch` | 批量评分（body: `{"stock_codes": [...]}`）|
| POST | `/score/pool` | 池子评分（body: `{"pool": "core" \| "watch"}`）|

### 5.5 风控

| Method | Endpoint | 说明 |
|--------|----------|------|
| GET | `/risk/exposure` | 当前风险敞口 |
| GET | `/risk/check` | 组合风控检查 |
| GET | `/risk/position-size` | 仓位计算 |
| GET | `/risk/stop-loss` | 止损状态 |
| GET | `/risk/should-exit` | 是否应退出 |

### 5.6 市场数据

| Method | Endpoint | 说明 |
|--------|----------|------|
| GET | `/data/technical/{stock_code}` | 技术指标数据 |
| GET | `/data/financial/{stock_code}` | 基本面数据 |
| GET | `/data/flow/{stock_code}` | 资金流向数据 |
| GET | `/data/realtime/{stock_code}` | 实时行情 |
| GET | `/data/market-index` | 大盘指数（沪深/港股） |

### 5.7 筛选与选股

| Method | Endpoint | 说明 |
|--------|----------|------|
| POST | `/screen/tracked` | 追踪选股（同 `stock_screener tracked`）|
| POST | `/screen/market` | 市场选股（同 `stock_screener market`）|
| GET | `/blacklist` | 黑名单列表 |

### 5.8 影子交易

| Method | Endpoint | 说明 |
|--------|----------|------|
| GET | `/shadow/status` | 影子交易状态 |
| GET | `/shadow/check-stops` | 检查止损单 |
| POST | `/shadow/buy-new-picks` | 买入新标的 |
| POST | `/shadow/reconcile` | 对账 |
| GET | `/shadow/report` | 影子交易报告 |

### 5.9 回测

| Method | Endpoint | 说明 |
|--------|----------|------|
| POST | `/backtest/run` | 运行回测（body: 参数配置）|
| POST | `/backtest/sweep` | 参数扫描 |
| POST | `/backtest/walk-forward` |  walk-forward 分析 |
| POST | `/backtest/strategy-replay` | 信号驱动回放 |
| GET | `/backtest/history` | 历史回测记录 |
| POST | `/backtest/compare` | 策略对比 |

### 5.10 MX 数据源（妙想/东方财富）

| Method | Endpoint | 说明 |
|--------|----------|------|
| GET | `/mx/search?q=` | 资讯搜索 |
| GET | `/mx/xuangu` | 智能选股结果 |
| GET | `/mx/zixuan` | 自选股列表 |
| GET | `/mx/moni` | 模拟盘持仓/历史 |
| GET | `/mx/data/{stock_code}` | 妙想行情数据 |
| POST | `/mx/llm-judge` | LLM 评分判断 |

### 5.11 数据同步

| Method | Endpoint | 说明 |
|--------|----------|------|
| POST | `/state/sync` | 同步状态到数据库 |

### 5.12 报告

| Method | Endpoint | 说明 |
|--------|----------|------|
| GET | `/reports/weekly` | 周报 |
| GET | `/reports/monthly` | 月报 |

### 5.13 Pipeline 触发（Hermes 主动执行）

| Method | Endpoint | 说明 |
|--------|----------|------|
| POST | `/pipeline/run` | 运行 pipeline（body: `{"name": "morning\|noon\|evening\|scoring\|sentiment\|hk_monitor\|monthly"}`）|
| POST | `/pipeline/orchestrate` | 编排工作流（body: `{"workflow": "morning_brief\|noon_check\|close_review\|weekly_review"}`）|

---

## 6. 数据模型（PostgreSQL Schema）

基于现有 SQLite 表结构设计：

```sql
-- 组合与持仓
portfolio_balances (account_scope, cash, total_asset, updated_at)
portfolio_positions (account_scope, stock_code, shares, cost, current_price, unrealized_pnl, ...)

-- 交易记录
trade_events (account_scope, stock_code, action, shares, price, timestamp, ...)

-- 信号与评分
signal_snapshots (stock_code, signal_type, score, reason_codes, timestamp, account_scope)
decision_snapshots (account_scope, decisions, timestamp)

-- 池子
candidate_snapshots (pool_type, stock_code, scores_4d, timestamp)

-- 风控
alert_center (alert_type, stock_code, message, severity, timestamp, account_scope)

-- 订单
orders (order_id, account_scope, stock_code, action, shares, price, status, created_at)

-- 元数据
state_meta (key, value, updated_at)
```

---

## 7. 数据迁移

### 迁移路径

SQLite (`data/ledger/trade_state.sqlite3`) → PostgreSQL

### 迁移方式

一次性迁移脚本 `scripts/migration/sqlite_to_pg.py`：
- 读取 SQLite 各表
- 写入 PostgreSQL 对应表
- 验证数据一致性

### 迁移后

- `data/ledger/` 目录保留但不再写入
- `trade-vault/` 目录停止写入（作为历史存档）
- 所有查询走 PostgreSQL

---

## 8. 部署设计

### Docker Compose 结构

```yaml
services:
  api:
    build: .
    ports:
      - "8000:8000"
    depends_on:
      - db
    volumes:
      - ./html:/app/html

  db:
    image: postgres:16
    environment:
      POSTGRES_DB: stock_trading
      POSTGRES_USER: xxx
      POSTGRES_PASSWORD: xxx
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
```

### 目录结构

```
a-stock-trading/
├── Dockerfile
├── docker-compose.yml
├── html/
│   └── dashboard.html
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── routers/
│   │   ├── health.py
│   │   ├── status.py
│   │   ├── portfolio.py
│   │   ├── positions.py
│   │   ├── signals.py
│   │   ├── pool.py          # 池子
│   │   ├── market_timing.py
│   │   ├── score.py
│   │   ├── risk.py
│   │   ├── data.py          # 市场数据
│   │   ├── screen.py        # 筛选选股
│   │   ├── shadow.py        # 影子交易
│   │   ├── backtest.py
│   │   ├── mx.py            # MX 数据源
│   │   ├── orders.py
│   │   ├── reports.py
│   │   ├── pipeline.py
│   │   └── state.py
│   ├── db/
│   │   ├── connection.py
│   │   └── models.py
│   └── core/
│       └── client.py         # 封装 scripts/ 调用
├── scripts/                  # 现有逻辑不动
├── data/                     # 迁移后 legacy
├── trade-vault/              # 迁移后停止写入
├── docs/superpowers/specs/
│   └── 2026-04-13-ultimate-version-design.md
└── requirements.txt
```

---

## 9. Hermes 交互升级（完整对比）

| 场景 | 现状 | 目标 |
|------|------|------|
| 系统健康检查 | 读脚本 + 解析 | `GET /health` |
| 状态汇总 | 读脚本 + 解析 | `GET /status/today` |
| 查询持仓 | 读脚本 + 解析 | `GET /positions` |
| 查询信号 | 读脚本 + 解析 | `GET /signals/today` |
| 大盘择时 | 读脚本 + 解析 | `GET /market/timing` |
| 评分单只 | 读脚本 + 解析 | `POST /score/single` |
| 评分批量 | 读脚本 + 解析 | `POST /score/batch` |
| 风控检查 | 读脚本 + 解析 | `GET /risk/check` |
| 下单 | 读脚本 + 解析 | `POST /orders` |
| 选股 | 读脚本 + 解析 | `POST /screen/tracked` |
| 影子交易 | 读脚本 + 解析 | `GET /shadow/status` |
| 回测 | 读脚本 + 解析 | `POST /backtest/run` |
| MX 数据 | 读脚本 + 解析 | `GET /mx/search` 等 |
| 触发 pipeline | 读脚本 + 解析 | `POST /pipeline/run` |
| 周报 | 读脚本 + 解析 | `GET /reports/weekly` |

---

## 10. Discord 推送保留

现有 Discord 推送逻辑（盘前 8:25 / 午休 11:55 / 收盘 15:35 / 舆情 30min / 周报 Sun 20:00）保持不变，由 `scripts/utils/discord_push.py` 实现，不走 API 层。

---

## 11. Cron 任务保留

`hermes_cron.sh` 继续在宿主机运行，通过 `bin/trade` 调用 CLI：

```
08:25  trade orchestrate morning_brief
11:55  trade orchestrate noon_check
15:35  trade orchestrate close_review
15:40  trade run scoring
30min  trade run sentiment
16:30  trade run hk_monitor
Sun 20:00 trade orchestrate weekly_review
每月28日 trade run monthly
```

---

## 12. 未纳入本设计的内容

- WebSocket 实时推送（手动刷新）
- 多用户/权限系统（单机使用）
- 订单实盘对接（本版本为记录管理，不涉及真实下单通道）

---

## 13. 实施步骤

1. 新增 `app/` 目录，建立 FastAPI 骨架
2. 建立 PostgreSQL 连接层 + Schema
3. 实现 API 端点（按模块逐个接入现有 scripts/ 逻辑）
4. 新增 `html/dashboard.html`，覆盖所有展示
5. 数据迁移：SQLite → PostgreSQL
6. 停止 Obsidian 双写
7. 停止写入 `data/ledger/`
8. 验证 Hermes API 交互
9. Docker 构建测试

---

## 14. 决策记录

| 决策 | 选择 | 原因 |
|------|------|------|
| 数据库 | PostgreSQL | 结构化关系型，SQL 强，适合分析 |
| Web 刷新 | 手动刷新 | 偶尔查看，无需实时 |
| 部署方式 | Docker Compose | 一键启动 |
| Cron | 保留系统级 cron | 与容器解耦 |
| API 设计 | REST 资源型 + action 端点 | 覆盖全部现有功能 |
| Discord | 保持独立 | 不走 API，仍由 Python 脚本驱动 |

---

## 15. 事件溯源架构（Event Sourcing + CQRS）

**状态：提议阶段（v3 新增）**

### 15.1 现状问题

现有状态管理层是 **mutable snapshot**：

```
orders 表（现状）
order_id | code | side | price | shares | status | updated_at
    ↓ 直接覆写
状态变了，但"怎么变成这个状态"的历史丢了
```

```
positions 表（现状）
stock_code | shares | cost | updated_at
    ↓ 直接覆写
买入→加仓→减仓→清仓，每一步的"为什么"丢了
```

后果：
- **无法复盘**："当时为什么买？信号是什么？" → 查不到
- **审计失效**："这个止损是什么时候触发的？" → 不知道
- **对账困难**："持仓数字对不对？" → 没有事件链可以回放验证
- **跨表不一致**：`orders`/`positions`/`trade_events` 三张表各写各的，没有统一事件流

### 15.2 目标：用 Event Store 重建状态层

所有状态变化都先写入 **Append-only Event Store**，再由 Projector 投影到 Read Model：

```
  COMMAND                    EVENT STORE（写）              PROJECTOR              READ MODEL（读）
  ──────                     ─────────────────              ─────────              ─────────────
  fill_order()      ──▶     [OrderFilled]          ──▶    PositionProjector  ──▶  positions 表
  record_stop()     ──▶     [StopTriggered]        ──▶    OrderProjector      ──▶  orders 表
  market_close()     ──▶     [MarketClosed]         ──▶    DailyJournalProjector──▶  journal 文件
```

**核心原则**：
- Event Store 是唯一真相来源（Source of Truth）
- Read Model（positions/orders）永远从 Event Store 重建，不反向写入
- 每个事件带 `aggregate_id` + `version` 实现乐观并发锁
- 事件不可变（Immutable），只能追加（Append-only）

### 15.3 Event Store Schema（PostgreSQL）

```sql
CREATE TABLE event_store (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_id    VARCHAR(128) NOT NULL,      -- 聚合根 ID，如 "ORDER:600036:BUY:20260414"
    aggregate_type  VARCHAR(64) NOT NULL,        -- "Order" | "Position"
    event_type      VARCHAR(128) NOT NULL,        -- "OrderFilled" | "StopTriggered" 等
    payload         JSONB NOT NULL,               -- 事件数据（业务含义）
    metadata        JSONB DEFAULT '{}',           -- 触发源/actor/时间戳等
    version         INTEGER NOT NULL,             -- 聚合版本号，单调递增
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(aggregate_id, version)                 -- 防止重复写入
);

CREATE INDEX idx_event_store_aggregate ON event_store(aggregate_id, version);
CREATE INDEX idx_event_store_type ON event_store(event_type, created_at);
```

**payload 示例（OrderFilled）**：
```json
{
  "order_id": "ORD-20260414-001",
  "stock_code": "600036",
  "stock_name": "招商银行",
  "side": "buy",
  "price": 36.696,
  "shares": 500,
  "broker": "eastmoney",
  "scope": "cn_a_system",
  "filled_at": "2026-04-14T09:32:15+08:00",
  "signal_codes": ["BREAK_MA20", "GREEN_MARKET"],
  "reason_codes": [],
  "actor": "cli_order_fill"
}
```

### 15.4 Aggregate 聚合根定义

#### 15.4.1 Order 聚合根

```
状态机：
┌──────────┐  submit  ┌──────────┐  partial_fill  ┌────────────────┐
│  NONE    │ ──────▶ │  PENDING │ ─────────────▶ │ PARTIAL_FILLED │
└──────────┘         └──────────┘                └───────┬────────┘
                            │                              │
                            │ cancel                       │ fill
                            ▼                              ▼
                      ┌──────────┐                  ┌──────────┐
                      │CANCELLED│                  │  FILLED  │
                      └──────────┘                  └──────────┘
                            │
                     expire/timeout
                            │
                            ▼
                      ┌──────────┐
                      │ EXPIRED  │
                      └──────────┘
```

**事件列表**：

| 事件 | 触发条件 | payload 关键字段 |
|------|---------|----------------|
| `OrderSubmitted` | 用户/策略提交订单 | code, side, price, shares, signal_codes |
| `OrderFilled` | 成交确认 | price, shares_filled, broker_fill_id |
| `OrderPartialFill` | 部分成交 | price, shares_filled, shares_remaining |
| `OrderCancelled` | 用户取消 | cancel_reason |
| `OrderExpired` | 收盘未成交自动作废 | expiry_reason |
| `StopTriggered` | 止损信号触发 | stop_type, triggered_price |

**Command（写）**：
```python
class OrderAggregate:
    def submit_order(cmd: SubmitOrderCommand) -> list[Event]:
        # 验证：非 ST、非涨停（卖出除外）、非黑名单
        # 生成：OrderSubmitted 事件
        pass

    def fill_order(cmd: FillOrderCommand, expected_version: int) -> list[Event]:
        # 乐观锁：if current_version != expected_version → raise ConcurrencyError
        # 状态校验：只有 PENDING 才能 fill
        # 生成：OrderFilled 或 OrderPartialFill 事件
        pass
```

#### 15.4.2 Position 聚合根

每个 `stock_code + scope` 是一个 Position 聚合根。

```
事件流（时间顺序）：
[PositionOpened]  ──▶  [BuyExecuted × N]  ──▶  [SellExecuted]  ──▶  [PositionClosed]
                            │                        │
                            └──────▶ [StopTriggered] ←┘
```

**事件列表**：

| 事件 | 说明 | payload |
|------|------|---------|
| `PositionOpened` | 首次买入开仓 | code, name, price, shares, cost_basis |
| `BuyExecuted` | 加仓 | price, shares, new_total, avg_cost |
| `SellExecuted` | 减仓/清仓 | price, shares_sold, remaining, realized_pnl |
| `StopTriggered` | 止损触发 | stop_type, triggered_price, reason_code |
| `DividendReceived` | 分红 | amount, tax |
| `CostAdjusted` | 手动成本修正 | old_cost, new_cost, reason |

**Projector 重建 Position Read Model**：
```python
class PositionProjector:
    def project(aggregate_id: str) -> PositionSnapshot:
        events = event_store.get_events(aggregate_id)  # 按 version 排序
        state = Position.empty()
        for event in events:
            state = state.apply(event)  # 纯函数，无副作用
        return state.to_snapshot()      # 写入 positions 投影表
```

### 15.5 CQRS 分离

```
COMMAND SIDE（写）                        QUERY SIDE（读）
─────────────────                        ───────────────
bin/trade order fill ──▶ OrderAggregate ──▶ Event Store
                                     │
                                     │ Projector（异步）
                                     ▼
bin/trade score single ──▶ ScoreEngine ──▶ signal_snapshots 表
bin/trade risk check   ──▶ RiskEngine   ──▶ alert_center 表
```

**读模型表（PostgreSQL）**：
```sql
-- 持仓快照（从 PositionProjector 持续投影）
CREATE TABLE positions (
    id SERIAL PRIMARY KEY,
    account_scope VARCHAR(32) NOT NULL,
    stock_code VARCHAR(16) NOT NULL,
    stock_name VARCHAR(128),
    shares INTEGER NOT NULL DEFAULT 0,
    cost_basis DECIMAL(12,4) NOT NULL DEFAULT 0,  -- 加权平均成本
    market_value DECIMAL(14,4),
    unrealized_pnl DECIMAL(14,4),
    realized_pnl DECIMAL(14,4) DEFAULT 0,
    stop_price DECIMAL(10,4),
    stop_type VARCHAR(32),
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    version INTEGER NOT NULL,
    UNIQUE(account_scope, stock_code)
);

-- 订单快照
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(64) UNIQUE NOT NULL,
    account_scope VARCHAR(32) NOT NULL,
    stock_code VARCHAR(16) NOT NULL,
    side VARCHAR(8) NOT NULL,
    price DECIMAL(10,4) NOT NULL,
    shares INTEGER NOT NULL,
    shares_filled INTEGER DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    signal_codes JSONB DEFAULT '[]',
    reason_codes JSONB DEFAULT '[]',
    broker_order_id VARCHAR(128),
    broker_fill_id VARCHAR(128),
    submitted_at TIMESTAMPTZ,
    filled_at TIMESTAMPTZ,
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    version INTEGER NOT NULL,
    UNIQUE(account_scope, stock_code, submitted_at)
);

-- 事件溯源专用索引
CREATE INDEX idx_positions_scope ON positions(account_scope);
CREATE INDEX idx_orders_scope_status ON orders(account_scope, status);
CREATE INDEX idx_orders_pending ON orders(status) WHERE status IN ('PENDING', 'PARTIAL_FILLED');
```

### 15.6 Command Handler 实现骨架

```python
# app/commands/order_commands.py
from dataclasses import dataclass
from typing import Literal

@dataclass
class SubmitOrderCommand:
    stock_code: str
    side: Literal["buy", "sell"]
    price: float
    shares: int
    signal_codes: list[str]
    scope: str = "cn_a_system"
    actor: str = "cli"

@dataclass
class FillOrderCommand:
    order_id: str
    price: float
    shares: int
    broker_fill_id: str
    scope: str

class OrderCommandHandler:
    def __init__(self, event_store: 'EventStore'):
        self.event_store = event_store

    def handle_submit(self, cmd: SubmitOrderCommand) -> OrderSnapshot:
        agg_id = f"ORDER:{cmd.stock_code}:{cmd.side}:{date.today().isoformat()}"
        aggregate = OrderAggregate.reconstitute(self.event_store, agg_id)
        events = aggregate.submit(cmd)
        self.event_store.append(agg_id, events)
        return aggregate.to_snapshot()

    def handle_fill(self, cmd: FillOrderCommand) -> tuple[OrderSnapshot, PositionSnapshot]:
        agg_id = self._find_order_aggregate(cmd.order_id)
        order_agg = OrderAggregate.reconstitute(self.event_store, agg_id)
        order_events = order_agg.fill(cmd)
        self.event_store.append(agg_id, order_events)

        pos_agg_id = f"POSITION:{cmd.stock_code}:{cmd.scope}"
        pos_agg = PositionAggregate.reconstitute(self.event_store, pos_agg_id)
        pos_events = pos_agg.apply_trade(cmd)
        self.event_store.append(pos_agg_id, pos_events)

        return order_agg.to_snapshot(), pos_agg.to_snapshot()
```

### 15.7 三阶段迁移策略

**Phase 1（数据层热身）：Event Store 建表 + 双写（向后兼容）**

```
现状                            Phase 1
orders ──▶ SQLite              orders ──▶ Event Store（新增）
                              orders ──▶ SQLite（保留）
```
- 新建 `event_store` 表
- `record_trade_event()` 同时写入 SQLite + event_store
- 现有 pipeline 零改动
- 验收：event_store 数据与 SQLite 数据完全一致

**Phase 2（读取侧迁移）**

```
Phase 1                          Phase 2
SQLite orders（真相）             event_store（真相）
                                     │
                                     │ PositionProjector 重建
                                     ▼
                                  positions 表（投影）
```
- 实现 `PositionProjector` 和 `OrderProjector`
- 对比 event_store 重建的 positions 与 SQLite 原始数据
- 差异为 0 时，切换读路径到投影表
- 保留 SQLite 写路径（Phase 3 移除）

**Phase 3（完全迁移）**

```
Phase 2                          Phase 3（最终态）
写入 ──▶ SQLite + event_store    写入 ──▶ event_store（唯一写入路径）
读出 ──▶ 投影表                  读出 ──▶ 投影表（从 event_store 重建）
```
- 移除 SQLite 写入逻辑
- `record_trade_event()` 只写 event_store
- 删除或归档 SQLite `orders`/`positions` 表
- Event Store 成为单一真相来源

### 15.8 事件重放（Replay）场景

Event Store 的真正威力在于**任意时间点重放**：

```python
# 回到任意日期重建持仓状态
def rebuild_position_at(code: str, scope: str, as_of: date) -> PositionSnapshot:
    events = event_store.get_events_before(
        f"POSITION:{code}:{scope}",
        as_of=as_of  # WHERE created_at <= as_of
    )
    state = Position.empty()
    for event in events:
        state = state.apply(event)
    return state

# 用于对账：持仓快照 vs券商报告
snapshot = rebuild_position_at("600036", "cn_a_system", as_of=date.today())
assert snapshot.shares == broker_report.shares  # 对不上则触发告警
```

### 15.9 与 FastAPI 的整合

```
Hermes/Cron                       FastAPI                         Event Layer
─────────────                     ────────                         ───────────
bin/trade order fill
      │                           CommandHandler
      │                                │
      ▼                                ▼
 POST /orders/fill  ──▶     OrderCommandHandler.handle_fill()
                                  │
                                  ▼ Event append
                            EventStore.append()
                                  │
                                  │ async, 非阻塞
                                  ▼ Projector
                            PositionProjector.project()
                                  │
                                  ▼
                            positions 投影表更新
                                  │
                                  ▼
                            GET /positions 读投影表返回
```

**向后兼容接口**：
- `/orders/fill`（POST）→ CommandHandler → EventStore
- `/orders`（GET）→ 读 orders 投影表（Phase 2+）
- `/positions`（GET）→ 读 positions 投影表（Phase 2+）

### 15.10 决策记录（续）

| 决策 | 选择 | 原因 |
|------|------|------|
| 事件格式 | JSONB payload | 灵活，前向兼容，PostgreSQL 原生支持 |
| 聚合粒度 | Order + Position 两个聚合根 | 避免跨聚合事务，Order 事件不影响 Position |
| 并发控制 | 乐观锁（version 字段） | 轻量，无锁竞争，适合低并发场景 |
| 投影方式 | 同步投影（写入后立即更新） | 简单，满足当前性能需求 |
| 迁移策略 | 三阶段双写 | 零停机，每阶段可回滚 |

### 15.11 实施优先级与工作量

| 阶段 | 工作内容 | 估计（人时） | 收益 |
|------|---------|------------|------|
| Phase 0 | Event Store Schema + EventStore Python 类 | 4h | 骨架就位 |
| Phase 0 | Order Aggregate（含状态机） | 6h | 订单流完整 |
| Phase 1 | 双写 + Position Projector | 4h | 历史数据开始积累 |
| Phase 2 | 读取侧迁移到投影表 | 6h | 读性能提升 |
| Phase 3 | 清理 SQLite 写入 | 2h | 架构统一 |

**Phase 0 + Phase 1 = 14h，可以独立完成，MVP 阶段可先行交付。** Phase 2/3 依赖 FastAPI 上线节奏。