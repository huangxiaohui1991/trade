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