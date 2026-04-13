# 终极版本架构设计

**日期：** 2026-04-13
**状态：** 已批准

---

## 1. 目标

将现有 A 股交易系统演进为：
- **单一数据源** — PostgreSQL 替换 SQLite，消除 Obsidian 双写
- **Web Dashboard** — HTML 页面展示持仓、收益、信号、风控，手动刷新
- **REST API** — FastAPI 封装 Python engine，Hermes 通过 HTTP 调用，不再读内部脚本
- **Docker 部署** — 一键启动 API + PostgreSQL
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
| **HTML Dashboard** | 展示持仓、收益、信号、风控，手动刷新 |
| **FastAPI** | REST API 层，Hermes 和 Web 共用，不含业务逻辑 |
| **Python Engine** | 现有逻辑（scorer/risk/trading），不做改动 |
| **PostgreSQL** | 单一数据源，替换 SQLite |
| **hermes_cron.sh** | 保留在宿主机，继续调用 `bin/trade` |

---

## 4. API 端点设计

### 组合与持仓

| Method | Endpoint | 说明 |
|--------|----------|------|
| GET | `/portfolio/summary` | 组合总览：持仓/现金/总资产/今日盈亏 |
| GET | `/positions` | 当前持仓列表，含成本/现价/盈亏 |
| GET | `/positions/{stock_code}` | 单只持仓详情 |

### 信号与决策

| Method | Endpoint | 说明 |
|--------|----------|------|
| GET | `/signals/today` | 今日信号列表 |
| GET | `/池子/core` | 核心池状态及评分 |
| GET | `/池子/watch` | 观察池状态 |

### 风控

| Method | Endpoint | 说明 |
|--------|----------|------|
| GET | `/risk/exposure` | 当前风险敞口 |
| GET | `/risk/stops` | 止损单状态 |

### 订单

| Method | Endpoint | 说明 |
|--------|----------|------|
| POST | `/orders` | 下单（Hermes 调用） |
| GET | `/orders` | 订单历史 |

### 报告

| Method | Endpoint | 说明 |
|--------|----------|------|
| GET | `/reports/weekly` | 周报 |
| GET | `/reports/monthly` | 月报 |

---

## 5. 数据迁移

### 迁移路径

SQLite (`data/ledger/trade_state.sqlite3`) → PostgreSQL

### 迁移方式

一次性迁移脚本：
- 读取 SQLite 各表
- 写入 PostgreSQL 对应表
- 验证数据一致性

### 迁移后

- 停止写 Obsidian
- HTML Dashboard 直接读 PostgreSQL
- Hermes 通过 API 读取数据，不再读内部脚本

---

## 6. 部署设计

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
      - ./html:/app/html  # HTML Dashboard

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

### 目录结构（新增）

```
a-stock-trading/
├── Dockerfile
├── docker-compose.yml
├── html/
│   └── dashboard.html    # Web Dashboard
├── app/
│   ├── __init__.py
│   ├── main.py           # FastAPI 入口
│   ├── routers/          # API 路由
│   │   ├── portfolio.py
│   │   ├── positions.py
│   │   ├── signals.py
│   │   ├── risk.py
│   │   ├── orders.py
│   │   └── reports.py
│   └── db/
│       ├── connection.py  # PG 连接
│       └── models.py      # 数据模型
├── scripts/               # 现有逻辑不动
├── data/
│   ├── ledger/            # 迁移后废弃
├── config/
├── trade-vault/           # 迁移后停止写入
└── docs/superpowers/specs/
    └── 2026-04-13-ultimate-version-design.md  # 本文档
```

---

## 7. Hermes 交互升级

### 现状

Hermes 读 `scripts/cli/trade.py` 内部脚本解析 JSON 输出

### 目标

Hermes 通过 HTTP API 获取数据，调用动作

### 变化

| 场景 | 现状 | 目标 |
|------|------|------|
| 查询持仓 | 读脚本 + 解析 | `GET /positions` |
| 查询信号 | 读脚本 + 解析 | `GET /signals/today` |
| 下单 | 读脚本 + 解析 | `POST /orders` |
| 风控检查 | 读脚本 + 解析 | `GET /risk/exposure` |

---

## 8. 实施步骤

1. 新增 `app/` 目录，建立 FastAPI 骨架
2. 新增 `html/dashboard.html`，简单展示页面
3. 建立 PostgreSQL 连接层
4. 实现核心 API 端点（先 `/positions` 和 `/portfolio/summary`）
5. 数据迁移：SQLite → PostgreSQL
6. 停止 Obsidian 双写
7. 验证 Hermes API 交互

---

## 9. 未纳入本设计的内容

- WebSocket 实时推送（本版本为手动刷新）
- 多用户/权限系统（本版本为单机使用）
- 历史数据迁移细节（待定）
- 回测模块改动（本版本不涉及）

---

## 10. 决策记录

| 决策 | 选择 | 原因 |
|------|------|------|
| 数据库 | PostgreSQL | 结构化关系型数据，SQL 能力强，适合分析 |
| Web 刷新 | 手动刷新 | 用户偶尔查看，无需实时推送 |
| 部署方式 | Docker Compose | 一键启动，便携 |
| Cron | 保留系统级 cron | 与容器解耦，宿主机管理 |
| API 设计 | REST 资源型 | 接口清晰，Hermes 易理解 |
| Hermes 交互 | HTTP API | 消除读内部脚本 |