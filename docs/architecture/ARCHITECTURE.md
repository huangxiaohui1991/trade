# 架构总览

## 架构图

```
CLI (typer) / MCP Server (FastMCP stdio)
         │
         ▼
┌────────────────────────────────────────────────────────┐
│                    platform                             │
│  EventStore · ConfigRegistry · RunJournal · CLI · MCP   │
│                    SQLite Event Kernel                   │
│  event_log · config_versions · run_log                  │
└────────────────────────┬───────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│   market     │ │  strategy    │ │    risk      │
│ AkShare/MX   │ │ Scorer 纯函数│ │ Rules 纯函数 │
│ adapters     │ │ Decider      │ │ Sizing       │
│ MarketStore  │ │ Classifier   │ │ RiskService  │
│ MarketService│ │ Timer        │ │              │
│              │ │ StrategyServ │ │              │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       └────────────────┼────────────────┘
                        ▼
              ┌──────────────────┐
              │   execution      │
              │ OrderManager     │
              │ PositionManager  │
              │ PositionProjector│
              │ ExecutionService │
              │ SimulatedBroker  │
              │ MXBroker         │
              └────────┬─────────┘
                       ▼
              ┌──────────────────┐
              │   reporting      │
              │ ProjectionUpdater│
              │ ReportGenerator  │
              │ ObsidianProjector│
              │ Discord 格式化   │
              └──────────────────┘
```

## 6 个 Context

| Context | 职责 | IO |
|---------|------|----|
| platform | DB、config 版本、run lifecycle、事件分发、CLI/MCP | SQLite |
| market | 行情/财报/资金流/舆情抓取与标准化 | AkShare/MX HTTP |
| strategy | 评分、决策、风格分类、择时 | 无（纯函数） |
| risk | 止损/止盈/仓位 sizing/组合风控 | 无（纯函数） |
| execution | 订单、持仓、投影重建 | SQLite |
| reporting | 报告生成、Obsidian/Discord 投影 | SQLite + 文件 |

## 核心运行链路

```
1. CreateRun → run_id + freeze config_version
2. CollectMarketData → MarketService.collect_batch() → market_observations
3. RunStrategy → StrategyService.evaluate() → score.calculated + decision.suggested 事件
4. RunRisk → RiskService.assess_position() → risk.* 事件
5. Execute → ExecutionService.execute_buy/sell() → order.* + position.* 事件
6. UpdateProjections → ProjectionUpdater.rebuild_all()
7. EmitReports → ReportGenerator → report_artifacts
8. CompleteRun → run_log status=completed
```

## 目录结构

```
src/hermes/
├── platform/
│   ├── db.py              # SQLite 连接、schema、WAL
│   ├── events.py          # EventStore (append-only)
│   ├── config.py          # ConfigRegistry (版本化 freeze)
│   ├── runs.py            # RunJournal (幂等 lifecycle)
│   ├── cli.py             # typer CLI
│   └── mcp_server.py      # FastMCP Server (13 tools)
├── market/
│   ├── models.py          # StockQuote, TechnicalIndicators, StockSnapshot, ...
│   ├── adapters.py        # Protocol + AkShare/MX adapters
│   ├── store.py           # MarketStore (observations + bars + TTL cache)
│   ├── service.py         # MarketService (并发 + fallback + 限流)
│   └── mx_async.py        # httpx async MX client
├── strategy/
│   ├── models.py          # ScoreResult, DecisionIntent, StyleResult, ...
│   ├── scorer.py          # Scorer 四维评分 (纯函数)
│   ├── decider.py         # Decider 综合决策 (纯函数)
│   ├── classifier.py      # 风格判定 (纯函数)
│   ├── timer.py           # 大盘择时 (纯函数)
│   └── service.py         # StrategyService (评分+决策+事件写入)
├── risk/
│   ├── models.py          # ExitSignal, RiskParams, PositionSize, ...
│   ├── rules.py           # 止损/止盈/时间止损/MA离场 (纯函数)
│   ├── sizing.py          # 仓位计算 (纯函数)
│   └── service.py         # RiskService (风控+事件写入)
├── execution/
│   ├── models.py          # Order, Position, Balance, TradeEvent
│   ├── orders.py          # OrderManager (事件化)
│   ├── positions.py       # PositionManager + PositionProjector
│   └── service.py         # ExecutionService + SimulatedBroker + MXBroker
└── reporting/
    ├── projectors.py      # ProjectionUpdater (event → projection)
    ├── reports.py         # ReportGenerator (盘前/收盘/评分/周报)
    ├── obsidian.py        # ObsidianProjector (vault 投影)
    └── discord.py         # Discord embed 格式化

tests/hermes/
├── platform/              # EventStore, Config, Runs, MCP tools
├── strategy/              # Scorer, Decider, Classifier, Timer, StrategyService
├── risk/                  # Rules, Sizing, RiskService
├── market/                # MarketStore, MarketService
├── execution/             # Orders, Positions, Projections, ExecutionService
└── reporting/             # Projectors, Reports, Discord, Obsidian
```

## MCP Tools (13 个)

| Tool | 说明 |
|------|------|
| trade_market_signal | 大盘择时信号 |
| trade_score_stock | 单股四维评分 |
| trade_score_batch | 批量评分 |
| trade_portfolio | 当前持仓 |
| trade_pool_status | 核心池/观察池 |
| trade_check_risk | 单票风控 |
| trade_check_portfolio_risk | 组合风控 |
| trade_calc_position | 仓位计算 |
| trade_screener | 选股筛选 |
| trade_score_history | 历史评分 |
| trade_trade_events | 交易记录 |
| trade_run_pipeline | 运行 pipeline |
| trade_backtest | 策略回测 |

## 设计约束

- strategy/ 和 risk/ 不 import HTTP/SQL/YAML/文件系统
- 所有 projection_* 表可从 event_log 完全重建
- 金额字段用 _cents 整数
- 每次 run 冻结 config_version + run_id
- reporting/ 不反写业务表
