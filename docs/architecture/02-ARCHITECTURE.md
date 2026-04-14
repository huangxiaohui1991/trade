# 02 — 架构总览

## 一句话定版

```
SQLite 事件内核 + 6 个粗粒度业务 Context + 可重建 Projection 的模块化单体
```

复杂度放在模型里，不放在运维里。

## 终局架构图

```
CLI / Hermes Agent (MCP)
         │
         ▼
┌────────────────────────────────────────────────────────┐
│                    platform                             │
│  run lifecycle · config versioning · event dispatch     │
│  ┌──────────┐ ┌──────────┐ ┌────────┐ ┌───────────┐  │
│  │ runs.py  │ │config.py │ │events.py│ │  cli.py   │  │
│  │ run_id   │ │版本化冻结 │ │event_log│ │  MCP入口  │  │
│  └────┬─────┘ └────┬─────┘ └────┬───┘ └───────────┘  │
│       │            │            │                       │
│  ─────┴────────────┴────────────┴───────────────────── │
│                    SQLite Event Kernel                   │
│  event_log · config_versions · run_log                  │
└────────────────────────┬───────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│   market     │ │  strategy    │ │    risk      │
│              │ │              │ │              │
│ 行情/财报/   │ │ 粗筛/评分/   │ │ 单票风控/    │
│ 资金流/舆情  │ │ 风格分类/    │ │ 组合风控/    │
│ 抓取+标准化  │ │ 择时/决策    │ │ 仓位sizing   │
│              │ │              │ │              │
│ market_      │ │ 纯函数内核   │ │ 纯函数内核   │
│ observations │ │ 无IO依赖     │ │ 无IO依赖     │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │                │                │
       └────────────────┼────────────────┘
                        ▼
              ┌──────────────────┐
              │   execution      │
              │                  │
              │ 订单/成交/持仓/  │
              │ 资金/PnL/对账    │
              │                  │
              │ → event_log      │
              │ → projection_*   │
              └────────┬─────────┘
                       ▼
              ┌──────────────────┐
              │   reporting      │
              │                  │
              │ 日报/周报/月报   │
              │ Obsidian 投影    │
              │ Discord 投影     │
              │                  │
              │ 只读消费事实     │
              │ 不反写业务真相   │
              └──────────────────┘
```

## 6 个 Context

| Context | 职责 | 持久化重点 |
|---------|------|-----------|
| `platform` | DB、config 版本、run_id、事件分发、CLI/MCP 接口 | `event_log`、`run_log`、`config_versions` |
| `market` | 行情/财报/资金流/新闻/舆情抓取与标准化，时序存储 | `market_observations`、`market_bars` |
| `strategy` | 粗筛、评分、风格分类、择时、交易意图生成 | 事件化结果写入 `event_log` |
| `risk` | 单票风控、组合风控、仓位 sizing、阻断原因 | 事件化结果写入 `event_log` |
| `execution` | 订单、成交、持仓、资金、PnL、对账 | `event_log` 投影到 `projection_orders`/`positions`/`balances` |
| `reporting` | 当前状态视图、日报/周报、Obsidian/Discord 投影 | `projection_*`、`report_artifacts` |

## 核心运行链路

```
CLI / Hermes Agent
  │
  ▼
1. CreateRun
   → 生成 run_id
   → 冻结 config_version (deep copy + hash)
   → 写入 run_log (status=running)

2. CollectMarketObservations
   → market context 抓取数据
   → 标准化为 StockSnapshot
   → 追加到 market_observations
   → 记录 data_snapshot_ref

3. RunStrategyEvaluation
   → strategy context 纯函数评分
   → 输出 ScoreResult、DecisionIntent
   → 每个结果带 run_id + config_version + data_snapshot_ref
   → 追加到 event_log

4. RunRiskAssessment
   → risk context 纯函数风控
   → 输出 RiskAdjustment 或 BlockedReason
   → 追加到 event_log

5. Execute (如果有交易意图)
   → execution context 生成 OrderIntent
   → 调用 broker adapter
   → 回报 OrderFilled / OrderCancelled
   → 追加到 event_log

6. UpdateProjections
   → 从 event_log 同步更新 projection 表
   → 当前持仓、当前池子、当前资金

7. EmitReports
   → reporting context 消费事实和投影
   → 生成日报、Discord 消息、Obsidian 页面
   → 写入 report_artifacts

8. CompleteRun
   → 更新 run_log (status=completed)
```

## 回测 / 实盘同构

```
实盘:  market(live adapters) → strategy(Scorer) → risk(RiskMgr) → execution(broker)
回测:  market(historical)    → strategy(Scorer) → risk(RiskMgr) → execution(simulated)
                               ↑ 完全相同的纯函数内核 ↑
```

区别只在：
- 时钟：实盘用 `datetime.now()`，回测用模拟时钟
- 数据源：实盘用 live adapter，回测用 timeseries store
- 执行器：实盘用 broker adapter，回测用 SimulatedPortfolio
- 其他所有东西（评分、决策、风控）完全相同

## 目录结构

```
src/hermes/
├── platform/
│   ├── db.py              # SQLite 连接、migration
│   ├── events.py          # EventStore (append-only event_log)
│   ├── config.py          # ConfigRegistry (版本化、freeze、校验)
│   ├── runs.py            # RunJournal (run lifecycle、幂等)
│   └── cli.py             # CLI 入口 (typer)
│
├── market/
│   ├── service.py         # MarketService (编排抓取+标准化)
│   ├── adapters.py        # AkShare/MX/Sina adapter (Protocol)
│   ├── store.py           # market_observations 读写
│   └── models.py          # StockQuote, TechnicalIndicators, ...
│
├── strategy/
│   ├── models.py          # ScoreResult, DecisionIntent, ...
│   ├── scorer.py          # 四维评分 (纯函数)
│   ├── decider.py         # 综合决策 (纯函数)
│   ├── classifier.py      # 风格判定 (纯函数)
│   ├── timer.py           # 大盘择时 (纯函数)
│   └── service.py         # StrategyService (编排评分+决策)
│
├── risk/
│   ├── models.py          # ExitSignal, RiskAssessment, ...
│   ├── rules.py           # 止损/止盈/时间止损/风格切换 (纯函数)
│   ├── sizing.py          # 仓位计算 (纯函数)
│   └── service.py         # RiskService (编排风控)
│
├── execution/
│   ├── models.py          # Order, Position, Balance, ...
│   ├── orders.py          # 订单管理 + broker adapter
│   ├── positions.py       # 持仓投影 (从 event_log 重建)
│   └── service.py         # ExecutionService
│
└── reporting/
    ├── projectors.py      # 投影更新器 (event → projection 表)
    ├── reports.py         # 日报/周报/月报生成
    ├── obsidian.py        # Obsidian vault 写入
    └── discord.py         # Discord 消息格式化
```
