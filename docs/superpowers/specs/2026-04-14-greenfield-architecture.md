# A 股交易系统 · 零基础完美架构方案

**日期：** 2026-04-14
**性质：** 全新设计（无历史包袱，从第一性原理出发）
**核心思想：** 事件溯源 + CQRS + 领域驱动设计

---

## 哲学立场

现有系统是从脚本堆砌演进来的，每一步都有当时的合理性，但留下了：

- **mutable snapshot** — 状态覆写，历史丢失
- **双写陷阱** — SQLite + Obsidian，哪个是真？
- **聚合根模糊** — order 和 position 写同一张 trade_events 表，但各有各的一致性边界
- **pipeline 即领域逻辑** — scoring.py 里既有数据抓取又有决策逻辑，拆不开

**完美架构的核心信念：**

> **事实不可变，状态可重建。** 所有变化的只是事件的追加，状态是事件流的投影。

---

## 核心设计原则

```
1. Event Store 是唯一真相来源（Single Source of Truth）
   一切状态变化都是事件。一切读取都是投影。

2. 聚合根是一致性边界（Aggregate as Consistency Boundary）
   Order 自己管自己的状态。Position 自己管自己的成本。
   跨聚合只能用最终一致性。

3. Command / Query 完全分离（CQRS）
   写：严格的业务规则校验 → 事件
   读：任意视图，按需投影，无副作用

4. 时间旅行是原生能力（Temporal Queries as First-Class）
   "2026-03-15 的持仓是什么？" 与 "现在的持仓是什么？" 是同一个 API 的不同参数。

5. 外部系统是事件来源，不是状态持有者
   券商订单、妙想模拟盘、舆情 API → 都是事件的 producer，不是 truth 的持有者。

6. 领域逻辑不依赖于基础设施
   Domain 层不 import ORM，不 import HTTP 库，不 import 文件系统。
```

---

## 限界上下文（Bounded Contexts）

系统分为 5 个限界上下文，每个上下文有自己独立的 Event Store namespace：

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         A 股交易系统                                      │
│                                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │  PORTFOLIO   │  │ INTELLIGENCE │  │    RISK       │  │  MARKET    │ │
│  │  组合管理     │  │   智能选股    │  │    风控       │  │  市场时钟  │ │
│  │              │  │              │  │              │  │            │ │
│  │ Order        │  │ ScreenJob    │  │ RiskRule     │  │ MarketIndex│ │
│  │ Position     │  │ Signal       │  │ Alert        │  │ Timing     │ │
│  │ Balance      │  │ CandidatePool│  │ StopTrigger  │  │ SignalFeed │ │
│  │ P&L          │  │ Score        │  │              │  │            │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └─────┬──────┘ │
│         │                 │                  │                 │          │
│         └─────────────────┴──────────────────┴─────────────────┘          │
│                                   │                                         │
│                           Shared Event Bus                                  │
│                                   │                                         │
│                    ┌──────────────▼──────────────────┐                       │
│                    │       REPORTING CONTEXT         │                       │
│                    │         报告与复盘               │                       │
│                    │  DailyJournal │ WeeklyReport   │                       │
│                    │  MonthlyReview │ TradeMemo     │                       │
│                    └─────────────────────────────────┘                       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 事件总纲（Event Schema）

### 跨上下文通用字段

所有事件都有：

```typescript
interface BaseEvent {
  event_id:      string;   // UUID v7（时间有序）
  aggregate_id:  string;   // "Order:600036:BUY:20260414"
  aggregate_type: string;  // "Order" | "Position" | "Signal" | "Alert"
  event_type:    string;   // "OrderFilled" | "PositionOpened" ...
  version:       number;   // 聚合内单调递增
  payload:       object;   // 业务数据（JSON）
  metadata: {
    trace_id:    string;   // 全链路追踪
    actor:       string;   // "cli" | "cron" | "mx_api" | "hermes"
    scope:       string;   // "cn_a_system" | "paper_mx" | "hk_legacy"
    occurred_at: string;   // ISO 8601，带时区
  };
}
```

---

## 上下文 1：PORTFOLIO（组合管理）

**职责：** 订单生命周期管理、持仓重建、资金余额、P&L 计算

### 1.1 Order 聚合根

```
Order 聚合根：管理一个订单从提交到终结的完整生命周期

聚合 ID 格式：ORDER:{stock_code}:{side}:{date}:{seq}
示例：ORDER:600036:BUY:20260414:001

状态机：
┌─────────┐ submit ┌─────────┐ partial ┌────────────────┐
│  NONE   │───────▶│ PENDING │────────▶│ PARTIAL_FILLED │
└─────────┘        └────┬────┘         └───────┬────────┘
                        │                       │
               cancel   │           full_fill   │
                ┌───────┘           ┌───────────┘
                ▼                   ▼
          ┌──────────┐        ┌──────────┐
          │CANCELLED │        │  FILLED  │
          └──────────┘        └──────────┘
                ▲
                │ expire（收盘未成交）
          ┌──────────┐
          │ EXPIRED  │
          └──────────┘
```

**Command / Event 清单**：

| Command | Event | 说明 |
|---------|-------|------|
| `SubmitOrder` | `OrderSubmitted` | 提交订单（含信号代码来源） |
| `FillOrder` | `OrderFilled` | 成交 |
| `PartialFillOrder` | `OrderPartialFilled` | 部分成交 |
| `CancelOrder` | `OrderCancelled` | 取消（含原因） |
| `ExpireOrder` | `OrderExpired` | 收盘自动作废 |

**payload 示例（OrderFilled）**：
```json
{
  "order_id": "ORD-20260414-001",
  "stock_code": "600036",
  "stock_name": "招商银行",
  "side": "BUY",
  "price": 36.696,
  "shares": 500,
  "amount": 18348.0,
  "broker": "eastmoney",
  "external_order_id": "EM-12345678",
  "filled_at": "2026-04-14T09:32:15+08:00",
  "signal_codes": ["BREAK_MA20", "GREEN_MARKET"],
  "source_trigger": "shadow_trade",
  "position_size": 20000,
  "cost_recorded": true
}
```

### 1.2 Position 聚合根

```
Position 聚合根：管理单只股票在一个 scope 下的完整持仓历史

聚合 ID 格式：POSITION:{stock_code}:{scope}
示例：POSITION:600036:cn_a_system

每个事件推进持仓状态：
[PositionOpened] → [BuyExecuted × N] → [SellExecuted] → [PositionClosed]
       │                   │                  │
       └────▶ [StopTriggered] ◀──────────────┘
```

**Command / Event 清单**：

| Command | Event | 说明 |
|---------|-------|------|
| `OpenPosition` | `PositionOpened` | 首次买入开仓，记录成本基础 |
| `AddPosition` | `BuyExecuted` | 加仓 |
| `ReducePosition` | `SellExecuted` | 减仓/清仓（含已实现盈亏） |
| `TriggerStop` | `StopTriggered` | 止损触发（含触发类型） |
| `AdjustCost` | `CostAdjusted` | 手动成本修正（需理由） |
| `RecordDividend` | `DividendReceived` | 分红到账 |

**Position 投影（Read Model）**：

```python
@dataclass
class PositionSnapshot:
    stock_code:     str
    stock_name:     str
    scope:          str
    shares:         int          # 净持仓股数
    cost_basis:     Decimal      # 加权平均成本
    total_cost:     Decimal      # 总投入
    market_value:   Decimal      # 市值（实时）
    unrealized_pnl: Decimal       # 未实现盈亏
    realized_pnl:   Decimal      # 已实现盈亏（历史累计）
    stop_price:     Decimal | None
    stop_type:      str | None
    opened_at:      datetime
    last_updated:   datetime
    version:        int
```

### 1.3 TradeEvent 事件（低层事件）

Position 和 Order 聚合建立在 `TradeEvent` 之上：

```
TradeEvent（最低层事件，所有成交的原子记录）
────────────────────────────────────────────
event_type: BUY | SELL | DIVIDEND | ADJUSTMENT
stock_code, stock_name
shares, price, amount
realized_pnl（仅 SELL）
scope, broker
broker_trade_id（幂等性）
occurred_at（精确到秒）
```

> **为什么有两层？**
> `TradeEvent` 是原子事实层，不可撼动。
> `Position` / `Order` 是业务语义层，有状态机规则。
> 所有高层事件必须能回溯到 TradeEvent。

---

## 上下文 2：INTELLIGENCE（智能选股）

**职责：** 股票筛选、信号生成、候选池管理、多维评分

### 2.1 ScreenJob 聚合根

一次选股作业是一个聚合：

```
聚合 ID：SCREEN:{job_type}:{date}:{seq}
示例：SCREEN:TRACKED:20260414:001

事件：
ScreenJobStarted → CriteriaDefined → [SignalGenerated × N] → ScreenJobCompleted
```

| Command | Event | 说明 |
|---------|-------|------|
| `StartScreenJob` | `ScreenJobStarted` | 启动选股（含选股条件） |
| `EmitSignal` | `SignalGenerated` | 发现一个信号（单股+信号类型+分值） |
| `CompleteScreenJob` | `ScreenJobCompleted` | 选股完成（含汇总） |

### 2.2 Signal 信号

Signal 是跨上下文共享的"领域事件"（Domain Event），被多个上下文消费：

```
SignalGenerated 事件字段：
────────────────────────────────────
stock_code, stock_name
signal_type: "BREAK_MA20" | "RSI_OVERSOLD" | "FUNDAMENTAL_BUY" | "SENTIMENT_BULLISH" | ...
source: "technical" | "fundamental" | "flow" | "sentiment"
scores: { technical: 0-10, fundamental: 0-10, flow: 0-10, sentiment: 0-10 }
veto_codes: []          # 一票否决信号
reason_text: str
emitted_at: datetime
expires_at: datetime     # 信号有效期
```

### 2.3 CandidatePool 聚合根

候选池是选股结果的容器：

```
聚合 ID：POOL:{pool_type}:{scope}
示例：POOL:CORE:cn_a_system

事件：
CandidateAdded → CandidateScored → CandidateRanked → CandidatePromoted → CandidateRemoved
```

| Command | Event | 说明 |
|---------|-------|------|
| `AddCandidate` | `CandidateAdded` | 新股入池（含信号来源） |
| `ScoreCandidate` | `CandidateScored` | 四维评分完成 |
| `RankPool` | `PoolRanked` | 重新排序 |
| `PromoteCandidate` | `CandidatePromoted` | 升降池 |
| `RemoveCandidate` | `CandidateRemoved` | 出池（含原因） |

### 2.4 Score 实体

评分是一个值对象（Value Object），不是聚合：

```python
@dataclass(frozen=True)
class FourDimensionalScore:
    stock_code:    str
    scored_at:     datetime

    # 四维评分（满分10分）
    technical:     float    # 技术面（权重 20%）
    fundamental:   float    # 基本面（权重 30%）
    flow:          float    # 资金流（权重 20%）
    sentiment:     float    # 舆情（权重 30%）

    total:         float    # 加权总分

    veto_codes:    list[str]  # 一票否决码
    data_quality:  str        # "complete" | "partial" | "stale"

    def is_buy_candidate(self, min_score: float = 7.0) -> bool:
        return self.total >= min_score and not self.veto_codes
```

---

## 上下文 3：RISK（风控）

**职责：** 风控规则引擎、黑名单管理、止损触发、告警中心

### 3.1 RiskRule 实体

风控规则是可配置的值对象：

```python
@dataclass(frozen=True)
class RiskRule:
    rule_id:       str
    name:          str
    category:      str          # "blacklist" | "stop_loss" | "exposure" | "liquidity"

    # 规则参数
    params:        dict         # 如 {"max_position_pct": 0.10}

    # 评估函数（Domain 层，零依赖）
    def evaluate(self, context: RiskContext) -> RiskResult:
        ...

# 预置风控规则
RULES = {
    "BLACKLIST_ST": RiskRule("BLACKLIST_ST", "ST/退市股禁止买入",
        category="blacklist",
        params={"enabled": True},
        evaluate=lambda ctx: RiskResult(allowed=not ctx.is_st)),

    "RISK_DYNAMIC_STOP": RiskRule("RISK_DYNAMIC_STOP", "动态止损 -4%",
        category="stop_loss",
        params={"loss_pct": 0.04},
        evaluate=lambda ctx: RiskResult(
            allowed=True,
            signal=SignalType.RISK_DYNAMIC_STOP if ctx.drawdown_pct >= 0.04 else None)),

    "RISK_ABSOLUTE_STOP": RiskRule("RISK_ABSOLUTE_STOP", "绝对止损 -7%",
        category="stop_loss",
        params={"loss_pct": 0.07},
        evaluate=lambda ctx: RiskResult(
            allowed=True,
            signal=SignalType.RISK_ABSOLUTE_STOP if ctx.drawdown_pct >= 0.07)),

    "MAX_POSITION_SIZE": RiskRule("MAX_POSITION_SIZE", "单只最大仓位 4%",
        category="exposure",
        params={"max_pct": 0.04},
        evaluate=lambda ctx: RiskResult(allowed=ctx.proposed_pct <= 0.04)),

    "MAX_WEEKLY_TRADES": RiskRule("MAX_WEEKLY_TRADES", "周买入次数 ≤ 2",
        category="exposure",
        params={"max_count": 2},
        evaluate=lambda ctx: RiskResult(allowed=ctx.weekly_buy_count < 2)),
}
```

### 3.2 RiskEngine（Command Side）

```python
class RiskEngine:
    def __init__(self, event_store: EventStore, rules: dict[str, RiskRule]):
        self.event_store = event_store
        self.rules = rules

    def check_order(self, cmd: RiskCheckCommand) -> RiskCheckResult:
        """
        对一个 proposed order 执行全量风控检查
        返回：(allowed: bool, passed_rules: list, failed_rules: list, signals: list)
        """
        ctx = self._build_context(cmd)
        passed, failed, signals = [], [], []

        for rule_id, rule in self.rules.items():
            result = rule.evaluate(ctx)
            if result.allowed and not result.signal:
                passed.append(rule_id)
            elif result.allowed and result.signal:
                signals.append(result.signal)
            else:
                failed.append((rule_id, result.denial_reason))

        return RiskCheckResult(
            allowed=len(failed) == 0,
            passed_rules=passed,
            failed_rules=failed,
            advisory_signals=signals,
            context_snapshot=ctx.to_dict()
        )
```

### 3.3 Alert 聚合根

告警是一个独立的聚合：

```
聚合 ID：ALERT:{scope}:{stock_code}:{alert_type}:{date}
示例：ALERT:cn_a_system:600036:STOP_TRIGGERED:20260414

事件：AlertRaised → AlertAcknowledged → AlertResolved
```

---

## 上下文 4：MARKET（市场时钟）

**职责：** 大盘择时信号、多指数状态、情绪量化

### MarketIndex 实体

```python
@dataclass(frozen=True)
class MarketIndexSnapshot:
    index_code:    str          # "SH000001" | "SZ399001" | "HSI"
    index_name:     str
    closed_at:      datetime

    close_price:    Decimal
    ma20_price:     Decimal      # MA20 位置
    ma60_price:     Decimal      # MA60 位置
    ma20_distance:  float        # 相对 MA20 的偏离%（正=上方）
    ma60_distance:  float        # 相对 MA60 的偏离%

    # 多指数综合信号
    overall_signal: "GREEN" | "YELLOW" | "RED" | "CLEAR"
    """GREEN=三指数全在MA20上，YELLOW=部分在，RED=全下，CLEAR=无法判断"""

    signal_reason:  str
```

### TimingEngine（Command Side）

```python
class TimingEngine:
    """
    每天收盘后运行，从多指数状态计算大盘信号
    """
    def compute_signal(self, snapshots: list[MarketIndexSnapshot]) -> TimingSignal:
        above_ma20 = [s for s in snapshots if s.ma20_distance > 0]

        if len(above_ma20) == len(snapshots):
            return TimingSignal("GREEN", "全指数站上MA20")
        elif len(above_ma20) >= len(snapshots) * 0.6:
            return TimingSignal("YELLOW", "多数指数站上MA20")
        elif len(above_ma20) == 0:
            return TimingSignal("RED", "全指数跌破MA20")
        else:
            return TimingSignal("CLEAR", "信号不明，等待确认")

@dataclass(frozen=True)
class TimingSignal:
    signal:   "GREEN" | "YELLOW" | "RED" | "CLEAR"
    reason:   str
    position_multiplier: float  # GREEN=1.0, YELLOW=0.5, RED/CLEAR=0.0
```

---

## 上下文 5：REPORTING（报告与复盘）

**职责：** 每日交易日志、周报、月报、交易备忘

### DailyJournal 聚合根

每日收盘后自动创建一个 Journal 聚合：

```
聚合 ID：JOURNAL:DAILY:{scope}:{date}
示例：JOURNAL:DAILY:cn_a_system:20260414

事件：
JournalOpened → TradeRecorded × N → PositionUpdated → JournalEnriched → JournalClosed
```

| Command | Event | 说明 |
|---------|-------|------|
| `OpenJournal` | `JournalOpened` | 自动创建（含当日大盘信号） |
| `RecordTrade` | `TradeRecorded` | 追加一笔交易（含情绪标注） |
| `UpdatePositionSummary` | `PositionUpdated` | 更新持仓快照 |
| `EnrichJournal` | `JournalEnriched` | 追加市场解读、AI 复盘摘要 |
| `CloseJournal` | `JournalClosed` | 收盘后锁定，不可修改 |

> **设计要点**：`JournalOpened` 的 `occurred_at` 是 `date 00:00:00`（整天都有效），`JournalClosed` 在 15:35 之后。不允许在 Journal 关闭后追加内容，保证日志的不可篡改性。

---

## 事件总线（Shared Event Bus）

跨上下文事件通过共享 Event Bus 传播：

```
┌──────────────┐ emit  ┌──────────────┐ emit  ┌──────────────┐
│ INTELLIGENCE │──────▶│  Event Bus    │──────▶│  PORTFOLIO   │
│ SignalGenerated│      │              │       │ Subscribe   │
│              │       │              │       │ handle():   │
│              │       │              │       │ if SIG_BUY: │
└──────────────┘       │              │       │   propose   │
                        │              │       │   order()   │
                        └──────────────┘       └──────────────┘

订阅关系（Saga / Process Manager）：
─────────────────────────────────────
SignalGenerated      → RiskEngine（检查是否可买）
                    → CandidatePool（更新评分）
                    → TimingEngine（如果是大盘信号）

OrderFilled          → PositionProjector（重建持仓）
                    → DailyJournal（追加交易记录）
                    → AlertProjector（更新告警）

MarketClosed         → TimingEngine（计算大盘信号）
                    → ScoringEngine（触发收盘评分）
                    → DailyJournal（关闭当日日志）
```

---

## 技术架构

### 系统分层

```
┌────────────────────────────────────────────────────────────────────────┐
│                           用户层（User Layer）                          │
│                                                                        │
│  Web Dashboard（React, 手动刷新）     │     Hermes Agent（自动编排）     │
│  个人盯盘、手动操作                   │     定时 pipeline、Discord 推送   │
└────────────────────┬─────────────────┴────────────────────────────────┘
                     │ HTTP
┌────────────────────▼────────────────────────────────────────────────────┐
│                         API 网关层（API Gateway）                        │
│                                                                        │
│  FastAPI（ASGI）                                                        │
│  ├── Command Routes  (/commands/...)  → Command Handlers → Event Store  │
│  ├── Query Routes    (/queries/...)  → Query Handlers → Read Models   │
│  ├── Subscriptions   (/ws/...)       → WebSocket 实时推送              │
│  └── Health          (/health, /ready)                                  │
└────────────────────┬───────────────────────────────────────────────────┘
                     │
         ┌───────────┼───────────────────────────────────┐
         │           │                                   │
┌────────▼────────┐ ┌▼────────────────────────┐ ┌──────▼──────────┐
│  PostgreSQL     │ │  External Integrations   │ │  File Storage   │
│  (Event Store   │ │                          │ │                  │
│   + Read Models)│ │  MX API（妙想模拟盘）     │ │  PDF 报告        │
│                 │ │  Broker API（券商订单）    │ │  导出文件        │
│                 │ │  TrendRadar（舆情）       │ │                  │
│                 │ │  行情数据（AKShare）      │ │                  │
└─────────────────┘ └──────────────────────────┘ └─────────────────┘
```

### 数据库设计（PostgreSQL）

```
event_store（全局 Event Store，namespace 区分上下文）
───────────────────────────────────────────────────────
id              BIGSERIAL PRIMARY KEY
namespace       VARCHAR(32)      -- "portfolio" | "intelligence" | "risk" | "market" | "reporting"
aggregate_id    VARCHAR(128)     -- "ORDER:600036:BUY:20260414"
aggregate_type  VARCHAR(64)     -- "Order" | "Position" | "Signal" | "Alert"
event_type      VARCHAR(128)     -- "OrderFilled" | "PositionOpened" ...
version         INTEGER          -- 聚合内单调递增
payload         JSONB NOT NULL   -- 业务数据
metadata        JSONB DEFAULT '{}'
causation_id    BIGINT          -- 触发此事件的 command event_id（用于因果链追踪）
correlation_id   VARCHAR(64)     -- 同一业务操作的多个事件共享（如一次选股 job）
created_at      TIMESTAMPTZ DEFAULT NOW()

UNIQUE(namespace, aggregate_id, version)
INDEX idx_es_namespace_agg (namespace, aggregate_id, version)
INDEX idx_es_correlation (correlation_id)
INDEX idx_es_event_type (event_type, created_at)  -- 用于 projections 订阅

read_models.portfolio_positions（Position 投影）
───────────────────────────────────────────────────────
id              SERIAL PRIMARY KEY
stock_code      VARCHAR(16) NOT NULL
stock_name      VARCHAR(128)
scope           VARCHAR(32) NOT NULL
shares          INTEGER NOT NULL DEFAULT 0
cost_basis      NUMERIC(14,4) NOT NULL DEFAULT 0
total_cost      NUMERIC(14,4) NOT NULL DEFAULT 0
realized_pnl    NUMERIC(14,4) NOT NULL DEFAULT 0
stop_price      NUMERIC(10,4)
stop_type       VARCHAR(32)
opened_at       TIMESTAMPTZ
last_event_id   BIGINT          -- 最后一次更新来自哪个事件（因果验证）
version         INTEGER          -- 用于乐观锁
UNIQUE(scope, stock_code)

read_models.orders（Order 投影）
───────────────────────────────────────────────────────
id              SERIAL PRIMARY KEY
order_id        VARCHAR(64) UNIQUE NOT NULL
scope           VARCHAR(32) NOT NULL
stock_code      VARCHAR(16) NOT NULL
side            VARCHAR(8) NOT NULL
price           NUMERIC(10,4) NOT NULL
shares          INTEGER NOT NULL
shares_filled   INTEGER DEFAULT 0
status          VARCHAR(32) NOT NULL  -- PENDING/FILLED/CANCELLED/EXPIRED/PARTIAL_FILLED
signal_codes    JSONB DEFAULT '[]'
reason_codes    JSONB DEFAULT '[]'
submitted_at    TIMESTAMPTZ
filled_at       TIMESTAMPTZ
last_event_id   BIGINT
version         INTEGER
UNIQUE(scope, stock_code, submitted_at)

read_models.signals（Signal 投影，活跃信号索引）
───────────────────────────────────────────────────────
id              SERIAL PRIMARY KEY
stock_code      VARCHAR(16) NOT NULL
signal_type     VARCHAR(64) NOT NULL
source          VARCHAR(32) NOT NULL
scores          JSONB
veto_codes      JSONB
reason_text     TEXT
emitted_at      TIMESTAMPTZ
expires_at      TIMESTAMPTZ
scope           VARCHAR(32)
UNIQUE(stock_code, signal_type, emitted_at::date)

read_models.candidate_pools（候选池投影）
───────────────────────────────────────────────────────
id              SERIAL PRIMARY KEY
pool_type       VARCHAR(32) NOT NULL  -- "core" | "watch" | "shadow"
stock_code      VARCHAR(16) NOT NULL
stock_name      VARCHAR(128)
total_score     NUMERIC(4,2)
scores_4d       JSONB          -- {technical, fundamental, flow, sentiment}
rank            INTEGER
added_at        TIMESTAMPTZ
last_scored_at  TIMESTAMPTZ
scope           VARCHAR(32)
UNIQUE(pool_type, stock_code)

read_models.alert_center（告警投影）
───────────────────────────────────────────────────────
id              SERIAL PRIMARY KEY
alert_type      VARCHAR(64) NOT NULL
stock_code      VARCHAR(16)
scope           VARCHAR(32) NOT NULL
severity        VARCHAR(16) NOT NULL  -- "critical" | "warning" | "info"
message         TEXT
raised_at       TIMESTAMPTZ
acknowledged_at TIMESTAMPTZ
resolved_at     TIMESTAMPTZ
last_event_id   BIGINT
UNIQUE(alert_type, stock_code, raised_at::date)

read_models.daily_journals（日志投影）
───────────────────────────────────────────────────────
id              SERIAL PRIMARY KEY
journal_date    DATE NOT NULL
scope           VARCHAR(32) NOT NULL
market_signal   VARCHAR(16)
opening_balance NUMERIC(14,4)
closing_balance NUMERIC(14,4)
total_pnl       NUMERIC(14,4)
trades_count    INTEGER
positions_summary JSONB   -- 收盘持仓快照（JSON 方便全文检索）
enrichment      TEXT     -- 收盘后的 AI 复盘摘要
is_closed       BOOLEAN DEFAULT FALSE
UNIQUE(scope, journal_date)
```

### Event Handler / Projector 实现模式

```python
# 每个聚合的投影处理器（从 Event Store 消费事件）
class PositionProjector:
    """
    将 Position 事件流投影为 positions 读模型
    完全无状态幂等投影：从任意时间点重放都得到相同结果
    """

    def project(self, aggregate_id: str, events: list[Event]) -> PositionSnapshot:
        state = Position.empty()
        for event in sorted(events, key=lambda e: e.version):
            state = state.apply(event)  # 纯函数
        return state.to_snapshot()

    def project_all(self) -> list[PositionSnapshot]:
        """全量重建（用于初始化或修复漂移）"""
        positions = {}
        events = self.event_store.get_events_by_type("PositionOpened", "BuyExecuted",
                                                       "SellExecuted", "StopTriggered",
                                                       "CostAdjusted", "DividendReceived")
        for event in events:
            key = f"{event.aggregate_id}"
            if key not in positions:
                positions[key] = Position.empty()
            positions[key] = positions[key].apply(event)
        return [s.to_snapshot() for s in positions.values() if s.is_active()]

    def project_up_to(self, as_of: datetime) -> list[PositionSnapshot]:
        """时间点投影（任意历史时刻重建持仓）"""
        positions = {}
        events = self.event_store.get_events_before(
            event_types=["PositionOpened", "BuyExecuted", "SellExecuted",
                         "StopTriggered", "CostAdjusted", "DividendReceived"],
            before=as_of
        )
        for event in events:
            key = event.aggregate_id
            if key not in positions:
                positions[key] = Position.empty()
            positions[key] = positions[key].apply(event)
        return [s.to_snapshot() for s in positions.values() if s.is_active()]


class Position:
    """Position 聚合的状态，不可变"""

    def __init__(self, ...):
        self.shares = 0
        self.cost_basis = Decimal(0)
        self.realized_pnl = Decimal(0)
        self.stop_price = None
        self._version = 0

    def apply(self, event: Event) -> "Position":
        """纯函数：event + state → new_state"""
        handlers = {
            "PositionOpened":   self._apply_opened,
            "BuyExecuted":     self._apply_buy,
            "SellExecuted":    self._apply_sell,
            "StopTriggered":   self._apply_stop,
            "CostAdjusted":    self._apply_cost_adj,
            "DividendReceived": self._apply_dividend,
        }
        handler = handlers.get(event.event_type)
        if handler:
            return handler(event)
        return self

    def _apply_buy(self, event: Event) -> "Position":
        new_shares = self.shares + event.payload["shares"]
        new_cost = (self.total_cost + event.payload["amount"]) / new_shares if new_shares else 0
        return Position(shares=new_shares, cost_basis=new_cost, ...)

    def _apply_sell(self, event: Event) -> "Position":
        sold_shares = event.payload["shares"]
        realized = event.payload.get("realized_pnl", 0)
        remaining_shares = max(0, self.shares - sold_shares)
        # 成本不因卖出改变（先进先出）
        return Position(shares=remaining_shares, cost_basis=self.cost_basis,
                        realized_pnl=self.realized_pnl + realized, ...)
```

---

## 关键 API 设计

### Command API（写）

```
POST /commands/submit-order
Body: { stock_code, side, price, shares, scope, signal_codes, actor }
→ RiskEngine.check()
→ OrderAggregate.submit()
→ EventStore.append([OrderSubmitted])
→ [async] PositionProjector.project()
→ [async] DailyJournal.RecordTrade()

POST /commands/fill-order
Body: { order_id, stock_code, side, price, shares, broker_fill_id, scope }
→ EventStore.append([OrderFilled, BuyExecuted/SellExecuted])
→ [async] PositionProjector.project()
→ [async] DailyJournal.RecordTrade()

POST /commands/start-screen
Body: { job_type, criteria, scope }
→ EventStore.append([ScreenJobStarted])
→ [async] ScreeningEngine.run() → [SignalGenerated × N]
→ EventStore.append(signals)
→ [async] CandidatePool.AddCandidate()
```

### Query API（读）

```
GET /queries/positions
Query: ?scope=cn_a_system&as_of=2026-03-15  ← 时间旅行！
→ 构造时间点查询，返回该时刻的持仓快照

GET /queries/positions/{stock_code}
Query: ?scope=cn_a_system
→ 返回当前持仓（含实时行情）

GET /queries/trade-history
Query: ?scope=cn_a_system&from=2026-01-01&to=2026-04-14&group_by=month
→ 按月分组的历史交易汇总

GET /queries/pools/{pool_type}
Query: ?scope=cn_a_system&include_stale=true
→ 返回候选池（含评分、veto 信号、数据质量标记）

GET /queries/risk-check
Query: ?scope=cn_a_system
→ 组合风控状态（暴露度、周买入次数、黑名单匹配）

GET /queries/market-signal
→ 大盘择时信号（GREEN/YELLOW/RED/CLEAR）+ 各指数详情

GET /queries/daily-journal/{date}
→ 当日交易日志（含 Enrichment）
```

### WebSocket Subscription（实时）

```
WS /ws/portfolio?scope=cn_a_system
→ 持仓变化实时推送（Position 投影更新时触发）
→ 告警实时推送
→ 模拟盘成交实时推送

WS /ws/market-signal
→ 大盘信号变化推送
```

---

## 时间旅行（Temporal Queries）实现

这是 Event Sourcing 的核心差异化能力：

```python
class TemporalQueryService:
    """
    任意时间点的状态重建
    "我的 3 月 15 日持仓是什么？"
    "过去 30 天我的已实现盈亏是多少？"
    "当时为什么买了 600036？"
    """

    def positions_at(self, scope: str, as_of: date) -> list[PositionSnapshot]:
        """重建 as_of 日期的持仓快照"""
        events = self.event_store.get_events_before(
            event_types=["PositionOpened", "BuyExecuted", "SellExecuted",
                         "StopTriggered", "PositionClosed"],
            before=as_of.end_of_day(),
            scope=scope
        )
        return self._project_positions(events)

    def realized_pnl_between(self, scope: str, from_date: date, to_date: date) -> Decimal:
        """区间已实现盈亏"""
        events = self.event_store.get_events_between(
            event_type="SellExecuted",
            from_=from_date,
            to=to_date,
            scope=scope
        )
        return sum(e.payload.get("realized_pnl", 0) for e in events)

    def why_bought(self, stock_code: str, scope: str) -> OrderContext:
        """回溯这笔买入的决策上下文（信号来源、评分、时间点）"""
        order_events = self.event_store.get_events(
            aggregate_id__startswith=f"ORDER:{stock_code}:",
            event_type="OrderFilled"
        )
        if not order_events:
            return None
        order = order_events[0]
        # 找同期信号
        signal_events = self.event_store.get_events(
            stock_code=stock_code,
            event_type="SignalGenerated",
            from_=order.created_at - timedelta(hours=24),
            to=order.created_at
        )
        return OrderContext(order=order, signals=signal_events)

    def reconcile_with_broker(self, scope: str, broker_positions: list) -> ReconciliationReport:
        """
        对账：事件流重建的持仓 vs 券商报告的持仓
        找出差异 → 生成 CompensationEvents（补偿事件）
        """
        event_positions = self.positions_at(scope, today)
        diff = self._compute_diff(event_positions, broker_positions)

        if diff.has_gaps or diff.has_surplus:
            return ReconciliationReport(
                status="DRIFT",
                event_based=event_positions,
                broker_based=broker_positions,
                gaps=diff.gaps,
                surplus=diff.surplus,
                mismatches=diff.share_mismatches,
                recommended_actions=[
                    CompensationEvent(...) for _ in diff.issues
                ]
            )
        return ReconciliationReport(status="CONSISTENT")
```

---

## 外部系统集成

### 作为事件 Producer 的设计原则

```
券商系统（EastMoney / 超级韭菜）
        │
        │ WebSocket / Polling（定时拉取订单状态）
        ▼
┌─────────────────────────────┐
│  BrokerSyncService          │
│  将外部订单状态映射为内部事件 │
│  OrderSubmitted → OrderSync │  券商创建订单
│  OrderFilled → OrderSync    │  券商成交推送
│  OrderCancelled → OrderSync │  券商撤单
└──────────────┬──────────────┘
               │ append to Event Store
               ▼
        Event Store（唯一真相来源）
               │ Projector
               ▼
        Position/Order 投影更新

妙想模拟盘（MX API）
        │
        │ 定时拉取持仓 + 历史成交
        ▼
┌─────────────────────────────┐
│  MXSyncService              │
│  scope="paper_mx"           │
│  TradeSynced → TradeEvent   │  同步成交
│  BalanceSynced → BalanceEvent│  同步余额
└──────────────┬──────────────┘
               │ append to Event Store
               ▼
        Position 投影（scope=paper_mx）

TrendRadar（舆情 API）
        │
        │ 定时拉取新闻情感
        ▼
┌─────────────────────────────┐
│  SentimentService           │
│  SentimentReceived → SignalGenerated
└──────────────┬──────────────┘
               │
               ▼
        Signal Pool → Scoring Engine
```

---

## 一致性策略

```
强一致性（within aggregate）：
────────────────────────────────────
Order 聚合内的状态变化：OrderSubmitted → OrderFilled → OrderCancelled
通过 version 乐观锁保证：写入时检查 version == expected_version

最终一致性（cross aggregate）：
────────────────────────────────────
OrderFilled 事件 → 触发 Position.BuyExecuted 事件
PositionProjector 异步消费，更新 positions 读模型
延迟 < 1 秒（PostgreSQL NOTIFY/LISTEN）

多 aggregate 原子性：
────────────────────────────────────
fill-order 命令涉及 Order + Position 两个聚合
解决：Saga Pattern
  Step 1: append OrderFilled to ORDER aggregate（成功）
  Step 2: append BuyExecuted to POSITION aggregate（失败 → 发布补偿事件 PositionAdjusted）

补偿事件模式（CompensationEvent）：
  PositionAdjusted event.payload = {
    "compensates": "BuyExecuted:POSITION:600036:cn_a_system:v12",
    "reason": "reconciliation_drift",
    "adjustment": {"shares_delta": -500}
  }
```

---

## 部署架构

```
┌──────────────────────────────────────────────────────────────────┐
│                        宿主机（macOS）                            │
│                                                                  │
│  hermes_cron.sh（定时调度）                                        │
│  hermes-agent（AI 编排层）                                         │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                     Docker Compose                          │  │
│  │                                                             │  │
│  │  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐  │  │
│  │  │   FastAPI   │  │  PostgreSQL  │  │   React App       │  │  │
│  │  │  (uvicorn)  │  │   (pg16)     │  │   (nginx serve)   │  │  │
│  │  │  port:8000  │  │  port:5432   │  │   port:3000       │  │  │
│  │  └─────────────┘  └──────────────┘  └───────────────────┘  │  │
│  │                                                             │  │
│  │  ┌─────────────────────────────────────────────────────┐  │  │
│  │  │              Event Processor（Sidecar）                │  │  │
│  │  │  每 5 秒轮询 Event Store → 驱动所有 Projector         │  │  │
│  │  │  处理：PositionProjector, OrderProjector,             │  │  │
│  │  │         DailyJournalProjector, AlertProjector        │  │  │
│  │  └─────────────────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

---

## 实施路线图

### Phase 0（第 1-2 周）：骨架

```
目标：Event Store 跑通，端到端流程验证
─────────────────────────────────────
1. PostgreSQL Schema 初始化（event_store + read_models）
2. EventStore Python SDK（append, get_events, subscribe）
3. Order + Position 聚合根（含状态机）
4. PositionProjector
5. FastAPI Command + Query 端点（订单录入 + 持仓查询）
6. 与 MX API 集成（paper_mx scope）
验收：时间旅行 API（GET /queries/positions?as_of=date）可用
```

### Phase 1（第 3-4 周）：Intelligence 集成

```
目标：选股信号进入事件流，风控引擎上线
─────────────────────────────────────
1. Signal 事件类型 + SignalProjector
2. CandidatePool 聚合根
3. 四维评分引擎（集成 AKShare 数据）
4. RiskEngine（含全部预置规则）
5. /commands/start-screen 和 /queries/pools 端点
6. 黑名单管理端点
```

### Phase 2（第 5-6 周）：报告自动化

```
目标：每日日志自动化，Discord 推送全量事件驱动
─────────────────────────────────────
1. MarketTimingEngine + TimingSignal 事件
2. DailyJournal 聚合根（JournalOpened → JournalClosed）
3. 每日 15:35 Pipeline → EventStore（替代现有 evening.py）
4. Discord 推送 Engine（订阅事件流，非轮询）
5. 核心池评分 Pipeline → EventStore
6. 舆情 Pipeline → EventStore
```

### Phase 3（第 7-8 周）：前端 + 对账

```
目标：Web Dashboard 上线，对账机制完备
─────────────────────────────────────
1. React Dashboard（持仓/收益/池子/告警）
2. ReconciliationService（事件流 vs 券商 vs MX）
3. CompensationEvent + 补偿执行
4. 历史数据迁移（从现有 SQLite/JSON 历史）
5. 周报 + 月报 Generator（基于 EventStore 重放）
```

### Phase 4（持续）：演进增强

```
- WebSocket 实时推送
- 回测引擎（基于历史 EventStore 重放）
- 多账号支持（hk_legacy scope）
- PDF 报告生成
```

---

## 核心差异化总结

| 能力 | 传统 mutable DB | Event Store（完美架构） |
|------|----------------|----------------------|
| 状态历史 | 要么丢了，要么靠 audit log | 天然存在，不可篡改 |
| 对账 | 快照 vs 快照，不可靠 | 事件流重建 vs 外部数据，可验证 |
| 复盘 | "为什么买？" → 靠猜 | 事件链回溯，信号+评分+订单完整上下文 |
| 任意时间点状态 | 无法支持 | `positions_at(date)` 一行 API |
| 多系统数据一致性 | 手动同步，容易漂移 | 补偿事件 + ReconciliationService |
| 新增分析维度 | 要改 schema，历史无数据 | 回放事件流即可，无需 schema 变更 |
| 错误恢复 | 脏数据难以清理 | 补偿事件，不删除历史 |
| 单元测试 | 依赖数据库 | 纯函数，无依赖 |

---

## 附录：现有系统迁移映射

| 现有概念 | 完美架构对应 |
|---------|------------|
| `trade_events` 表 | Event Store（TradeEvent 事件） |
| `portfolio_positions` 表 | Position 聚合根的 Read Model |
| `orders` 表 | Order 聚合根的 Read Model |
| `candidate_snapshot_history` | CandidatePool 聚合根的事件历史 |
| `pool_entries` 表 | CandidatePool 的 Read Model |
| `alert_snapshots` | Alert 聚合根 |
| `market_snapshots` | MarketIndexSnapshot 值对象 |
| `decision_snapshot_history` | DailyJournal 聚合根 |
| `evening.py` 收盘流程 | DailyJournal Pipeline（EventStore 驱动） |
| `scoring.py` 评分 | ScoringEngine（EventStore 事件 + Projection） |
| `shadow_trade.py` | MXSyncService + ShadowOrderCommand |
| `hermes_cron.sh` | Cron → FastAPI Pipeline 端点 |
| `Obsidian vault/*.md` | DailyJournal Read Model（HTML 渲染） |
| `discord_push.py` | Event Bus → Discord Forwarder（订阅模式） |
