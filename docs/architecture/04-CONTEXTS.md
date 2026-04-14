# 04 — Context 详解

## platform

基础设施层。管理数据库、配置版本、运行生命周期、事件存储、对外接口。

### 文件

```
platform/
├── db.py         # SQLite 连接管理、migration、WAL 模式
├── events.py     # EventStore: append(), query(), rebuild_projection()
├── config.py     # ConfigRegistry: load, validate, freeze, version
├── runs.py       # RunJournal: start_run, complete_run, fail_run, 幂等检查
└── cli.py        # typer CLI 入口 + MCP Server 入口
```

### events.py — EventStore

```python
class EventStore:
    """append-only 事件存储"""

    def append(self, stream: str, stream_type: str, event_type: str,
               payload: dict, metadata: dict) -> str:
        """追加事件，返回 event_id。自动递增 stream_version。"""

    def query(self, stream: str = None, event_type: str = None,
              since: str = None, until: str = None) -> list[dict]:
        """查询事件。"""

    def get_stream(self, stream: str) -> list[dict]:
        """获取某个 stream 的全部事件（按 version 排序）。"""
```

### config.py — ConfigRegistry

```python
class ConfigRegistry:
    """配置版本化管理"""

    def freeze(self) -> ConfigSnapshot:
        """
        加载 + 校验 + deep copy + 计算 hash。
        返回 frozen snapshot，写入 config_versions 表。
        整个 run 期间使用这份 snapshot，不再读文件。
        """

    def get_version(self, config_version: str) -> dict:
        """按版本号加载历史配置（回测用）。"""
```

### runs.py — RunJournal

```python
class RunJournal:
    """运行生命周期管理"""

    def start_run(self, run_type: str, config_version: str) -> str:
        """创建 run，返回 run_id。"""

    def complete_run(self, run_id: str, artifacts: dict = None) -> None:
        """标记完成。"""

    def fail_run(self, run_id: str, error: str) -> None:
        """标记失败。"""

    def is_completed_today(self, run_type: str) -> bool:
        """幂等检查：今天是否已成功执行过。"""

    def get_failed_runs(self, days: int = 7) -> list[dict]:
        """查询近期失败的 run（供重放）。"""
```

---

## market

市场数据获取、标准化、时序存储。这是唯一允许做外部 IO 的业务 context。

### 文件

```
market/
├── service.py    # MarketService: 编排抓取 + 标准化 + 存储
├── adapters.py   # Protocol 接口 + AkShare/MX/Sina 实现
├── store.py      # market_observations / market_bars 读写
└── models.py     # StockQuote, TechnicalIndicators, FinancialReport, ...
```

### adapters.py — 数据源 Protocol

```python
class MarketDataProvider(Protocol):
    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]: ...
    async def get_kline(self, code: str, period: str, count: int) -> pd.DataFrame: ...

class FinancialDataProvider(Protocol):
    async def get_financial(self, code: str) -> FinancialReport: ...

class FlowDataProvider(Protocol):
    async def get_fund_flow(self, code: str, days: int) -> FundFlow: ...
```

### service.py — MarketService

```python
class MarketService:
    """编排数据抓取，自动 fallback + 缓存 + 限流"""

    def __init__(self, providers: list, store: MarketStore, concurrency: int = 5):
        self._chain = providers
        self._store = store
        self._sem = asyncio.Semaphore(concurrency)

    async def collect_snapshot(self, code: str, run_id: str) -> StockSnapshot:
        """抓取 + 标准化 + 追加到 market_observations + 返回 snapshot"""

    async def collect_batch(self, codes: list[str], run_id: str) -> list[StockSnapshot]:
        """批量抓取（受 semaphore 限流）"""
```

---

## strategy

纯函数内核。评分、决策、风格分类、择时。**不做任何 IO。**

### 文件

```
strategy/
├── models.py     # ScoreResult, DecisionIntent, StyleResult, ...
├── scorer.py     # Scorer: 四维评分 (纯函数)
├── decider.py    # Decider: 综合决策 (纯函数)
├── classifier.py # 风格判定 (纯函数)
├── timer.py      # 大盘择时 (纯函数，输入是 market 数据)
└── service.py    # StrategyService: 编排评分+决策，追加事件
```

### scorer.py

```python
class Scorer:
    """四维评分 — 纯函数，无副作用，无 IO"""

    def __init__(self, weights: ScoringWeights, veto_rules: list[str]):
        self.weights = weights
        self.veto_rules = veto_rules

    def score(self, snapshot: StockSnapshot) -> ScoreResult:
        """输入 snapshot，输出 ScoreResult。不碰网络、数据库、文件。"""
```

### service.py — StrategyService

```python
class StrategyService:
    """编排评分 + 决策，结果写入 event_log"""

    def __init__(self, scorer: Scorer, decider: Decider, event_store: EventStore):
        ...

    def evaluate(self, snapshots: list[StockSnapshot], market_state: MarketState,
                 run_id: str, config_version: str) -> list[DecisionIntent]:
        results = [self.scorer.score(s) for s in snapshots]
        decisions = [self.decider.decide(r, market_state) for r in results]

        for r in results:
            self.event_store.append(
                stream=f"strategy:{r.code}",
                stream_type="strategy",
                event_type="score.calculated",
                payload=r.to_dict(),
                metadata={"run_id": run_id, "config_version": config_version},
            )
        return decisions
```

---

## risk

纯函数内核。止损、止盈、仓位计算、组合风控。**不做任何 IO。**

### 文件

```
risk/
├── models.py     # ExitSignal, RiskAssessment, PositionSize, ...
├── rules.py      # 止损/止盈/时间止损/MA离场/风格切换 (纯函数)
├── sizing.py     # 仓位计算 (纯函数)
└── service.py    # RiskService: 编排风控，追加事件
```

### rules.py

```python
def check_exit_signals(position: Position, snapshot: StockSnapshot,
                       risk_params: RiskParams) -> list[ExitSignal]:
    """纯函数：检查所有离场信号"""

def check_portfolio_risk(portfolio: Portfolio,
                         limits: PortfolioLimits) -> list[RiskBreach]:
    """纯函数：检查组合级风控"""
```

### sizing.py

```python
def calc_position_size(decision: DecisionIntent, portfolio: Portfolio,
                       market_multiplier: float,
                       limits: PositionLimits) -> PositionSize:
    """纯函数：计算建议仓位"""
```

---

## execution

订单管理、持仓投影、资金管理。从 event_log 重建状态。

### 文件

```
execution/
├── models.py     # Order, Position, Balance, TradeEvent, ...
├── orders.py     # 订单管理 + broker adapter
├── positions.py  # 持仓投影 (从 event_log 重建)
└── service.py    # ExecutionService
```

### positions.py

```python
class PositionProjector:
    """从 event_log 重建当前持仓"""

    def rebuild(self, event_store: EventStore) -> list[Position]:
        """遍历 position.* 事件，重建当前持仓状态"""

    def update_from_event(self, event: dict) -> None:
        """增量更新：处理单个新事件"""
```

---

## reporting

只读消费事实和投影，生成报告。**不反写业务真相。**

### 文件

```
reporting/
├── projectors.py  # 投影更新器 (event → projection 表)
├── reports.py     # 日报/周报/月报生成
├── obsidian.py    # Obsidian vault 写入 (只是投影)
└── discord.py     # Discord 消息格式化
```

### projectors.py

```python
class ProjectionUpdater:
    """从 event_log 同步更新所有 projection 表"""

    def sync_all(self, since: str = None) -> dict:
        """全量或增量同步。返回更新统计。"""

    def rebuild_all(self) -> dict:
        """删除所有 projection 表数据，从 event_log 完全重建。"""
```
