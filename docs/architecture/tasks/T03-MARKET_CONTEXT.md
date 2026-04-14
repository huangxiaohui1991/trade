# T03 — market 数据层

> Phase 3 | 预估 1-2 周 | 优先级：P0 | 依赖：T01, T02

## 目标

数据获取与业务逻辑彻底分离。通过批量接口、缓存分层、并发限流降低耗时。
市场观察追加到 market_observations，与业务事件逻辑分离。

## 子任务

### T03.1 market models

- [ ] 创建 `src/hermes/market/models.py`
- [ ] `StockQuote` — 实时行情
- [ ] `TechnicalIndicators` — 技术指标（MA/RSI/量比/乖离率/...）
- [ ] `FinancialReport` — 财务数据（ROE/营收增长/现金流/PE/PB）
- [ ] `FundFlow` — 资金流向
- [ ] `SentimentData` — 舆情数据
- [ ] `StockSnapshot` — 评分所需全部数据的聚合（strategy context 消费）
- [ ] `IndexQuote` — 指数行情

### T03.2 数据源 Protocol + adapters

- [ ] 创建 `src/hermes/market/adapters.py`
- [ ] `MarketDataProvider(Protocol)` — get_realtime / get_kline / get_index
- [ ] `FinancialDataProvider(Protocol)` — get_financial
- [ ] `FlowDataProvider(Protocol)` — get_fund_flow / get_northbound
- [ ] `SentimentProvider(Protocol)` — search_news
- [ ] `ScreenerProvider(Protocol)` — search_stocks
- [ ] `AkShareMarketAdapter` — 优先使用批量接口（stock_zh_a_spot_em）
- [ ] `AkShareFinancialAdapter`
- [ ] `AkShareFlowAdapter`
- [ ] `MXMarketAdapter`
- [ ] `MXScreenerAdapter`
- [ ] `MXSentimentAdapter`
- [ ] 同步 akshare 调用用 `asyncio.to_thread()` 包装
- [ ] httpx async client 用于 MX API

**迁移映射：**
| V1 | V2 |
|----|-----|
| `engine/technical.py` | `market/adapters.py::AkShareMarketAdapter` |
| `engine/financial.py` | `market/adapters.py::AkShareFinancialAdapter` |
| `engine/flow.py` | `market/adapters.py::AkShareFlowAdapter` |
| `engine/mx_client.py` | `market/adapters.py::MXMarketAdapter` |
| `mx/mx_xuangu.py` | `market/adapters.py::MXScreenerAdapter` |

### T03.3 MarketStore

- [ ] 创建 `src/hermes/market/store.py`
- [ ] `save_observation(source, kind, symbol, payload, run_id)` — 追加到 market_observations
- [ ] `save_bars(symbol, bars_df)` — 追加到 market_bars（金额存分）
- [ ] `get_bars(symbol, start, end) -> pd.DataFrame`
- [ ] `get_latest_observation(symbol, kind) -> dict`
- [ ] 缓存层：SQLite 表 + TTL 检查

**TTL 配置：**
| 数据类型 | TTL |
|---------|-----|
| realtime | 30 秒 |
| technical | 5 分钟 |
| financial | 24 小时 |
| flow | 10 分钟 |
| sentiment | 30 分钟 |

### T03.4 MarketService

- [ ] 创建 `src/hermes/market/service.py`
- [ ] `__init__(providers, store, concurrency_limit=5)`
- [ ] `collect_snapshot(code, run_id) -> StockSnapshot`
  - 五个维度并发获取
  - 自动 fallback（MX → AkShare → Sina）
  - 追加到 market_observations
  - 数据质量标记（DEGRADED / ERROR）
- [ ] `collect_batch(codes, run_id) -> list[StockSnapshot]`
  - semaphore 限流
- [ ] `collect_market_state(run_id) -> MarketState`
  - 拉取指数数据 → 计算大盘信号

### T03.5 Pipeline 迁移

- [ ] `pipeline/core_pool_scoring.py` 改为调用 MarketService
- [ ] `pipeline/morning.py` 改为调用 MarketService
- [ ] `pipeline/evening.py` 改为调用 MarketService
- [ ] `pipeline/stock_screener.py` 改为调用 MarketService + ScreenerProvider

### T03.6 测试

- [ ] adapter 单元测试（mock HTTP → 验证标准化输出）
- [ ] MarketService fallback 测试（provider 1 失败 → 自动降级）
- [ ] 缓存测试（TTL 内命中 → TTL 后重新获取）
- [ ] 性能基准：30 只股票批量获取耗时

## 验收标准

- [ ] 评分耗时从 ~60-90s 降至 ~15-25s
- [ ] 数据源可插拔（实现 Protocol 即可）
- [ ] 市场观察追加到 market_observations 并带 run_id
- [ ] 并发不触发数据源封禁
- [ ] 现有 pipeline 行为不变
