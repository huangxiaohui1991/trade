# 01 — 第一性原理

## 交易系统里真正重要的四类对象

### 事实 (Facts)

不可变的业务事件。发生了就不能改。

- 订单创建、订单成交、订单取消
- 持仓变化（开仓、加仓、减仓、清仓）
- 策略决策（评分结果、买入意图、卖出意图）
- 风险触发（止损、止盈、时间止损、熔断）
- 池子变动（晋升核心池、降级、移除）

**存储方式：** append-only event log，只 INSERT 不 UPDATE/DELETE。

### 观察 (Observations)

外部世界的观测数据。也是追加保存，但量级和查询模式与业务事实不同。

- K 线（日线、分时）
- 财报数据
- 资金流向
- 新闻、舆情
- 大盘指数

**存储方式：** 追加式时序存储。前期 SQLite，体量膨胀后外溢到 Parquet/DuckDB。

### 规则 (Rules)

策略参数、风控规则、仓位规则、择时规则。规则必须版本化。

- strategy.yaml 的每次变更都是一个新版本
- 每次运行必须冻结 config_version + config_hash
- 回测时可精确还原任意历史版本的规则

**存储方式：** config_versions 表，每行一个完整的 config JSON 快照。

### 投影 (Projections)

从事实和观察派生出来的当前状态视图。全部可删可重建。

- 当前持仓表
- 当前核心池 / 观察池
- 日报、周报、月报
- Obsidian 页面
- Discord 消息
- Dashboard 数据

**投影不是事实源。** Obsidian 只能是生成物或人工注释层，不能再当主状态源。

---

## 非谈判项

这些是架构的硬约束，不可妥协：

1. **每个评分、决策、下单、报告都必须带 `run_id`**
2. **每个业务结果都必须带 `strategy_version`、`config_hash`、`data_snapshot_ref`**
3. **Domain 代码不能 import HTTP、SQL、YAML、文件系统**
4. **回测和实盘共用同一套 strategy/risk 核心，只替换时钟、数据源、执行器**
5. **不允许直接改"当前状态表"作为事实源，只能追加事件，再投影成当前状态**
6. **任何"当前状态"表都必须能从 `event_log` + `market_observations` 重建**
7. **金额存最小货币单位整数（分），避免浮点坑**

---

## 先不做的事

- 不先上 FastAPI。等出现第二个稳定调用方再包 HTTP 层。
- 不先上后台 worker。单进程同步投影足够。
- 不先拆太细的 bounded context。等某个 context 超过 10 个文件再细分。
- 不让 Obsidian 继续承担真相源。
