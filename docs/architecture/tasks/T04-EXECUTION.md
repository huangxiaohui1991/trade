# T04 — execution + portfolio

> Phase 4 | 预估 1 周 | 优先级：P1 | 依赖：T01, T02

## 目标

订单和持仓事件化。当前状态从 event_log 投影重建，不直接修改状态表。

## 背景

当前 `state/service.py`（1500+ 行）直接读写持仓/订单表。
迁移策略：先做薄 adapter 桥接，逐步将写操作改为"追加事件 → 更新投影"。

## 子任务

### T04.1 execution models

- [ ] 创建 `src/hermes/execution/models.py`
- [ ] `Order` — order_id, code, side, shares, price_cents, status, broker
- [ ] `Position` — code, name, style, shares, avg_cost_cents, entry_date, ...
- [ ] `Balance` — scope, cash_cents, total_asset_cents, weekly_buy_count, ...
- [ ] `TradeEvent` — 统一交易事件（买入/卖出/止损/止盈）

### T04.2 订单事件化

- [ ] 创建 `src/hermes/execution/orders.py`
- [ ] `create_order(code, side, shares, price, broker, run_id)` → 追加 `order.created` 事件
- [ ] `fill_order(order_id, fill_price, fee, run_id)` → 追加 `order.filled` 事件
- [ ] `cancel_order(order_id, reason, run_id)` → 追加 `order.cancelled` 事件
- [ ] 每个操作同步更新 `projection_orders`

### T04.3 持仓投影

- [ ] 创建 `src/hermes/execution/positions.py`
- [ ] `PositionProjector.rebuild(event_store) -> list[Position]`
  - 遍历 position.* 事件，重建当前持仓
- [ ] `PositionProjector.update_from_event(event)` — 增量更新
- [ ] `open_position(code, shares, cost, style, run_id)` → 追加 `position.opened` + 更新投影
- [ ] `close_position(code, shares, price, run_id)` → 追加 `position.closed` + 更新投影

### T04.4 ExecutionService

- [ ] 创建 `src/hermes/execution/service.py`
- [ ] `get_portfolio() -> dict` — 从投影表读取
- [ ] `get_positions() -> list[Position]` — 从投影表读取
- [ ] `execute_intent(intent: DecisionIntent, run_id) -> Order` — 生成订单
- [ ] `process_fill(order_id, fill_price, fee, run_id)` — 处理成交回报
- [ ] `rebuild_projections()` — 从 event_log 完全重建

### T04.5 桥接现有 state/service.py

- [ ] 现有 `state/service.py` 的读操作 → 委托给 ExecutionService
- [ ] 现有写操作 → 逐步改为"追加事件 + 更新投影"
- [ ] 不急着删 state/service.py，先共存

### T04.6 模拟盘 adapter

- [ ] `SimulatedBroker` — 回测用，立即成交
- [ ] `MXBroker` — 妙想模拟盘 adapter（兼容现有 shadow_trade）
- [ ] broker adapter 统一接口：`submit_order() -> OrderResult`

### T04.7 测试

- [ ] 事件化测试：create_order → fill_order → 验证 projection_orders 状态
- [ ] 重建测试：删除 projection_positions → rebuild → 数据一致
- [ ] 幂等测试：重复 fill 同一 order 不产生重复事件

## 验收标准

- [ ] 持仓状态可从 event_log 完全重建
- [ ] 删除 projection_positions 后 rebuild，数据与重建前一致
- [ ] 订单操作全部事件化，event_log 有完整审计链
- [ ] 现有 pipeline 行为不变
