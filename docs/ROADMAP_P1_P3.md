# 交易系统路线图（P1 / P2 / P3）

> 更新日期：2026-04-10
> 状态：全部完成 ✅

## 目标

把系统从"P0 闭环已打通"推进到：

1. `P1`：主页可信，自动化口径一致，状态可审计
2. `P2`：执行层完整，组合风控和订单状态进入结构化管理
3. `P3`：策略验证与复盘归因完善，形成持续优化闭环

---

## 总览

| 方向 | 状态 | 优先级 | 备注 |
|---|---|---|---|
| 统一仓位账本 | ✅ | `P1` | 三账户标准 snapshot，paper_mx 完全收口 |
| 统一信号总线 | ✅ | `P1` | reason_code registry 统一，signal_bus 接入 status today |
| 可观测性与契约测试 | ✅ | `P1` | contract tests / fixtures / e2e smoke |
| 池子引擎强化 | ✅ | `P2` | 事务化 snapshot + projection 对账 + action history |
| 组合级风控 | ✅ | `P2` | 板块集中度 / 相关性 / 事件风险 / 连续亏损冷却 |
| 订单生命周期 | ✅ | `P2` | partial_fill / cancel_replace / review_queue |
| 回测与 walk-forward | ✅ | `P3` | run / sweep / walk-forward / strategy-replay 全部落地 |
| 复盘归因 | ✅ | `P3` | entry/exit factors / pnl_attribution / rule_deviation |
| 告警中心 | ✅ | `P3` | 财报/异动/池子失分/去重节流/处理状态 |

---

## P1：可信状态层 ✅

### P1-1 统一仓位账本收口 ✅

- `load_portfolio_snapshot(scope=...)` 三账户统一接口
- `paper_mx` 经 broker 刷新并回写 ledger，失败时保留缓存快照
- paper snapshot summary 统一输出 `holding_count / current_exposure / cash_value / total_capital`
- `status today` / `doctor` / `state audit` 共享同一份 paper snapshot
- drift 可识别、审计、reconcile 修复

### P1-2 统一信号总线 ✅

- 共享 reason code registry，覆盖 `market / score / pool / trade / risk / reconcile`
- `status today` 输出统一 `signal_bus`（market / pool / trade 各有 primary_code + state + reason_codes）
- `today_decision` 输出标准 market reason codes
- pool / trade / shadow trade / reconcile 全部收敛到统一 registry

### P1-3 可观测性与契约测试 ✅

- CLI contract tests 覆盖 `doctor / state audit / state reconcile / status today / backtest`
- deterministic fixtures，不依赖外部 broker / 网络
- E2E smoke：`sync -> audit -> status`
- 变更 JSON 结构时会被测试拦住

---

## P2：执行控制层 ✅

### P2-1 池子引擎强化 ✅

- pool snapshot 写入与 projection 更新统一事务边界
- `audit_state` 增加 missing/extra/score/bucket drift 明细
- pool action 历史记录可追溯
- `promote/demote/remove/keep` 事件流结构化

### P2-2 组合级风控 ✅

- `check_portfolio_risk` 输出板块集中度、相关性分组集中度、事件风险日 warning
- 单日最大回撤阈值 + 连续亏损冷却机制
- `today_decision` 输出组合级风控限制
- 买入建议会因组合风险被拒绝或降级

### P2-3 订单生命周期 ✅

- 订单状态表：`candidate -> placed -> partially_filled -> filled -> cancelled -> exception -> reviewed`
- broker sync 接入 `cancel_requested / cancel_replace_pending / review_required / partially_filled`
- 部分成交按增量写交易事件并保留 raw broker status
- `status today` 可见待确认/异常订单

---

## P3：研究与优化层 ✅

### P3-1 回测与 walk-forward ✅

- `backtest run / sweep / walk-forward` 全部落地
- proxy 级 parameter sweep + walk-forward 训练窗口选参 + 测试窗口评估
- 信号驱动逐日策略回放引擎（`strategy_replay.py`）
- 参数 sweep 扩展到 veto presets / watch_threshold / reject_threshold / time_stop_days
- MFE / MAE 真实重建 + 代理估算
- 组合级回放（资金占用 / 持仓上限 / 连续亏损冷却）
- 回测结果对比视图（leaderboard / 风险状态分布）

### P3-2 复盘归因 ✅

- entry / exit factors 结构化沉淀
- pnl_attribution（outcome / pnl_pct / MFE 捕获率 / exit_style）
- rule_deviation（compliant / reconcile / manual_override / drift）
- 组合级归因摘要（portfolio_attribution_summary），周报同步展示

### P3-3 告警中心 ✅

- 统一 alert model（类型 / 严重度 / 状态 / 处理记录）
- 财报/异动/放量破位/涨停回落/池子失分接入
- 告警去重与节流（alert_key / throttled / suppressed count）
- 告警审计与处理状态（handling_status / ack）

---

## 后续方向

所有 P1-P3 任务已收口。后续可考虑：

- 舆情监控定时任务接入独立告警流
- Discord 消息监听机制（Hermes 接收用户回复）
- 回测参数校准实盘参数
- 超时未确认提醒（T+1 再提醒 / T+2 异常标记）
