# 交易系统任务跟踪

更新时间：2026-04-10

说明：

- `[x]` 已完成
- `[ ]` 未完成
- 本文档只跟踪可执行任务，不重复写背景说明
- 后续每完成一项，直接在这里打勾并补一行结果备注

---

## 已完成

### 状态层 / 账本

- [x] 结构化 ledger 落地，统一状态服务可读 `portfolio / market / pool / activity`
- [x] 三账户 scope 建模：`cn_a_system / hk_legacy / paper_mx`
- [x] `state bootstrap / audit / sync / reconcile` 命令接入 CLI
- [x] `paper_mx` drift 审计和 reconcile 流程落地
- [x] `status today` 统一读结构化状态，不再直接解析 Markdown

### 信号 / 风控 / 执行

- [x] `reason_code` 主干收敛，`status today` 有统一 `signal_bus`
- [x] 大盘信号统一成单一来源，修正指数口径问题
- [x] 周买入次数、交易事件、周报统计改读结构化事件
- [x] advisory 风控输出：`time_stop / drawdown_take_profit`
- [x] 组合级风控第一版接入 `today_decision`
- [x] 订单状态第一版：`orders` 表、状态快照、提醒、确认
- [x] pool snapshot / action history / projection 双写打通

### 回测 / 复盘 / 观测

- [x] `backtest run / sweep / walk-forward` 基础框架
- [x] backtest 历史样本、JSON 结果、Markdown 报表落盘
- [x] `backtest history / compare` 历史索引与横向对比
- [x] MFE / MAE 代理估算落地
- [x] 有历史日线时优先重建真实 `MFE / MAE`
- [x] 历史日线可用时，单笔止损/止盈规则重放
- [x] 评分权重 sweep 接入 backtest
- [x] 组合级回放第一版：逐日时间线、峰值占用、并发持仓
- [x] 组合级回放加入总仓位 / 单票上限 / 资金占用约束
- [x] 告警中心第一版
- [x] 结构化复盘归因第一版
- [x] CLI contract tests / fixture / core e2e smoke 主干建立

### MX 能力层

- [x] `scripts.mx.cli_tools` 能力注册表 / dispatcher 落地
- [x] `bin/trade mx list / groups / run / health` 接入
- [x] `stock_screener` 改走 MX capability layer
- [x] `shadow_trade` 改走 MX capability layer
- [x] `status today` 暴露 `mx_health`

---

## 进行中

### 回测研究层

- [x] 把组合级回放从“闭合交易回放”推进到“真实逐日组合模拟”
备注：已落地日级事件主循环，逐日推进 cash / entry / exit / exposure；当前仍基于闭合交易样本，完整信号驱动策略引擎继续放在 P3。

- [x] 同日多信号竞争资金的排序规则
备注：已按 `entry_score desc -> realized_pnl desc -> desired_capital asc` 固定排序，并输出 accepted/rejected allocation 明细。

- [x] walk-forward 多 fold 报表强化
备注：已补 fold comparison / aggregate comparison，输出 selected parameters 以及 train vs eval 的 pnl / win_rate / sample_count 对照。

### MX 流程接入

- [x] `morning` 资讯搜索完全切到 MX capability layer
备注：盘前资讯已统一经 `dispatch_mx_command("news", ...)` 走 capability layer，并补了可用/降级测试。

---

## 待做

### P1 收尾

- [x] `paper_mx` 完全收口为标准 portfolio snapshot
备注：`load_portfolio_snapshot("paper_mx")` 固定经 broker 刷新并回写 ledger，失败时保留缓存快照。
- [x] `paper_mx` 的 balance / exposure 契约与主仓一致
备注：paper snapshot summary 已统一输出 `holding_count / current_exposure / cash_value / total_capital`。
- [x] `status today` / `doctor` / `state audit` 完全共享同一份 paper snapshot
备注：`status today` 暴露 `paper_mx_portfolio`，`state audit/doctor` 共享 `paper_portfolio_snapshot` 检查。

### P2 执行层增强

- [x] pool 引擎事务化与 projection 对账强化
备注：`screener` 已收敛为只走统一 `save_pool_snapshot` 投影；`audit_state` 增加 missing/extra/score/bucket drift 明细。
- [x] 订单生命周期第二阶段：`partial_fill / cancel_replace / review_queue`
- [x] broker 回报更细状态接入订单状态表
备注：broker sync 已接入 `cancel_requested / cancel_replace_pending / review_required / partially_filled`，部分成交按增量写交易事件并保留 raw broker status。
- [x] 组合风控扩展到板块集中度 / 相关性 / 事件风险
备注：`check_portfolio_risk` 已输出板块集中度、相关性分组集中度、事件风险日 warning reason code 与 metrics。

### P3 研究级能力

- [ ] 真正的逐日策略回放引擎
- [ ] 回测参数扩展到 veto / 评分更多维度
- [ ] 组合层连续亏损冷却纳入回测
- [ ] 组合层资金占用与持仓上限纳入 walk-forward 评估
- [ ] 回测结果对比视图继续强化

### 复盘归因深化

- [ ] entry factor 结构化沉淀
- [ ] exit factor 结构化沉淀
- [ ] 收益归因拆解
- [ ] 规则偏离解释
- [ ] 组合级归因摘要

### 告警中心深化

- [ ] 财报告警接入
- [ ] 异动 / 放量破位 / 涨停回落告警接入
- [ ] 池子失分告警接入
- [ ] 告警去重、节流、处理状态

---

## 下一阶段顺序

1. [x] `morning` 完全切到 MX capability layer
2. [x] 同日多信号竞争资金排序规则
3. [x] 逐日组合模拟主循环
4. [x] 订单生命周期第二阶段
5. [x] 组合级风控深化
6. [ ] 复盘归因深化

---

## 完成定义

- “完成”指代码已落地、测试已覆盖、CLI 或流程已有可验证输出
- “进行中”指已有主干，但口径或深度还不足以算收口
- “待做”指尚未进入稳定实现阶段
