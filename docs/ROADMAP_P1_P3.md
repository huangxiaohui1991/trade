# 交易系统路线图（P1 / P2 / P3）

## 目标

把当前系统从“P0 闭环已打通、可以跑、部分结果可信”推进到：

1. `P1`：主页可信，自动化口径一致，状态可审计
2. `P2`：执行层完整，组合风控和订单状态进入结构化管理
3. `P3`：策略验证与复盘归因完善，形成持续优化闭环

执行原则：

- 先补自动化真相层，再补执行层，最后做研究优化层
- 优先做“会影响今天决策可信度”的项
- 每一项都要求有可验证的 JSON 输出、状态审计或测试覆盖

---

## 总览

| 方向 | 当前状态 | 优先级 | 备注 |
|---|---|---|---|
| 统一仓位账本 | 部分完成 | `P1` | `cn_a_system/hk_legacy` 已结构化，`paper_mx` 仍未完全收口为标准持仓快照 |
| 统一信号总线 | 部分完成 | `P1` | 已有部分 `reason_code`，但 market/pool/trade 还没有统一 schema |
| 真正的池子引擎 | 基本可用 | `P2` | snapshot + projection + 自选股同步已接通，但还缺事务化与对账强化 |
| 回测与 walk-forward | 未开始 | `P3` | 当前只有文档/目录占位，没有实际引擎 |
| 组合级风控 | 未开始 | `P2` | 目前只做到单票/总仓位/周频控制 |
| 订单生命周期 | 未开始 | `P2` | 只有 broker 能力与文档设想，没有结构化状态表 |
| 复盘归因 | 未开始 | `P3` | 没有 MFE/MAE / entry-exit factor / rule attribution |
| 告警中心 | 少量基础 | `P3` | 只有零散 Discord 推送和 veto/advisory，没有统一中心 |
| 可观测性 | 部分完成 | `P1` | 已有 doctor/state/status/tests，但 contract fixtures 和 E2E 覆盖不足 |

---

## P1：可信状态层

### P1-1 统一仓位账本收口

目标：

- 三个账户 `cn_a_system / hk_legacy / paper_mx` 都能通过同一接口读取标准持仓对象
- `status today` 不再对 `paper_mx` 走特例逻辑
- drift 可以被识别、审计、修复

当前基础：

- `cn_a_system`、`hk_legacy` 已结构化
- `paper_mx` 已有交易事件、对账审计、reconcile 计划

待办 backlog：

1. 把 `paper_mx` 真正写入标准 portfolio snapshot
2. 给 `paper_mx` 增加结构化 balance / exposure 输出
3. 统一 `load_portfolio_snapshot(scope=...)` 的字段契约
4. 让 `status today` / `doctor` / `state audit` 直接消费同一份 `paper_mx` snapshot
5. 给 `paper_mx` 加回归测试：空仓 / 持仓 / drift / reconcile 后恢复一致

验收标准：

- `load_portfolio_snapshot(scope="paper_mx")` 可稳定返回 positions + balances + summary
- `bin/trade status today --json` 能同时给出主仓位和模拟盘标准摘要
- `bin/trade state audit --json` 能稳定识别 paper drift

---

### P1-2 统一信号总线

目标：

- 市场状态、评分状态、池子动作、交易动作都输出统一标准原因码
- 上层 agent 只需要读结构化 `reason_code` / `reason_category`，不再依赖自然语言解析

当前基础：

- trade events 已部分有 `reason_code`
- pool / scorer / shadow trade 已有局部 code，但 schema 不统一

待办 backlog：

1. 建立共享 reason code registry
2. 定义 reason category：`market / score / pool / trade / risk / reconcile`
3. `status today` 输出统一 reason summary
4. `today_decision` 输出标准 market reason codes
5. pool suggestions 输出标准 action reason codes
6. trade events / shadow trade / reconcile actions 全部收敛到统一 registry

验收标准：

- `status today --json` 含统一 `reason_summary`
- `today_decision` / `paper_trade_audit` / `pool_sync_state` 都能映射到标准 code
- 新增 contract tests 覆盖 JSON schema

---

### P1-3 可观测性与契约测试

目标：

- `status today` 成为可信首页
- CLI 输出契约化，便于 Hermes/OpenClaw 长期消费

当前基础：

- 已有 `doctor / state audit / state reconcile / status today`
- 已有 P0/P1 单元测试

待办 backlog：

1. 增加 CLI contract tests：`doctor / state audit / state reconcile / status today`
2. 增加 deterministic fixtures，避免测试依赖外部 broker / 网络
3. 增加最小 E2E smoke：`sync -> audit -> status`
4. 明确 JSON 字段版本和兼容策略
5. 为关键 drift 场景保留固定样例

验收标准：

- 关键 CLI JSON 都有稳定测试
- 不接外部服务也能跑核心 contract tests
- 变更 `status today` 结构时会被测试拦住

---

## P2：执行控制层

### P2-1 真正的池子引擎强化

目标：

- 将当前“基本可用”的池子引擎做成强约束、可对账、可回滚的状态变更层

待办 backlog：

1. pool snapshot 写入与 projection 更新统一成单事务边界
2. 自选股同步结果纳入结构化审计
3. pool drift 从“评分不一致”扩展到“projection / 自选股 / snapshot 全量一致”
4. 增加 pool action 历史记录表
5. 支持明确的 `promote/demote/remove/keep` 事件流

验收标准：

- 一次 pool 运行后，snapshot / yaml / md / 自选股状态可审计
- pool action 有历史可追溯

---

### P2-2 组合级风控

目标：

- 决策从单票风控升级到组合风控

待办 backlog：

1. 板块/题材集中度
2. 组合相关性约束
3. 单日最大回撤阈值
4. 连续亏损冷却机制
5. 事件风险日历（财报/解禁/重大事项）

验收标准：

- `today_decision` 能输出组合级风控限制
- 买入建议会因为组合风险被拒绝或降级

---

### P2-3 订单生命周期

目标：

- 把“建议单 / 条件单 / 成交 / 撤单 / 复核”做成结构化状态流

待办 backlog：

1. 新增订单状态表
2. 定义生命周期：`candidate -> placed -> partially_filled -> filled -> cancelled -> exception -> reviewed`
3. 接入 broker 返回的委托/成交信息
4. 挂单提醒与复核提醒统一入状态表
5. 周报/状态页读取订单状态，而不是只看 free-form 文案

验收标准：

- 任一订单都有明确生命周期状态
- `status today` 能看见待确认/异常订单

---

## P3：研究与优化层

### P3-1 回测与 walk-forward

目标：

- 让评分阈值、权重、止损止盈参数可验证，而不是靠经验固定

待办 backlog：

1. 历史样本数据组织
2. 评分重放与策略回测引擎
3. parameter sweep
4. walk-forward 验证
5. 回测结果持久化与报告

验收标准：

- 至少能回测买入阈值、评分权重、止损/止盈参数
- 有 walk-forward 报表，而不是单段历史拟合

---

### P3-2 复盘归因

目标：

- 每笔交易都有结构化复盘材料

待办 backlog：

1. entry / exit 因子记录
2. 违规则标注
3. MFE / MAE
4. 持仓时长与收益拆解
5. 周报/复盘自动生成归因摘要

验收标准：

- 任一交易可以回答“为什么买、为什么卖、是否按规则执行、最佳/最差路径如何”

---

### P3-3 告警中心

目标：

- 从“零散消息推送”升级成统一告警调度层

待办 backlog：

1. 统一 alert model
2. 舆情/财报/异动/涨停回落/放量破位/池子失分接入
3. 告警去重与节流
4. 告警审计与处理状态
5. 与 Discord / CLI / 周报统一消费

验收标准：

- 告警具备类型、严重度、状态、处理记录
- 同类异常不会重复轰炸

---

## 推荐执行顺序

### 第一阶段

1. `P1-1` 统一仓位账本收口
2. `P1-2` 统一信号总线
3. `P1-3` 可观测性与契约测试

目标：

- 让首页可信、状态统一、自动化能放心消费

### 第二阶段

4. `P2-2` 组合级风控
5. `P2-3` 订单生命周期
6. `P2-1` 池子引擎强化

目标：

- 让执行控制层完整，减少“系统建议正确但执行面失真”

### 第三阶段

7. `P3-1` 回测与 walk-forward
8. `P3-2` 复盘归因
9. `P3-3` 告警中心

目标：

- 让系统从“可运行”进入“可验证、可优化、可持续演进”

---

## 当前并行推进项

- `P1-1`：paper 标准账本闭环
- `P1-2`：reason code / signal bus 基础统一
- `P1-3`：CLI contract tests / fixtures

主线程职责：

- 保持路线图与优先级不漂移
- 审核并集成并行结果
- 每完成一个 P1 子项，就更新 `status today` 契约和测试
