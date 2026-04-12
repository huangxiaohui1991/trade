# Hermes-Agent / OpenClaw 接入手册

## 目标

让 `Hermes-Agent` 与 `OpenClaw` 共用同一套交易系统编排契约：

- 只调用 `bin/trade`
- 优先使用 `workflows` + `orchestrate`
- 只消费 JSON 结果
- 不直接 import `scripts.pipeline.*`

## 推荐启动顺序

说明：CLI 已兼容 `bin/trade --json doctor` 和 `bin/trade doctor --json`，下面示例统一使用 `command --json` 的推荐写法。

### 1. 先发现能力

```bash
bin/trade workflows --json
bin/trade templates --json
```

用途：
- 列出所有共享 workflow
- 获取推荐超时
- 获取可重试步骤
- 获取可复用的 agent 响应模板

### 2. 再做健康检查

```bash
bin/trade doctor --json
bin/trade state sync --target all --json
bin/trade state audit --json
bin/trade state reconcile --json
```

处理建议：
- `error`：立即停止
- `warning`：继续执行，但把 warning 原因写进摘要
- 手工修改 `portfolio.md` 或本周交易记录后，先跑一次 `state sync --target all`
- `state audit=drift`：优先修正 pool 投影漂移，再继续执行
- 若 `paper_trade_consistency=drift`，先查看 `state reconcile --json` 计划；确认后再执行 `bin/trade state reconcile --apply --json`

### 3. 最后跑工作流

盘前：

```bash
bin/trade orchestrate morning_brief --json
```

收盘：

```bash
bin/trade orchestrate close_review --json
```

周报：

```bash
bin/trade orchestrate weekly_review --json
```

全市场扫描：

```bash
bin/trade orchestrate market_scan --json
```

## 推荐超时

| Workflow | 推荐超时 |
|---|---|
| `morning_brief` | 90 秒 |
| `noon_check` | 90 秒 |
| `close_review` | 180 秒 |
| `weekly_review` | 120 秒 |
| `tracked_scan` | 240 秒 |
| `market_scan` | 360 秒 |

## 推荐重试策略

- `doctor=error`：不重试，直接汇报
- `status=blocked`：间隔 30-60 秒重试 1 次
- `status=warning`：通常不自动重试，直接继续并提示
- `market_scan`：可重试 1 次；若仍失败，改跑 `tracked_scan`

## 推荐读取字段

Hermes-Agent / OpenClaw 在读取 `run` 或 `orchestrate` 结果时，优先消费：

- `status`
- `doctor`
- `steps`
- `artifacts`
- `next_actions`
- `status_after.today_decision`
- `status_after.pool_management`

不要优先依赖：

- `result` 的大对象原文
- pipeline 内部打印日志

## Hermes-Agent 建议

- 定时任务统一调用 `orchestrate`
- 将 `artifacts` 中的文件路径直接作为汇报依据
- 只在 `close_review` 成功后再考虑触发 `market_scan`

## OpenClaw 建议

- 会话开始先跑 `workflows --json`
- 会话开始先跑 `templates --json`
- 根据用户意图选择 workflow，而不是手拼多步命令
- 只从以下字段取摘要：
  - `status`
  - `doctor`
  - `steps`
  - `artifacts`
  - `status_after.today_decision`
  - `status_after.pool_management`

## 数据查询 / 评分 / 风控 / 模拟盘命令

Agent 需要直接查询数据时（不在 orchestrate workflow 内），使用以下命令：

### 数据查询

```bash
# 技术指标（均线、RSI、动量）
bin/trade data technical 000001 --days 60 --json

# 基本面（ROE、营收增长率、经营现金流）
bin/trade data financial 000001 --json

# 资金流向（主力净流入、北向资金）
bin/trade data flow 000001 --days 5 --json

# 实时行情（价格、涨跌幅）
bin/trade data realtime 000001 600036 600519 --json

# 大盘指数状态（MA20/MA60 位置、GREEN/RED 信号）
bin/trade data market-index --json
```

### 评分

```bash
# 单股评分
bin/trade score single 000001 --json

# 批量评分（代码列表）
bin/trade score batch --codes 000001,600036 --json

# 核心池全部评分
bin/trade score pool --pool core --json

# 观察池全部评分
bin/trade score pool --pool watch --json
```

### 风控

```bash
# 黑名单 + 风险检查（ST、涨停、流动性、PE）
bin/trade risk check 000001 --json

# 组合风控（总仓位暴露、周买入次数、持仓数限制）
bin/trade risk portfolio --exposure 100000 --week-buys 2 --holding-count 5 --proposed 50000 --json

# 仓位计算（4% 风险公式）
bin/trade risk position-size --capital 450000 --price 18.50 --risk-pct 4 --json

# 止损价格计算（动态止损 + 绝对止损）
bin/trade risk stop-loss --cost 18.00 --ma20 17.50 --style momentum --highest-price 20.00 --json

# 是否应该离场检查
bin/trade risk should-exit 000001 --price 17.00 --ma20 17.50 --highest-price 20.00 --json
```

### 市场时钟

```bash
# 大盘信号（GREEN / YELLOW / RED / CLEAR）
bin/trade market signal --json

# 各指数详细数据
bin/trade market detail --json

# 仓位系数（GREEN=1.0, YELLOW=0.5, RED/CLEAR=0.0）
bin/trade market multiplier --json
```

### 模拟盘（Shadow Trade）

```bash
# 模拟盘总状态
bin/trade shadow status --json

# 检查止损/止盈信号（dry-run）
bin/trade shadow check-stops --dry-run --json

# 执行模拟买入（dry-run）
bin/trade shadow buy-new-picks --dry-run --json

# 对账并修复漂移（先 dry-run 查看）
bin/trade shadow reconcile --json
# 确认后执行修复
bin/trade shadow reconcile --apply --json

# 检查事件流与券商状态一致性
bin/trade shadow consistency --window 180 --json

# 生成 Obsidian 报告
bin/trade shadow report --json
```

## 建议的技能提示词

Hermes-Agent 的完整可粘贴版本见：
- [HERMES_AGENT_PROMPT.md](/Users/huangxiaohui/Documents/workspace/trade/docs/HERMES_AGENT_PROMPT.md)

### Hermes-Agent

```text
你是交易系统的定时编排层。优先调用 `bin/trade workflows --json` 了解能力，再调用 `bin/trade orchestrate <workflow> --json` 执行。不要直接 import Python 模块。遇到 warning 继续执行并汇报风险，遇到 blocked 或 error 停止并汇报原因。输出以 artifacts、today_decision、pool_management 为主。
```

### OpenClaw

```text
你是交易系统的交互编排层。优先读取 `bin/trade workflows --json`，根据用户意图选择最匹配的 workflow，再调用 `bin/trade orchestrate <workflow> --json`。不要手工拼接 pipeline 命令，除非用户明确要求单步重跑。只消费 JSON 输出，不依赖仓库内部模块返回结构。
```
