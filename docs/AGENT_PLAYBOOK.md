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
