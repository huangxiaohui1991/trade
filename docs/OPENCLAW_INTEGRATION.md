# OpenClaw 集成说明

## 定位

本仓库提供稳定的交易系统内核，`openclaw/hermes` 作为外层 orchestrator，不直接 import 仓库内部模块。

推荐分层：

- 本仓库：数据获取、评分、择时、风控、Obsidian 落盘、运行状态、缓存
- OpenClaw：技能封装、命令编排、多步对话、任务拆分、结果展示

## 推荐调用方式

统一走 CLI：

```bash
bin/trade workflows --json
bin/trade doctor --json
bin/trade status today --json
bin/trade orchestrate morning_brief --json
bin/trade orchestrate close_review --json
bin/trade orchestrate weekly_review --json
bin/trade run morning --json
bin/trade run noon --json
bin/trade run evening --json
bin/trade run scoring --json
bin/trade run weekly --json
bin/trade run screener --universe market --pool all --json
```

如果不使用 `bin/trade`，也可以直接：

```bash
python3 -m scripts.cli.trade --json doctor
```

建议先看共享 workflow：

```bash
bin/trade workflows --json
```

## CLI 契约

### 0. `workflows`

用途：
- 返回 Hermes-Agent / OpenClaw 可共用的 workflow 列表
- 给外层提供推荐超时和可重试步骤

返回关键字段：

```json
{
  "command": "workflows",
  "items": [
    {
      "name": "close_review",
      "steps": ["status", "evening", "scoring"],
      "timeout_seconds": 180,
      "retryable_steps": ["evening", "scoring"]
    }
  ]
}
```

### 1. `doctor`

用途：
- 执行健康检查
- 在自动化运行前做 preflight

返回关键字段：

```json
{
  "command": "doctor",
  "status": "success|warning|error",
  "hard_fail": [],
  "warning": [],
  "checks": {
    "python": {},
    "mx_apikey": {},
    "discord_webhook": {},
    "vault": {},
    "writable": {},
    "daily_state": {},
    "mx_connectivity": {},
    "akshare_connectivity": {}
  }
}
```

使用建议：
- `status=error`：直接阻断后续步骤
- `status=warning`：允许继续，但要在对话里提示降级风险

### 2. `run <pipeline>`

支持：
- `morning`
- `noon`
- `evening`
- `scoring`
- `weekly`
- `screener`

筛选命令：

```bash
bin/trade run screener --universe tracked --pool all --json
bin/trade run screener --universe market --pool all --json
```

返回关键字段：

```json
{
  "pipeline": "morning",
  "run_id": "morning_20260409_171512_85202",
  "status": "success|warning|error|skipped|blocked",
  "retryable": false,
  "started_at": "2026-04-09T17:15:12",
  "finished_at": "2026-04-09T17:15:53",
  "duration_seconds": 41.261,
  "details": {},
  "result": {},
  "result_path": "data/runs/...",
  "daily_state_path": "data/runtime/..."
}
```

状态语义：
- `success`：运行成功
- `warning`：运行完成，但存在降级或依赖问题
- `error`：运行失败
- `skipped`：根据输入或状态主动跳过
- `blocked`：被 `doctor` 或运行锁阻断

### 3. `status today`

用途：
- 查看今日运行摘要
- 获取统一的 `today_decision`

返回关键字段：

```json
{
  "command": "status",
  "date": "2026-04-09",
  "pipelines": {},
  "today_decision": {
    "decision": "BUY_ALLOWED|REDUCED_BUY|NO_TRADE",
    "market_signal": "GREEN|YELLOW|RED|CLEAR",
    "risk": {},
    "reasons": []
  },
  "pool_management": {
    "summary": {
      "promote_to_core": 0,
      "keep_watch": 0,
      "add_to_watch": 0,
      "demote_from_core": 0,
      "remove_or_avoid": 0
    }
  }
}
```

### 4. `orchestrate <workflow>`

推荐给 `openclaw/hermes` 直接调用，减少外层自己编排多步命令。

支持：
- `morning_brief`
- `noon_check`
- `close_review`
- `weekly_review`
- `tracked_scan`
- `market_scan`

返回关键字段：

```json
{
  "command": "orchestrate",
  "workflow": "close_review",
  "status": "success|warning|error|blocked",
  "doctor": {},
  "steps": [],
  "artifacts": [],
  "status_before": {},
  "status_after": {}
}
```

建议：
- Hermes 定时任务优先调用 `orchestrate`
- OpenClaw 技能也优先调用 `orchestrate`
- 只有在需要单独重跑某一步时，才直接调用 `run <pipeline>`

## 推荐的 openclaw/hermes Skill 编排

### 盘前

1. `trade orchestrate morning_brief --json`
2. 读取 `status_after.today_decision`
3. 读取 `status_after.pool_management`
4. 把 `decision + market_signal + weekly_remaining + 池子调整建议数` 转成用户可读摘要

### 收盘

1. `trade orchestrate close_review --json`
2. 如需全市场扫描，再执行 `trade orchestrate market_scan --json`
3. 汇总 `artifacts` 和 `status_after` 指向的落盘文件与状态摘要

### 周报

1. `trade orchestrate weekly_review --json`
2. 用 `artifacts` 和 `status_after` 生成总结

## 工程建议

- `openclaw` 不要依赖 `scripts.pipeline.*` 的返回结构
- `openclaw` 只消费 CLI JSON
- 对 `warning` 做“继续但提示”的处理
- 对 `blocked` 和 `error` 做“停止并汇报”的处理
- 对长耗时命令保留超时与重试策略

## 当前边界

- `sentiment` 定时任务仍未接入独立 skill
- `run` 命令返回的 `result` 体量可能较大，`openclaw` 只应消费摘要字段
- 市场数据链路虽然已有缓存和降级，但外部接口慢时耗时仍会偏高
