# T06 — MCP Server + Hermes Agent 融合

> Phase 6 | 预估 1 周 | 优先级：P1 | 依赖：T01-T05

## 目标

将交易系统能力通过 MCP 暴露给 Hermes Agent。
配置 Skills、Cron、Memory，实现端到端自动化。

## 子任务

### T06.1 MCP Server

- [ ] 创建 `src/hermes/platform/mcp_server.py`
- [ ] 使用 `mcp` Python SDK，stdio transport
- [ ] 实现以下 tools：

| Tool | 参数 | 说明 |
|------|------|------|
| `trade_market_signal` | 无 | 大盘择时信号 |
| `trade_score_stock` | `code` | 单股四维评分 |
| `trade_score_batch` | `codes` (可选) | 批量评分 |
| `trade_portfolio` | 无 | 当前持仓 |
| `trade_pool_status` | 无 | 核心池/观察池 |
| `trade_check_risk` | `code` | 持仓风控检查 |
| `trade_check_portfolio_risk` | 无 | 组合风控 |
| `trade_calc_position` | `code, score, price` | 仓位计算 |
| `trade_screener` | `query` (可选) | 选股筛选 |
| `trade_backtest` | `start, end, preset` | 策略回测 |
| `trade_score_history` | `code, days` | 历史评分 |
| `trade_trade_events` | `days` | 交易记录 |
| `trade_run_pipeline` | `pipeline_type` | 运行 pipeline（带幂等） |

- [ ] 每个 tool handler 调用对应 context 的 service
- [ ] 幂等设计：查询类天然幂等，pipeline 类通过 RunJournal 检查
- [ ] 错误处理：返回 `{error, fallback_data}`，不抛异常
- [ ] Config freeze on MCP Server 启动

### T06.2 Hermes Agent 配置

- [ ] MCP Server 配置片段：

```yaml
# ~/.hermes/config.yaml
mcp_servers:
  hermes_trade:
    command: "python"
    args: ["-m", "hermes.platform.mcp_server"]
    env:
      HERMES_ROOT: "/path/to/hermes-trade"
    timeout: 120
```

### T06.3 Skills

- [ ] `agent/skills/trade-morning/SKILL.md` — 盘前摘要流程
- [ ] `agent/skills/trade-evening/SKILL.md` — 收盘报告流程
- [ ] `agent/skills/trade-scoring/SKILL.md` — 核心池评分流程
- [ ] `agent/skills/trade-screener/SKILL.md` — 选股分析流程
- [ ] `agent/skills/trade-analysis/SKILL.md` — 个股深度分析流程

### T06.4 Cron Jobs

- [ ] 盘前摘要：周一~五 08:25
- [ ] 午休检查：周一~五 11:55
- [ ] 收盘报告：周一~五 15:35
- [ ] 核心池评分：周一~五 15:40
- [ ] 周报：周日 20:00

### T06.5 CONTEXT.md + Memory

- [ ] `agent/CONTEXT.md` — 项目上下文（系统概述、可用 tools、关键规则）
- [ ] Memory 初始内容 — 交易规则、偏好、风格

### T06.6 CLI 保留

- [ ] `hermes run morning/evening/scoring` — 手动触发 pipeline
- [ ] `hermes score <code>` — 单股评分
- [ ] `hermes status` — 持仓概览
- [ ] `hermes events query` — 查询事件
- [ ] CLI 调用同一套 service，与 MCP 共享逻辑

### T06.7 端到端验证

- [ ] Agent 对话："帮我看看 002138 能不能买" → 完整评分 + 建议
- [ ] Agent 对话："今天有什么好股票" → 选股 + 评分 + 排序
- [ ] Cron 盘前摘要 → Discord 收到消息
- [ ] Cron 收盘报告 → Discord 收到消息
- [ ] 止损触发 → Agent 主动告警
- [ ] 旧 crontab 停用后系统正常运行

## 验收标准

- [ ] Hermes Agent 能通过 MCP 调用所有 trade_* tools
- [ ] Cron 按时执行，结果投递到 Discord/Telegram
- [ ] Agent 记住交易规则（Memory 生效）
- [ ] 旧 crontab 可安全停用
- [ ] CLI 仍可独立使用（不依赖 Agent）
