# Hermes 交易系统 — Agent 上下文

## 系统概述

Hermes 是一个 A 股量化交易辅助系统，采用"四维评分 + 风格双轨 + 事件溯源"架构。

核心流程：选股筛选 → 四维评分 → 风控检查 → 仓位计算 → 订单执行 → 报告生成

## 可用 MCP Tools

| Tool | 用途 | 副作用 |
|------|------|--------|
| `trade_market_signal` | 大盘择时信号 | 无 |
| `trade_score_stock` | 单股四维评分 | 无 |
| `trade_score_batch` | 批量评分（默认核心池） | 无 |
| `trade_portfolio` | 当前持仓 | 无 |
| `trade_pool_status` | 核心池/观察池 | 无 |
| `trade_check_risk` | 单票风控检查 | 无 |
| `trade_check_portfolio_risk` | 组合风控 | 无 |
| `trade_calc_position` | 仓位计算 | 无 |
| `trade_screener` | 选股筛选 | 无 |
| `trade_score_history` | 历史评分 | 无 |
| `trade_trade_events` | 交易记录 | 无 |
| `trade_run_pipeline` | 运行 pipeline | 有（幂等） |
| `trade_backtest` | 策略回测 | 有 |
| `trade_auto_trade` | 模拟盘自动交易（选股→评分→风控→买卖） | 有 |
| `trade_paper_status` | 模拟盘状态（持仓+资金+交易记录） | 无 |

## 关键交易规则

### 评分体系（满分 10）
- 技术面 3 分：金叉/量比/RSI/均线排列/动量
- 基本面 2 分：ROE/营收增长/现金流
- 资金流 2 分：主力净流入/北向资金
- 舆情面 3 分：研报/新闻

### 买入条件（全部满足）
- 评分 ≥ 6.5
- 大盘信号 GREEN 或 YELLOW
- 本周买入次数 < 2
- 总仓位 < 60%
- 单只仓位 < 20%
- 无一票否决信号

### 一票否决
- 价格在 MA20 下方
- 当日涨停
- 主力连续 3 日流出（站稳 MA20 + 成交额 > 5 亿时降级为警告）
- MA20 趋势向下且价格在 MA20 下方

### 风格双轨
- 慢牛（slow_bull）：低波动、RSI 50-65、止损 -8%、MA60 绝对止损、不主动止盈
- 题材（momentum）：高波动、RSI 易冲高、止损 -8%、移动止盈 -10%、时间止损 15 天

### 大盘信号
- GREEN（≥60% 指数在 MA20 上方）：正常仓位
- YELLOW（30-60%）：半仓
- RED（<30%）：禁止新开仓
- CLEAR（≥60% 指数在 MA60 下方超 15 天）：清仓观望

## 偏好

- 风格偏好：慢牛成长股优先，题材股控制仓位
- 操作频率：低频，每周最多买入 2 次
- 风控优先：宁可错过不可做错，止损纪律严格执行
- 数据质量：degraded 数据不作为自动买入依据，需人工复核
