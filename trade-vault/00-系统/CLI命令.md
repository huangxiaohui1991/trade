---
type: guide
tags: [CLI, 命令, bin/trade]
version: v1.0
updated: 2026-04-13
---

# CLI 命令详解

> 版本：v1.0
> 更新日期：2026-04-13

> 所有命令均通过 `bin/trade` 调用，底层使用 `.venv/bin/python`

---

## 一、系统命令

### 健康检查

```bash
bin/trade doctor
```

检查：数据源连通性、API 状态、目录写权限、配置文件有效性。

### 查看状态

```bash
bin/trade status today       # 今日状态（持仓、信号、决策）
bin/trade --json status today  # JSON 格式输出（供 Agent 使用）
```

### 查看 Workflow

```bash
bin/trade workflows          # 列出所有编排 workflow
```

---

## 二、每日任务（run）

```bash
# 盘前 08:25
bin/trade run morning

# 午休 11:55
bin/trade run noon

# 收盘 15:00
bin/trade run evening

# 核心池评分 + 分池 15:40
bin/trade run scoring

# 选股流水线
bin/trade run screener --pool all --universe tracked   # 已跟踪池（推荐）
bin/trade run screener --pool all --universe market     # 全市场

# 舆情监控
bin/trade run sentiment --dry-run   # 仅扫描，不推送 Discord
bin/trade run sentiment             # 扫描 + 推送

# 港股监控
bin/trade run hk_monitor --dry-run

# 月度复盘
bin/trade run monthly --month 2026-04
```

---

## 三、编排任务（orchestrate）

```bash
# 盘前简报
bin/trade orchestrate morning_brief

# 午盘检查
bin/trade orchestrate noon_check

# 收盘复盘（收盘 + 评分）
bin/trade orchestrate close_review

# 周复盘
bin/trade orchestrate weekly_review

# 已跟踪池扫描（稳定，推荐日常使用）
bin/trade orchestrate tracked_scan --pool all

# 全市场扫描（依赖妙想 API，有超时风险）
bin/trade orchestrate market_scan --pool all
```

---

## 四、妙想 MX 命令

```bash
# 列出所有 MX 命令
bin/trade mx list

# 查看 MX 命令分组
bin/trade mx groups

# MX 健康检查
bin/trade mx health

# 运行 MX 命令
bin/trade mx run mx.xuangu.search --query "站上20日均线 ROE大于8%"
bin/trade mx run mx.zixuan.query
bin/trade mx run mx.moni.buy --stock-code 300938 --price 19.50 --shares 100
bin/trade mx run mx.moni.sell --stock-code 300938 --price 21.00 --shares 100
bin/trade mx run mx.moni.cancel --order-id 12345
bin/trade mx run mx.moni.cancel-all
```

---

## 五、数据查询

```bash
# 技术指标
bin/trade data technical 300938 --days 60

# 基本面数据
bin/trade data financial 300938

# 资金流
bin/trade data flow 300938
```

---

## 六、回测引擎

```bash
# 单股回测验证
bin/trade backtest validate-single --code 300938 --start 2026-01-01 --end 2026-04-13

# 批量回测（多只股票）
bin/trade backtest batch --codes 300938,688123,601126 --start 2026-01-01 --end 2026-04-13

# 批量回测（使用 Preset）
bin/trade backtest batch --codes 300938 --start 2026-01-01 --end 2026-04-13 --preset 保守验证C

# 参数扫描
bin/trade backtest sweep --start 2026-01-01 --end 2026-04-13 \
  --buy-thresholds 6.0,6.5,7.0 --stop-losses 0.05,0.08,0.10

#  Walk-Forward 回测
bin/trade backtest walk-forward --start 2026-01-01 --end 2026-04-13 --folds 3

# 策略健康报告
bin/trade backtest strategy-health --start 2026-01-01 --end 2026-04-13

# 一票否决分析
bin/trade backtest veto-analysis --start 2026-01-01 --end 2026-04-13

# 信号诊断
bin/trade backtest signal-diagnose --date 2026-04-11

# 回测历史
bin/trade backtest history --limit 10

# 对比历史回测
bin/trade backtest compare --limit 20

# 回撤分析
bin/trade backtest drawdown --code 300938 --days 365
bin/trade backtest drawdown --codes 300938,688123 --start 2026-01-01 --end 2026-04-13
```

---

## 七、状态管理

```bash
# 初始化状态文件
bin/trade state bootstrap

# 同步状态（从配置文件重建）
bin/trade state sync --target all

# 审计状态一致性
bin/trade state audit

# 状态对账（Paper 账本与结构化状态对比）
bin/trade state reconcile --window 180

# 查看委托单
bin/trade state orders --scope paper_mx --status pending --limit 20

# 确认委托（从 Discord 回复文本）
bin/trade state confirm --reply "止损触发了 艾比森 成交¥19.00" --scope paper_mx

# 推送待确认委托提醒
bin/trade state remind --scope paper_mx --send

# 查看池子操作历史
bin/trade state pool-actions --limit 50

# 查看交易复盘
bin/trade state trade-review --window 90

# 查看告警
bin/trade state alerts
```

---

## 八、订单管理

```bash
# 下条件单
bin/trade order place --code 300938 --name 信测标准 --side buy \
  --type dynamic_stop --price 18.50 --scope paper_mx --reason "回踩 MA20 买入"

bin/trade order place --code 300938 --name 信测标准 --side sell \
  --type take_profit_t1 --price 22.00 --scope paper_mx --reason "止盈 T1"

# 取消条件单
bin/trade order cancel --code 300938 --scope paper_mx

# 修改条件单价格
bin/trade order modify --code 300938 --price 18.00 --scope paper_mx

# 查询委托单列表
bin/trade order list --scope paper_mx --status pending --limit 20

# 检查逾期未确认委托
bin/trade order overdue-check --scope paper_mx --send
```

---

## 九、快速参考

| 场景 | 命令 |
|------|------|
| 盘前检查 | `bin/trade run morning` |
| 收盘评分 | `bin/trade run scoring` |
| 日常选股 | `bin/trade run screener --pool all --universe tracked` |
| 查看持仓 | `bin/trade status today` |
| 健康检查 | `bin/trade doctor` |
| 下单 | `bin/trade order place ...` |
| 查看委托 | `bin/trade state orders --scope paper_mx` |

---

> 返回索引：[[使用指南]]
