# trade

当前仓库采用“同仓分目录”结构：

- `scripts/`、`config/`、`data/` 放代码、配置和运行数据
- `trade-vault/` 放 Obsidian 内容区
- Python 运行环境统一使用 `.venv/`

常用约定：

- 默认 vault 路径由 [config/paths.yaml](/Users/hxh/Documents/a-stock-trading/config/paths.yaml:1) 指向 `trade-vault/`
- 如需临时覆盖，可设置环境变量 `AStockVault`
- 运行自检可用：`python -m scripts.cli.trade doctor --json`

`trade-vault/` 结构示例：

- `00-系统`：仪表盘、使用指南、模板
- `01-状态`：持仓、账户、池子
- `02-运行`：日志、模拟盘、信号快照、当日输出
- `03-分析`：周复盘、月复盘、专题分析、策略体检
- `04-决策`：今日决策、候选池、筛选结果、个股解释
