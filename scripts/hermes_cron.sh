#!/bin/bash
# hermes_cron.sh - Hermes 原生架构 cron 调度脚本
#
# 根据触发时间自动选择对应的 pipeline 模块执行。
# 由 Hermes cron 守护进程调用。
#
# 触发时间（与 ARCHITECTURE.md §五 对齐）：
#   08:25  → 盘前摘要（周一~五）
#   11:55  → 午休检查（周一~五）
#   15:35  → 收盘报告（周一~五）
#   15:40  → 核心池评分（周一~五）
#   20:00  → 周报（周日）
#   */30 * → 舆情监控（每30分钟，由 Hermes TrendRadar 触发）
#
# 环境变量（由 launchd 或 Hermes 注入）：
#   DISCORD_WEBHOOK_URL  - Discord webhook URL
#   AStockVault         - vault 路径（默认 ~/Documents/a-stock-trading）
#
# 用法（Hermes 调度）：
#   hermes_cron.sh morning
#   hermes_cron.sh noon
#   hermes_cron.sh evening
#   hermes_cron.sh scoring
#   hermes_cron.sh weekly
#   hermes_cron.sh sentiment

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VAULT_PATH="${AStockVault:-$REPO_ROOT}"
PYTHON="${HOME}/.venv/akshare/bin/python"

# 确保 vault 的 scripts 在 PYTHONPATH
export PYTHONPATH="${VAULT_PATH}:${HOME}/.venv/akshare/lib/python3.9/site-packages"
export PATH="${HOME}/.local/bin:$PATH"
export AStockVault="$VAULT_PATH"

# Discord 环境变量由 launchd 注入，此处不硬编码
# export DISCORD_WEBHOOK_URL  ← 由 Hermes/launchd 运行时注入

MODE="${1:-}"

if [[ -z "$MODE" ]]; then
    echo "用法: hermes_cron.sh [morning|noon|evening|scoring|weekly|sentiment]"
    exit 1
fi

echo "=== [Hermes Cron] $(date '+%Y-%m-%d %H:%M:%S') MODE=$MODE ==="

cd "$SCRIPT_DIR"

case "$MODE" in
    morning)
        echo ">> 盘前流程（8:25）"
        "$PYTHON" -m scripts.pipeline.morning 2>&1
        ;;
    noon)
        echo ">> 午休检查（11:55）"
        "$PYTHON" -m scripts.pipeline.noon 2>&1
        ;;
    evening)
        echo ">> 收盘流程（15:35）"
        "$PYTHON" -m scripts.pipeline.evening 2>&1
        ;;
    scoring)
        echo ">> 核心池评分（15:40）"
        "$PYTHON" -m scripts.pipeline.core_pool_scoring 2>&1
        ;;
    weekly)
        echo ">> 周报（周日20:00）"
        "$PYTHON" -m scripts.pipeline.weekly_review 2>&1
        ;;
    sentiment)
        echo ">> 舆情监控（TrendRadar）"
        # TODO: 调用 TrendRadar API，匹配关键词后推送
        # 目前为 placeholder，后续接入 TrendRadar MCP
        echo "[sentiment] Placeholder - TrendRadar 待接入"
        ;;
    *)
        echo "未知模式: $MODE"
        exit 1
        ;;
esac

echo "=== [Hermes Cron] 完成 $(date '+%Y-%m-%d %H:%M:%S') ==="
