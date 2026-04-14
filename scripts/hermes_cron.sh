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
#   AStockVault         - vault 路径（默认 repo/trade-vault，缺失时回退仓库根目录）
#
# 用法（Hermes 调度）：
#   hermes_cron.sh morning
#   hermes_cron.sh noon
#   hermes_cron.sh evening
#   hermes_cron.sh scoring
#   hermes_cron.sh weekly
#   hermes_cron.sh sentiment
#   hermes_cron.sh hk_monitor
#   hermes_cron.sh monthly

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEFAULT_VAULT_PATH="$REPO_ROOT/trade-vault"
if [[ ! -d "$DEFAULT_VAULT_PATH" ]]; then
    DEFAULT_VAULT_PATH="$REPO_ROOT"
fi
# Always point to trade-vault; external AStockVault may be repo-root, ignore it
VAULT_PATH="$DEFAULT_VAULT_PATH"
PYTHON="$REPO_ROOT/.venv/bin/python"

# PYTHONPATH: REPO_ROOT for scripts module, VAULT_PATH for vault data
export PYTHONPATH="${REPO_ROOT}:${VAULT_PATH}${PYTHONPATH:+:$PYTHONPATH}"
export PATH="${HOME}/.local/bin:$PATH"
export AStockVault="$VAULT_PATH"

if [[ ! -x "$PYTHON" ]]; then
    echo "错误：未找到项目虚拟环境 ($PYTHON)"
    echo "请先运行：pyenv exec python -m venv .venv && .venv/bin/pip install -r requirements.txt"
    exit 1
fi

# 加载项目 .env 文件（MX_APIKEY 等）
if [[ -f "$REPO_ROOT/.env" ]]; then
    while IFS='=' read -r key value; do
        key=$(echo "$key" | xargs)
        [[ -z "$key" || "$key" == \#* ]] && continue
        value=$(echo "$value" | xargs)
        export "$key=$value"
    done < "$REPO_ROOT/.env"
fi

# Discord 环境变量由 launchd 注入，此处不硬编码
# export DISCORD_WEBHOOK_URL  ← 由 Hermes/launchd 运行时注入

MODE="${1:-}"

if [[ -z "$MODE" ]]; then
    echo "用法: hermes_cron.sh [morning|noon|evening|scoring|weekly|sentiment|hk_monitor|monthly]"
    exit 1
fi

echo "=== [Hermes Cron] $(date '+%Y-%m-%d %H:%M:%S') MODE=$MODE ==="

cd "$SCRIPT_DIR"

record_non_trading_skip() {
    local pipeline="$1"
    "$PYTHON" - "$pipeline" "$MODE" <<'PY'
import sys

from scripts.utils.runtime_state import update_pipeline_state

pipeline = sys.argv[1]
mode = sys.argv[2]
update_pipeline_state(
    pipeline,
    "skipped",
    {
        "reason": "non_trading_day",
        "skipped_reason": "non_trading_day",
        "mode": mode,
        "source": "hermes_cron",
    },
)
PY
}

is_trading_day_gated_mode() {
    case "$1" in
        morning|noon|evening|scoring|sentiment)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# 交易日历检查（仅 A 股日内 pipeline 受交易日限制）
if is_trading_day_gated_mode "$MODE"; then
    IS_TRADING=$("$PYTHON" -c "from scripts.utils.trading_calendar import is_trading_day; print('yes' if is_trading_day() else 'no')" 2>/dev/null || echo "yes")
    if [[ "$IS_TRADING" == "no" ]]; then
        echo ">> 今日非交易日，跳过 $MODE"
        record_non_trading_skip "$MODE"
        exit 0
    fi
fi

echo ">> 同步结构化状态"
SYNC_JSON=$("$PYTHON" -m scripts.cli.trade state sync --target all --json)
echo "$SYNC_JSON"

SYNC_STATUS=$(/usr/bin/python3 -c 'import json,sys; print(json.loads(sys.argv[1]).get("status","error"))' "$SYNC_JSON")
if [[ "$SYNC_STATUS" == "error" ]]; then
    echo ">> state sync 失败，阻断执行"
    exit 2
fi

echo ">> 运行 doctor 检查"
DOCTOR_JSON=$("$PYTHON" -m scripts.cli.trade doctor --json)
echo "$DOCTOR_JSON"

DOCTOR_STATUS=$(/usr/bin/python3 -c 'import json,sys; print(json.loads(sys.argv[1]).get("status","error"))' "$DOCTOR_JSON")

if [[ "$DOCTOR_STATUS" == "error" ]]; then
    echo ">> doctor 失败，阻断执行"
    exit 2
fi

case "$MODE" in
    morning)
        echo ">> 盘前流程（8:25）"
        "$PYTHON" -m scripts.cli.trade orchestrate morning_brief --json 2>&1
        ;;
    noon)
        echo ">> 午休检查（11:55）"
        "$PYTHON" -m scripts.cli.trade orchestrate noon_check --json 2>&1
        ;;
    evening)
        echo ">> 收盘流程（15:35）"
        "$PYTHON" -m scripts.cli.trade orchestrate close_review --json 2>&1
        ;;
    scoring)
        echo ">> 核心池评分（15:40）"
        "$PYTHON" -m scripts.cli.trade run scoring --json 2>&1
        ;;
    weekly)
        echo ">> 周报（周日20:00）"
        "$PYTHON" -m scripts.cli.trade orchestrate weekly_review --json 2>&1
        ;;
    sentiment)
        echo ">> 舆情监控"
        "$PYTHON" -m scripts.cli.trade run sentiment 2>&1
        ;;
    hk_monitor)
        echo ">> 港股遗留仓位检查"
        "$PYTHON" -m scripts.cli.trade run hk_monitor --json 2>&1
        ;;
    monthly)
        echo ">> 月度复盘"
        "$PYTHON" -m scripts.cli.trade run monthly --json 2>&1
        ;;
    *)
        echo "未知模式: $MODE"
        exit 1
        ;;
esac

echo "=== [Hermes Cron] 完成 $(date '+%Y-%m-%d %H:%M:%S') ==="
