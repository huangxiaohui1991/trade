#!/bin/bash
# trading-agent-launcher.sh - 根据时间自动选择任务模式
# 由 launchd 调用，调用 trading-agent.py（负责生成报告+发Discord）

set -e

SCRIPT_DIR="/Users/hxh/Documents/a-stock-trading/scripts"
PYTHON="/Users/hxh/.venv/akshare/bin/python"

NOW=$(date '+%H%M')
DAY=$(date '+%u')

if [[ "$NOW" == "0830" ]]; then
    MODE="morning"
elif [[ "$NOW" == "1200" ]]; then
    MODE="noon"
elif [[ "$NOW" == "1530" ]]; then
    MODE="evening"
elif [[ "$NOW" == "1600" ]] && [[ "$DAY" == "7" ]]; then
    MODE="weekly"
elif [[ "$NOW" == "0900" ]] && [[ $(date '+%d') == "01" ]]; then
    MODE="monthly"
else
    echo "未匹配到任务时间窗口，跳过 ($(date '+%H%M'))"
    exit 0
fi

echo "=== $(date '+%Y-%m-%d %H:%M:%S') Running: $MODE ==="

cd "$SCRIPT_DIR"

# DISCORD_BOT_TOKEN 由 launchd plist 的 EnvironmentVariables 传入，勿在此处硬编码
DISCORD_CHANNEL_ID="1478608178621976617" \
PYTHONPATH="/Users/hxh/.venv/akshare/lib/python3.9/site-packages" \
PATH="/Users/hxh/.local/bin:$PATH" \
"$PYTHON" trading-agent.py "$MODE" 2>&1
