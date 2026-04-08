#!/bin/bash
# trading-cron.sh - A股交易系统定时任务入口
# 用法: trading-cron.sh [morning|noon|evening|weekly|monthly]

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="$HOME/.venv/akshare/bin/python"
cd "$SCRIPT_DIR"

MODE="${1:-evening}"
PYTHONPATH="$HOME/.venv/akshare/lib/python3.9/site-packages" \
  PATH="$HOME/.local/bin:$PATH" \
  "$PYTHON" daily_routine.py "$MODE" 2>&1
