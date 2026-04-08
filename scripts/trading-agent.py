#!/usr/bin/env python3
"""
A股交易定时任务 - 结果推送
通过 Discord Bot API 发送任务结果

环境变量:
  DISCORD_BOT_TOKEN - Discord bot token
  DISCORD_CHANNEL_ID - Discord channel ID
"""

import os
import sys
import json
import subprocess
import urllib.request
from datetime import datetime
from pathlib import Path

# 路径配置
SCRIPT_DIR = Path("/Users/hxh/Documents/a-stock-trading/scripts")
PYTHON = os.path.expanduser("~/.venv/akshare/bin/python")

DISCORD_TOKEN = os.environ.get("DISCORD_BOT_TOKEN")
if not DISCORD_TOKEN:
    raise RuntimeError("DISCORD_BOT_TOKEN environment variable not set")
DISCORD_CHANNEL = os.environ.get("DISCORD_CHANNEL_ID", "1478608178621976617")


def send_discord(content: str) -> bool:
    """发送消息到 Discord"""
    url = f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL}/messages"
    payload = json.dumps({"content": content}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={
        "Authorization": f"Bot {DISCORD_TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "WangCaiBot/1.0 (Trading System)"
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status in (200, 201)
    except urllib.error.HTTPError as e:
        print(f"Discord API error: {e.code} - {e.read().decode()[:200]}")
        return False
    except Exception as e:
        print(f"Failed to send: {e}")
        return False


def run_task(mode: str) -> str:
    """执行 format_report.py 生成 Markdown 报告"""
    try:
        result = subprocess.run(
            [PYTHON, f"{SCRIPT_DIR}/format_report.py", mode],
            capture_output=True, text=True, timeout=60, cwd=SCRIPT_DIR
        )
        return result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return "⏰ 任务执行超时（60秒）"
    except FileNotFoundError:
        return f"❌ Python 解释器未找到: {PYTHON}"
    except Exception as e:
        return f"❌ 执行失败: {e}"


def main():
    if len(sys.argv) < 2:
        print("用法: trading-agent.py <morning|noon|evening|weekly|monthly>")
        sys.exit(1)

    mode = sys.argv[1]
    now = datetime.now()

    titles = {
        "morning": "【每日】盘前检核",
        "noon": "【每日】午间复盘",
        "evening": "【每日】盘后总结",
        "weekly": "【每周】周复盘",
        "monthly": "【每月】月复盘"
    }
    emojis = {
        "morning": "🌅",
        "noon": "☀️",
        "evening": "🌙",
        "weekly": "📊",
        "monthly": "📈"
    }

    title = titles.get(mode, f"交易任务[{mode}]")
    emoji = emojis.get(mode, "📋")
    timestamp = now.strftime("%Y-%m-%d %H:%M")

    print(f"🚀 {timestamp} - {title}")

    # 执行脚本
    output = run_task(mode)
    print(f"脚本输出长度: {len(output)}")

    # 截断（Discord 2000字符限制）
    if len(output) > 1800:
        output = output[:1800] + "\n\n...（内容过长已截断）"

    # 报告直接就是 Markdown，直接发送
    message = output.strip()

    if send_discord(message):
        print("✅ 结果已推送到 Discord")
    else:
        print("⚠️ Discord 推送失败，结果仅打印到日志")
        print(output)


if __name__ == "__main__":
    main()
