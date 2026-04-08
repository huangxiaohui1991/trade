#!/bin/bash
# send-discord.sh - 发送消息到 Discord
# 用法: send-discord.sh "消息内容"

DISCORD_WEBHOOK_URL="${DISCORD_WEBHOOK_URL:-}"

if [ -z "$DISCORD_WEBHOOK_URL" ]; then
    # 如果没有 webhook，用 Discord API 直接发送
    CHANNEL_ID="1478608178621976617"  # 需要替换
    BOT_TOKEN="$BOT_TOKEN"
    if [ -n "$BOT_TOKEN" ] && [ -n "$CHANNEL_ID" ]; then
        curl -s -X POST \
            -H "Authorization: Bot $BOT_TOKEN" \
            -H "Content-Type: application/json" \
            -d "{\"content\":\"$1\"}" \
            "https://discord.com/api/v10/channels/$CHANNEL_ID/messages" 2>/dev/null
    else
        echo "Discord webhook URL 或 Bot token 未设置"
    fi
else
    curl -s -X POST \
        -H "Content-Type: application/json" \
        -d "{\"content\":\"$1\"}" \
        "$DISCORD_WEBHOOK_URL" 2>/dev/null
fi
