#!/usr/bin/env python3
"""
pipeline/hk_monitor.py — 港股遗留仓位自动监控

自动拉取港股遗留仓位的最新价格，检查止损条件，推送告警。

功能：
  - 从结构化账本读取 hk_legacy 持仓
  - 拉取最新港股价格（MX → akshare fallback）
  - 检查绝对止损（-15%）和反弹止损上调规则
  - 触发时推送 Discord 告警
  - 更新结构化账本中的价格

用法：
  python -m scripts.pipeline.hk_monitor           # 检查并推送
  python -m scripts.pipeline.hk_monitor --dry-run  # 只检查不推送
  bin/trade run hk_monitor --json
"""

import json
import os
import sys
from datetime import datetime

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from scripts.utils.logger import get_logger
from scripts.utils.discord_push import _post_embed_to_discord, _build_hk_alert_embed

_logger = get_logger("pipeline.hk_monitor")

# ---------------------------------------------------------------------------
# 港股遗留仓位规则（独立于 A 股系统）
# ---------------------------------------------------------------------------

HK_RULES = {
    "absolute_stop_pct": 0.15,       # -15% 绝对止损
    "rebound_threshold_pct": 0.05,   # 反弹 +5% 则止损价上调至成本价
    "no_add_position": True,         # 不补仓、不加仓
}


# ---------------------------------------------------------------------------
# 数据获取
# ---------------------------------------------------------------------------

def _fetch_hk_price(code: str) -> float | None:
    """获取港股最新价格，MX → akshare fallback"""
    # 尝试 MX
    try:
        from scripts.mx.cli_tools import dispatch_mx_command
        result = dispatch_mx_command("mx.data.query", query=f"{code} 港股最新价格")
        if isinstance(result, dict):
            content = str(result.get("content", result.get("data", result.get("text", ""))))
        elif isinstance(result, str):
            content = result
        else:
            content = ""
        # 尝试从返回内容中提取价格
        import re
        price_match = re.search(r'(?:现价|最新价|收盘价|价格)[^\d]*(\d+\.?\d*)', content)
        if price_match:
            return float(price_match.group(1))
        # 尝试直接匹配 HK$ 格式
        hk_match = re.search(r'HK\$?\s*(\d+\.?\d*)', content)
        if hk_match:
            return float(hk_match.group(1))
        # 尝试匹配纯数字（如果内容很短）
        if len(content) < 50:
            num_match = re.search(r'(\d+\.?\d+)', content)
            if num_match:
                return float(num_match.group(1))
    except Exception as exc:
        _logger.info(f"[hk] MX 获取 {code} 价格失败: {exc}")

    # 尝试 akshare
    try:
        import akshare as ak
        # 港股代码格式转换
        ak_code = code.replace(".HK", "").replace("HK", "").zfill(5)
        df = ak.stock_hk_spot_em()
        if df is not None and not df.empty:
            row = df[df["代码"] == ak_code]
            if not row.empty:
                return float(row.iloc[0].get("最新价", 0))
    except Exception as exc:
        _logger.info(f"[hk] akshare 获取 {code} 价格失败: {exc}")

    return None


# ---------------------------------------------------------------------------
# 核心逻辑
# ---------------------------------------------------------------------------

def run(dry_run: bool = False) -> dict:
    """
    检查港股遗留仓位

    Returns:
        {
            "status": "ok" | "warning" | "alert",
            "positions": [...],
            "alerts": [...],
            "discord_sent": int,
        }
    """
    started_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    # 从结构化账本读取 hk_legacy 持仓
    try:
        from scripts.state import load_portfolio_snapshot
        snapshot = load_portfolio_snapshot(scope="hk_legacy")
        positions = snapshot.get("positions", [])
    except Exception as exc:
        _logger.warning(f"[hk] 读取 hk_legacy 持仓失败: {exc}")
        # fallback: 从 portfolio.md 读取
        positions = _read_hk_from_markdown()

    if not positions:
        return {
            "status": "ok",
            "started_at": started_at,
            "positions": [],
            "alerts": [],
            "discord_sent": 0,
            "message": "no_hk_positions",
        }

    alerts = []
    discord_sent = 0
    updated_positions = []

    for pos in positions:
        code = str(pos.get("code", "")).strip()
        name = str(pos.get("name", "")).strip()
        shares = int(float(pos.get("shares", 0) or 0))
        cost = float(pos.get("avg_cost", 0) or 0)

        if not code or shares <= 0:
            continue

        # 拉取最新价格
        current_price = _fetch_hk_price(code)
        if current_price is None or current_price <= 0:
            _logger.warning(f"[hk] {name}({code}) 价格获取失败")
            updated_positions.append({**pos, "price_updated": False})
            continue

        pnl_pct = ((current_price / cost) - 1) if cost > 0 else 0
        absolute_stop_price = cost * (1 - HK_RULES["absolute_stop_pct"])
        rebound_price = cost * (1 + HK_RULES["rebound_threshold_pct"])

        position_info = {
            "code": code,
            "name": name,
            "shares": shares,
            "avg_cost": cost,
            "current_price": current_price,
            "pnl_pct": round(pnl_pct * 100, 2),
            "absolute_stop_price": round(absolute_stop_price, 2),
            "rebound_price": round(rebound_price, 2),
            "price_updated": True,
        }
        updated_positions.append(position_info)

        # 检查绝对止损
        if current_price <= absolute_stop_price:
            alert = {
                "type": "绝对止损触发",
                "code": code,
                "name": name,
                "level": "high",
                "current_price": current_price,
                "stop_price": absolute_stop_price,
                "pnl_pct": round(pnl_pct * 100, 2),
                "message": f"现价 HK${current_price:.2f} 已跌破绝对止损价 HK${absolute_stop_price:.2f}，建议立即清仓",
            }
            alerts.append(alert)

            if not dry_run:
                embeds = _build_hk_alert_embed(pos, current_price, "绝对止损触发", alert["message"])
                ok, err = _post_embed_to_discord(embeds)
                if ok:
                    discord_sent += 1
                else:
                    _logger.warning(f"[hk] Discord 推送失败: {err}")

        # 检查反弹止损上调
        elif current_price >= rebound_price:
            alert = {
                "type": "反弹止损上调",
                "code": code,
                "name": name,
                "level": "info",
                "current_price": current_price,
                "rebound_price": rebound_price,
                "new_stop_price": cost,
                "pnl_pct": round(pnl_pct * 100, 2),
                "message": f"现价 HK${current_price:.2f} 已反弹至 HK${rebound_price:.2f} 以上，止损价应上调至成本价 HK${cost:.2f}",
            }
            alerts.append(alert)

            if not dry_run:
                embeds = _build_hk_alert_embed(pos, current_price, "反弹止损上调提醒", alert["message"])
                ok, err = _post_embed_to_discord(embeds)
                if ok:
                    discord_sent += 1
                else:
                    _logger.warning(f"[hk] Discord 推送失败: {err}")

        # 更新结构化账本中的价格
        if not dry_run:
            try:
                from scripts.state.service import _connect, _now_ts, _today_str
                with _connect() as conn:
                    conn.execute(
                        """
                        UPDATE portfolio_positions
                        SET current_price = ?, market_value = ?,
                            as_of_date = ?, updated_at = ?
                        WHERE scope = 'hk_legacy' AND code = ?
                        """,
                        (
                            current_price,
                            round(current_price * shares, 2),
                            _today_str(),
                            _now_ts(),
                            code,
                        ),
                    )
            except Exception as exc:
                _logger.warning(f"[hk] 更新 {code} 价格到账本失败: {exc}")

    status = "ok"
    if any(a["level"] == "high" for a in alerts):
        status = "alert"
    elif alerts:
        status = "warning"

    return {
        "status": status,
        "started_at": started_at,
        "finished_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "positions": updated_positions,
        "alerts": alerts,
        "discord_sent": discord_sent,
        "dry_run": dry_run,
    }


def _read_hk_from_markdown() -> list[dict]:
    """从 portfolio.md 读取港股持仓（fallback）"""
    try:
        from scripts.utils.obsidian import ObsidianVault
        from scripts.utils.parser import parse_portfolio
        vault = ObsidianVault()
        portfolio_path = os.path.join(vault.vault_path, vault.portfolio_path)
        if not os.path.exists(portfolio_path):
            return []
        data = parse_portfolio(portfolio_path)
        hk_holdings = data.get("hk_legacy_holdings", [])
        positions = []
        for row in hk_holdings:
            code = str(row.get("代码", "")).strip()
            name = str(row.get("股票", "")).strip()
            shares_str = str(row.get("持有股数", "0")).replace(",", "")
            cost_str = str(row.get("平均成本", "0")).replace(",", "")
            try:
                shares = int(float(shares_str))
                cost = float(cost_str)
            except (ValueError, TypeError):
                continue
            if code and shares > 0:
                positions.append({
                    "code": code,
                    "name": name,
                    "shares": shares,
                    "avg_cost": cost,
                })
        return positions
    except Exception as exc:
        _logger.warning(f"[hk] 从 markdown 读取港股持仓失败: {exc}")
        return []


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    result = run(dry_run=dry)
    print(json.dumps(result, ensure_ascii=False, indent=2))
