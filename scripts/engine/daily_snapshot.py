#!/usr/bin/env python3
"""
engine/daily_snapshot.py — 每日持仓快照归档

职责：
  - snapshot()：每日收盘后归档当日账户状态
    - 大盘指数（收盘价、涨跌幅、MA20/MA60状态）
    - 持仓明细（代码、名称、成本、现价、盈亏、止损价、止盈价）
    - 账户总览（总市值、总盈亏、可用资金）
    - 核心池评分摘要
  - 存储：data/每日快照/YYYY-MM-DD.md
  - 用于回溯分析，无需每次从 akshare 拉历史

用法：
  from scripts.engine.daily_snapshot import snapshot, get_today_snapshot
  snapshot()
"""

import os
import sys
import warnings
from datetime import datetime, date
from pathlib import Path
from typing import Optional

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

from scripts.engine.data_engine import DataEngine
from scripts.engine.market_timer import MarketTimer
from scripts.utils.obsidian import ObsidianVault
from scripts.utils.logger import get_logger

_logger = get_logger("daily_snapshot")

SNAPSHOT_DIR = Path(_PROJECT_ROOT) / "data" / "每日快照"
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)


def _get_snapshot_path(dt: Optional[date] = None) -> Path:
    if dt is None:
        dt = date.today()
    return SNAPSHOT_DIR / f"{dt.isoformat()}.md"


def get_portfolio_data(engine: DataEngine) -> dict:
    """
    读取 portfolio.md，返回持仓数据
    """
    vault = ObsidianVault()
    portfolio_path = Path(vault.vault_path) / vault.portfolio_overview_path
    if not portfolio_path.exists():
        return {}

    content = portfolio_path.read_text(encoding="utf-8")

    # 简单解析持仓表格
    holdings = []
    lines = content.split("\n")
    in_table = False
    for line in lines:
        if "| # | 股票 | 代码 |" in line:
            in_table = True
            continue
        if in_table and line.startswith("|"):
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 5 and parts[2].isdigit():
                holdings.append({
                    "name": parts[3],
                    "code": parts[2],
                    "shares": int(parts[4]),
                })
        elif in_table and not line.startswith("|"):
            break

    if not holdings:
        return {}

    # 批量拉取现价
    codes = [h["code"] for h in holdings]
    prices = engine.batch_realtime(codes)

    # 合并数据
    total_value = 0
    total_cost = 0
    today_pnl = 0

    for h in holdings:
        code = h["code"]
        price_info = prices.get(code, {})
        current_price = price_info.get("current", 0)
        chg_pct = price_info.get("chg_pct", 0)

        # 从 content 解析成本（简化：依赖持仓概览格式）
        # 实际应用中由 calculator.py 更新持仓概览，此处只读取现价
        cost_estimated = 0  # 快照不记录成本，用持仓概览的历史数据

        value = current_price * h["shares"]
        h["current_price"] = current_price
        h["chg_pct"] = chg_pct
        h["value"] = value
        total_value += value

    return {
        "holdings": holdings,
        "total_value": total_value,
        "record_date": date.today().isoformat(),
    }


def snapshot(force: bool = False) -> dict:
    """
    生成当日快照

    Args:
        force: 如果快照已存在，是否覆盖

    Returns:
        {"success": bool, "path": str, "summary": str}
    """
    today = date.today()
    snap_path = _get_snapshot_path(today)
    vault = ObsidianVault()

    if snap_path.exists() and not force:
        return {"success": False, "path": str(snap_path),
                "message": f"今日快照已存在: {snap_path}"}

    engine = DataEngine()
    market = MarketTimer()
    market_detail = market.get_detail()
    market_signal = market.get_signal()

    # 大盘数据
    market_lines = ["| 指数 | 收盘价 | 涨跌 | MA20 | MA60 | 信号 |",
                    "|------|--------|------|------|------|------|"]
    for name, data in market_detail.get("indices", {}).items():
        sig = data.get("signal", "N/A")
        ma20_ok = "✅" if data.get("above_ma20") else "❌"
        ma60_ok = "✅" if data.get("above_ma60") else "❌"
        chg = data.get("chg_pct", 0)
        market_lines.append(
            f"| {name} | {data.get('price', 'N/A')} | "
            f"{chg:+.2f}% | {ma20_ok} | {ma60_ok} | {sig} |"
        )
    market_md = "\n".join(market_lines)

    # 持仓数据
    portfolio = get_portfolio_data(engine)
    holdings = portfolio.get("holdings", [])

    if holdings:
        pos_lines = ["| 股票 | 代码 | 股数 | 现价 | 涨跌幅 | 市值 |",
                     "|------|------|------|------|--------|------|"]
        for h in holdings:
            pos_lines.append(
                f"| {h['name']} | {h['code']} | {h['shares']} | "
                f"{h.get('current_price', 'N/A')} | "
                f"{h.get('chg_pct', 0):+.2f}% | "
                f"{h.get('value', 0):.0f} |"
            )
        pos_lines.append("")
        pos_lines.append(f"**账户总市值：{portfolio.get('total_value', 0):.0f} 元**")
    else:
        pos_lines = ["*今日无持仓*"]

    portfolio_md = "\n".join(pos_lines)

    # 组装快照
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    content = f"""---
date: {today.isoformat()}
type: daily_snapshot
tags: [每日快照, 自动归档]
---

# 每日快照 — {today.isoformat()}

> 生成时间: {now}
> 大盘信号: **{market_signal}**

## 大盘状态

{market_md}

## 持仓状态

{portfolio_md}

## 核心池评分（最近）

> 核心池评分报告路径: `{vault.screening_results_dir}/`

## 备注

- 本文件为自动归档，用于回溯分析
- 如需修改持仓成本，请手动编辑 `{vault.portfolio_overview_path}`
"""

    snap_path.write_text(content, encoding="utf-8")
    _logger.info(f"快照已保存: {snap_path}")

    return {
        "success": True,
        "path": str(snap_path),
        "market_signal": market_signal,
        "holdings_count": len(holdings),
        "total_value": portfolio.get("total_value", 0),
    }


def get_today_snapshot() -> Optional[dict]:
    """读取今日快照"""
    snap_path = _get_snapshot_path()
    if not snap_path.exists():
        return None
    content = snap_path.read_text(encoding="utf-8")
    return {"path": str(snap_path), "content": content}


def list_snapshots(months: int = 3) -> list:
    """列出近 N 个月的快照"""
    snaps = sorted(SNAPSHOT_DIR.glob("*.md"), reverse=True)
    cutoff = datetime.now() - timedelta(days=months * 30)
    return [str(s) for s in snaps
            if datetime.fromtimestamp(s.stat().st_mtime) > cutoff]


if __name__ == "__main__":
    import json
    result = snapshot()
    print(json.dumps(result, ensure_ascii=False, indent=2))
