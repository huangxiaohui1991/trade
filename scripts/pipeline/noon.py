#!/usr/bin/env python3
"""
pipeline/noon.py — 午休检查（11:55 执行）

职责：
  1. 获取上午大盘行情
  2. 检查持仓涨跌和条件单状态
  3. 检查是否有加仓机会
  4. Discord 推送午休检查

用法（CLI）：
  python -m scripts.pipeline.noon

用法（导入）：
  from scripts.pipeline.noon import run
  result = run()
"""

import os
import sys
import warnings
from datetime import datetime

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

from scripts.engine.data_engine import DataEngine
from scripts.utils.obsidian import ObsidianVault
from scripts.utils.discord_push import send_noon_check
from scripts.utils.logger import get_logger

_logger = get_logger("pipeline.noon")


def run() -> dict:
    """执行午休检查"""
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    weekday = weekday_names[datetime.now().weekday()]
    today_str = datetime.now().strftime("%Y-%m-%d")

    _logger.info(f"[NOON] 午休检查 {today_str} ({weekday})")

    vault = ObsidianVault()
    engine = DataEngine()

    # 1. 大盘数据
    _logger.info(">> 大盘数据")
    indices = {"上证指数": "000001", "创业板指": "399006"}
    market_info = {}
    for name, code in indices.items():
        rt = engine.get_realtime([code])
        stock_data = rt.get("data", {}).get(code, {})
        if stock_data:
            price = stock_data.get("price", 0)
            chg_pct = stock_data.get("change_pct", 0)
            high = stock_data.get("high", 0)
            low = stock_data.get("low", 0)
            market_info[name] = {
                "price": price,
                "chg_pct": chg_pct,
                "high": high,
                "low": low,
            }
            _logger.info(f"  {name}: {price} ({chg_pct:+.2f}%) 区间:{low}～{high}")

    # 2. 持仓状态
    _logger.info(">> 持仓状态")
    portfolio = vault.read_portfolio()
    holdings = portfolio.get("holdings", [])
    active = [
        h for h in holdings
        if str(h.get("股票", "")).strip() not in ["", "—", "空仓"]
        and int(float(h.get("持有股数", 0) or 0)) > 0
    ]

    position_list = []
    tips = []
    if active:
        codes = [str(h.get("代码", "")).strip() for h in active]
        rt = engine.get_realtime(codes)
        for h in active:
            code = str(h.get("代码", "")).strip()
            name = str(h.get("股票", "")).strip()
            cost = float(h.get("平均成本", 0) or 0)
            shares = int(float(h.get("持有股数", 0) or 0))
            stock_data = rt.get("data", {}).get(code, {})
            price = stock_data.get("price", cost)
            pnl_pct = ((price / cost) - 1) * 100 if cost > 0 else 0

            position_list.append({
                "name": name,
                "shares": shares,
                "cost": cost,
                "price": price,
                "pnl_pct": pnl_pct,
                "currency": "¥",
            })

            # 加仓机会判断
            if pnl_pct < -3:
                tips.append(f"{name}下跌{pnl_pct:.1f}%，注意止损")
            elif pnl_pct > 10:
                tips.append(f"{name}已涨{pnl_pct:.1f}%，关注止盈机会")

        _logger.info(f"  持仓{len(active)}只")
    else:
        _logger.info("  空仓")

    # 3. 推送 Discord
    discord_data = {
        "date": today_str,
        "weekday": weekday,
        "market": market_info,
        "positions": position_list,
        "tips": tips,
    }

    ok, err = send_noon_check(discord_data)
    if ok:
        _logger.info(">> Discord 推送成功")
    else:
        _logger.warning(f">> Discord 推送失败: {err}")

    _logger.info("[NOON] 午休检查完成")

    # 4. 影子交易：盘中检查止损止盈（午休是最佳执行时机）
    try:
        from scripts.pipeline.shadow_trade import check_stop_signals
        shadow_results = check_stop_signals()
        triggered = [r for r in shadow_results if r.get("action") != "持有"]
        if triggered:
            _logger.info(f">> 影子交易(午休): {len(triggered)} 只触发信号")
            for r in triggered:
                _logger.info(f"  {r['name']}({r['code']}): {r['action']} — {r['reason']}")
    except Exception as e:
        _logger.warning(f">> 影子交易检查失败: {e}")

    return discord_data


if __name__ == "__main__":
    result = run()
    print("\n午休检查已推送 Discord")
