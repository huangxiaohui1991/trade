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
from scripts.state import load_market_snapshot, load_portfolio_snapshot
from scripts.utils.obsidian import ObsidianVault
from scripts.utils.discord_push import send_noon_check
from scripts.utils.logger import get_logger
from scripts.utils.runtime_state import update_pipeline_state

_logger = get_logger("pipeline.noon")


def run() -> dict:
    """执行午休检查"""
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    weekday = weekday_names[datetime.now().weekday()]
    today_str = datetime.now().strftime("%Y-%m-%d")

    _logger.info(f"[NOON] 午休检查 {today_str} ({weekday})")
    try:
        engine = DataEngine()

        _logger.info(">> 大盘数据")
        market_snapshot = load_market_snapshot()
        indices = market_snapshot.get("indices") or market_snapshot.get("market") or {}
        market_info = {}
        for name, info in indices.items():
            if not isinstance(info, dict):
                _logger.warning(f"  {name}: 数据不可用")
                continue
            if info.get("error"):
                _logger.warning(f"  {name}: {info.get('error', '数据不可用')}")
                continue
            price = info.get("close", info.get("price", 0))
            chg_pct = info.get("change_pct", info.get("chg_pct", 0))
            market_info[name] = {
                "price": price,
                "chg_pct": chg_pct,
                "high": info.get("high", 0),
                "low": info.get("low", 0),
                "signal": info.get("signal", ""),
            }
            high = market_info[name]["high"]
            low = market_info[name]["low"]
            _logger.info(f"  {name}: {price} ({chg_pct:+.2f}%) 区间:{low}～{high}")

        _logger.info(">> 持仓状态")
        active = load_portfolio_snapshot(scope="cn_a_system").get("positions", [])

        position_list = []
        tips = []
        if active:
            codes = [str(h.get("code", "")).strip() for h in active]
            rt = engine.get_realtime(codes)
            for h in active:
                code = str(h.get("code", "")).strip()
                name = str(h.get("name", "")).strip()
                cost = float(h.get("avg_cost", 0) or 0)
                shares = int(float(h.get("shares", 0) or 0))
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

                if pnl_pct < -3:
                    tips.append(f"{name}下跌{pnl_pct:.1f}%，注意止损")
                elif pnl_pct > 10:
                    tips.append(f"{name}已涨{pnl_pct:.1f}%，关注止盈机会")

            _logger.info(f"  持仓{len(active)}只")
        else:
            _logger.info("  空仓")

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

        try:
            from scripts.pipeline.shadow_trade import check_stop_signals
            shadow_results = check_stop_signals()
            triggered = [r for r in shadow_results if r.get("action") != "持有"]
            advisories = [
                r for r in shadow_results
                if r.get("advisory_signals") and r.get("action") == "持有"
            ]
            if triggered:
                _logger.info(f">> 影子交易(午休): {len(triggered)} 只触发信号")
                for r in triggered:
                    _logger.info(f"  {r['name']}({r['code']}): {r['action']} — {r['reason']}")
            if advisories:
                _logger.info(f">> 影子交易(午休) advisory: {len(advisories)} 只")
                for r in advisories:
                    _logger.info(f"  {r['name']}({r['code']}): {r.get('advisory_summary', '')}")
        except Exception as e:
            _logger.warning(f">> 影子交易检查失败: {e}")

        update_pipeline_state(
            "noon",
            "warning" if not ok else "success",
            {
                "positions_count": len(position_list),
                "tips_count": len(tips),
                "discord_ok": ok,
                "discord_error": err,
                "market_signal": market_snapshot.get("signal", market_snapshot.get("market_signal", "")),
            },
            today_str,
        )

        return discord_data
    except Exception as e:
        update_pipeline_state(
            "noon",
            "error",
            {"error": str(e)},
            today_str,
        )
        raise


if __name__ == "__main__":
    result = run()
    print("\n午休检查已推送 Discord")
