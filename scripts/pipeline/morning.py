#!/usr/bin/env python3
"""
pipeline/morning.py — 盘前流程（8:25 执行）

职责：
  1. 拉大盘实时数据（vs MA20/MA60）
  2. 读 portfolio.md 报告持仓状态
  3. 查核心池异动（跌破 MA20 / 主力大幅流出）
  4. 格式化 → Discord 盘前摘要推送

用法（CLI）：
  python -m scripts.pipeline.morning

用法（导入）：
  from scripts.pipeline.morning import run
  run()  # 返回 dict 包含 market_data, positions, core_pool
"""

import os
import sys
import warnings
from datetime import datetime

# 确保项目根目录在 path
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

import pandas as pd
from scripts.engine.data_engine import DataEngine
from scripts.utils.obsidian import ObsidianVault
from scripts.utils.discord_push import send_morning_summary
from scripts.utils.config_loader import get_strategy
from scripts.utils.logger import get_logger

_logger = get_logger("pipeline.morning")


# ---------------------------------------------------------------------------
# 大盘指数列表（用于趋势判断）
# ---------------------------------------------------------------------------
_INDEX_CODES = {
    "上证指数": "000001",
    "深证成指": "399001",
    "创业板指": "399006",
    "科创50": "000688",
}


def _get_market_data(engine: DataEngine) -> dict:
    """
    获取大盘数据，计算 MA20/MA60 状态

    Returns:
        {
            "market": {名称: {price, chg_pct, ma20_pct, ma60_pct, ma60_days, signal}},
            "market_signal": str  # GREEN/YELLOW/RED/CLEAR
        }
    """
    result = {"market": {}, "market_signal": ""}

    for name, code in _INDEX_CODES.items():
        tech = engine.get_technical(code, 60)
        if "error" in tech:
            _logger.warning(f"[market] {name} 数据获取失败: {tech['error']}")
            continue

        price = tech.get("current_price", 0)
        ma20 = tech.get("ma", {}).get("MA20", 0)
        ma60 = tech.get("ma", {}).get("MA60", 0)
        chg_pct = tech.get("change_pct", 0)

        ma20_pct = ((price / ma20) - 1) * 100 if ma20 else 0
        ma60_pct = ((price / ma60) - 1) * 100 if ma60 else 0
        above_ma20 = price >= ma20 if ma20 else False

        # MA60 下方天数（简化：用连续跌破判断）
        ma60_days = 0
        hist = tech.get("hist", pd.DataFrame())
        if not hist.empty and ma60:
            below_days = 0
            for _, row in hist.iterrows():
                if row.get("close", 0) < ma60:
                    below_days += 1
                else:
                    break
            ma60_days = below_days

        result["market"][name] = {
            "price": price,
            "chg_pct": chg_pct,
            "ma20": ma20,
            "ma60": ma60,
            "ma20_pct": ma20_pct,
            "ma60_pct": ma60_pct,
            "ma60_days": ma60_days,
            "above_ma20": above_ma20,
            "signal": "GREEN" if above_ma20 else "RED",
        }

    # 综合信号
    green_count = sum(1 for v in result["market"].values() if v.get("above_ma20"))
    total = len(result["market"])
    if total == 0:
        result["market_signal"] = "CLEAR"
    elif green_count >= total * 0.6:
        result["market_signal"] = "GREEN"
    elif green_count >= total * 0.3:
        result["market_signal"] = "YELLOW"
    else:
        result["market_signal"] = "RED"

    return result


def _get_portfolio_positions(vault: ObsidianVault) -> list:
    """从 portfolio.md 读取有效持仓"""
    positions = []
    try:
        portfolio = vault.read_portfolio()
        holdings = portfolio.get("holdings", [])
        for h in holdings:
            code = str(h.get("代码", "")).strip()
            name = str(h.get("股票", "")).strip()
            shares = h.get("持有股数", 0)
            price = h.get("最新价", h.get("平均成本", 0))
            note = h.get("备注", "")
            if code and code not in ["", "—"] and name not in ["", "—", "空仓"]:
                try:
                    shares = int(float(shares)) if shares else 0
                    price = float(price) if price else 0
                except (ValueError, TypeError):
                    continue
                if shares > 0:
                    positions.append({
                        "name": name,
                        "code": code,
                        "shares": shares,
                        "price": price,
                        "note": note,
                    })
    except Exception as e:
        _logger.warning(f"[portfolio] 读取失败: {e}")
    return positions


def _get_core_pool_status(vault: ObsidianVault, engine: DataEngine) -> list:
    """检查核心池异动（跌破 MA20 / 主力流出）"""
    core_items = []
    try:
        core_pool = vault.read_core_pool()
        for item in core_pool:
            code = str(item.get("代码", "")).strip()
            name = str(item.get("股票", "")).strip()
            if not code or code in ["", "—"]:
                continue
            try:
                tech = engine.get_technical(code, 20)
                flow = engine.get_fund_flow(code)
            except Exception:
                core_items.append({"name": name, "code": code, "status": "数据获取失败", "score": 0})
                continue

            if "error" in tech:
                core_items.append({"name": name, "code": code, "status": f"技术面: {tech['error']}", "score": 0})
                continue

            price = tech.get("current_price", 0)
            ma20 = tech.get("ma", {}).get("MA20", 0)
            above_ma20 = price >= ma20 if ma20 else False
            score = float(item.get("四维总分", item.get("总分", 0)))

            if not above_ma20:
                status = "跌破MA20，需观察"
            elif flow.get("main_outflow", False):
                status = "主力流出"
            else:
                status = "正常"

            # 获取评分
            if score == 0:
                score = tech.get("overall_score", 0)

            core_items.append({
                "name": name,
                "code": code,
                "price": price,
                "ma20": ma20,
                "above_ma20": above_ma20,
                "main_outflow": flow.get("main_outflow", False),
                "status": status,
                "score": score,
            })
    except Exception as e:
        _logger.warning(f"[core_pool] 读取失败: {e}")
    return core_items


def _get_weekly_buy_count(vault: ObsidianVault) -> int:
    """从本周日志统计买入次数"""
    try:
        from scripts.utils.parser import parse_journal_dir
        journal_dir = os.path.join(vault.vault_path, vault.journal_dir)
        stats = parse_journal_dir(journal_dir, days=7)
        return stats.get("total_trades", 0)
    except Exception as e:
        _logger.warning(f"[weekly_buy_count] 统计失败: {e}")
        return 0


def _build_discord_data(market_data: dict, positions: list, core_items: list,
                        weekly_bought: int, weekday: str) -> dict:
    """构造 Discord 推送所需的 data 字典"""
    # 格式化 positions for Discord
    discord_positions = []
    for pos in positions:
        note = pos.get("note", "")
        # 判断持仓市场（A股代码以 0/3/6/8/9 开头）
        code = pos.get("code", "")
        currency = "HK$" if code.startswith(("008", "009", "006")) else "¥"
        discord_positions.append({
            "name": pos["name"],
            "shares": pos["shares"],
            "price": pos["price"],
            "currency": currency,
            "note": note,
        })

    # 格式化 core_pool for Discord
    discord_core = []
    for item in core_items:
        score = item.get("score", 0)
        note = item.get("status", "")
        discord_core.append({
            "name": item.get("name", ""),
            "score": score,
            "note": note,
        })

    # 格式化 market for Discord
    discord_market = {}
    for name, info in market_data.get("market", {}).items():
        discord_market[name] = {
            "price": info.get("price", 0),
            "chg_pct": info.get("chg_pct", 0),
            "ma20_pct": info.get("ma20_pct", 0),
            "ma60_pct": info.get("ma60_pct", 0),
            "ma60_days": info.get("ma60_days", 0),
            "signal": info.get("signal", ""),
        }

    today = datetime.now().strftime("%Y-%m-%d")
    return {
        "date": today,
        "weekday": weekday,
        "market_signal": market_data.get("market_signal", ""),
        "market": discord_market,
        "positions": discord_positions,
        "core_pool": discord_core,
        "weekly_bought": weekly_bought,
        "weekly_limit": 2,
    }


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def run() -> dict:
    """
    执行盘前流程

    Returns:
        包含 market_data, positions, core_pool, weekly_bought 的 dict
    """
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    weekday = weekday_names[datetime.now().weekday()]

    _logger.info(f"[MORNING] 盘前流程 {datetime.now().strftime('%Y-%m-%d %H:%M')} ({weekday})")

    # 初始化组件
    vault = ObsidianVault()
    engine = DataEngine()
    strategy_cfg = get_strategy()

    # 1. 大盘数据
    _logger.info(">> 大盘数据")
    market_data = _get_market_data(engine)
    for name, info in market_data.get("market", {}).items():
        _logger.info(
            f"  {name}: {info.get('price'):.2f} "
            f"({info.get('chg_pct'):+.2f}%) "
            f"MA20:{info.get('ma20_pct'):+.2f}% "
            f"MA60:{info.get('ma60_pct'):+.2f}% "
            f"[{info.get('signal', '')}]"
        )
    _logger.info(f"  → 信号: {market_data.get('market_signal', 'UNKNOWN')}")

    # 2. 持仓状态
    _logger.info(">> 持仓状态")
    positions = _get_portfolio_positions(vault)
    if positions:
        for pos in positions:
            _logger.info(f"  {pos['name']} {pos['shares']}股 @ ¥{pos['price']:.2f}")
    else:
        _logger.info("  空仓")

    # 3. 核心池异动
    _logger.info(">> 核心池异动检查")
    core_items = _get_core_pool_status(vault, engine)
    for item in core_items:
        _logger.info(f"  {item['name']}: {item.get('status', 'OK')}")

    # 4. 本周买入统计
    weekly_bought = _get_weekly_buy_count(vault)
    _logger.info(f">> 本周买入: {weekly_bought}/2")

    # 5. 推送 Discord
    discord_data = _build_discord_data(market_data, positions, core_items, weekly_bought, weekday)
    ok, err = send_morning_summary(discord_data)
    if ok:
        _logger.info(">> Discord 推送成功")
    else:
        _logger.warning(f">> Discord 推送失败: {err}")

    _logger.info("[MORNING] 盘前流程完成")

    return {
        "market_data": market_data,
        "positions": positions,
        "core_pool": core_items,
        "weekly_bought": weekly_bought,
        "discord_data": discord_data,
    }


if __name__ == "__main__":
    import pandas as pd  # 确保 hist 字段可用
    result = run()
    print(f"\n盘前摘要已推送 Discord")
