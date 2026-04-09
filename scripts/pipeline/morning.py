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

from scripts.engine.data_engine import DataEngine
from scripts.state import load_activity_summary, load_market_snapshot, load_pool_snapshot, load_portfolio_snapshot
from scripts.utils.obsidian import ObsidianVault
from scripts.utils.discord_push import send_morning_summary
from scripts.utils.config_loader import get_strategy
from scripts.utils.logger import get_logger
from scripts.utils.runtime_state import update_pipeline_state

_logger = get_logger("pipeline.morning")


# ---------------------------------------------------------------------------
def _get_portfolio_positions() -> list:
    """从结构化账本读取有效持仓。"""
    positions = []
    try:
        snapshot = load_portfolio_snapshot(scope="cn_a_system")
        for row in snapshot.get("positions", []):
            shares = int(row.get("shares", 0) or 0)
            if shares <= 0:
                continue
            positions.append({
                "name": str(row.get("name", "")).strip(),
                "code": str(row.get("code", "")).strip(),
                "shares": shares,
                "price": float(row.get("current_price", row.get("avg_cost", 0)) or 0),
                "note": str(row.get("note", "")).strip(),
            })
    except Exception as e:
        _logger.warning(f"[portfolio] 读取失败: {e}")
    return positions


def _get_core_pool_status(engine: DataEngine) -> list:
    """检查结构化核心池异动（跌破 MA20 / 主力流出）"""
    core_items = []
    from scripts.engine.scorer import score as score_stock
    try:
        core_pool = load_pool_snapshot().get("core_pool", [])
        for item in core_pool:
            code = str(item.get("code", item.get("代码", ""))).strip()
            name = str(item.get("name", item.get("股票", ""))).strip()
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
            raw_score = str(item.get("total_score", item.get("四维总分", item.get("总分", 0)))).replace("**", "").strip()
            try:
                score = float(raw_score) if raw_score else 0.0
            except (TypeError, ValueError):
                score = 0.0

            if not above_ma20:
                status = "跌破MA20，需观察"
            elif flow.get("main_outflow", False):
                status = "主力流出"
            else:
                status = "正常"

            # 获取评分
            if score == 0:
                try:
                    score = score_stock(code, name).get("total_score", 0)
                except Exception as e:
                    _logger.warning(f"[core_pool] 评分获取失败 {name}({code}): {e}")
                    score = 0

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


def _get_morning_news(core_items: list, positions: list) -> list:
    """
    盘前资讯：用 MX 搜索核心池和持仓相关的最新新闻/公告/研报。
    每只股票取最重要的 1-2 条，控制总量。
    """
    news_items = []
    try:
        from scripts.mx.mx_search import MXSearch
        mx = MXSearch()

        # 收集需要搜索的股票（核心池 + 持仓，去重）
        stocks_to_search = {}
        for item in core_items:
            name = item.get("name", "")
            code = item.get("code", "")
            if name and code:
                stocks_to_search[code] = name
        for pos in positions:
            name = pos.get("name", "")
            code = pos.get("code", "")
            if name and code:
                stocks_to_search[code] = name

        for code, name in stocks_to_search.items():
            try:
                result = mx.search(f"{name} 最新公告 新闻")
                data = result.get("data", {})
                inner = data.get("data", {})
                search_resp = inner.get("llmSearchResponse", {})
                items = search_resp.get("data", [])

                if not items:
                    continue

                # 取前 2 条最相关的
                for item in items[:2]:
                    title = item.get("title", "").strip()
                    date = str(item.get("date", "")).split()[0] if item.get("date") else ""
                    info_type = item.get("informationType", "")
                    type_map = {"REPORT": "研报", "NEWS": "新闻", "ANNOUNCEMENT": "公告"}
                    type_cn = type_map.get(info_type, info_type)
                    rating = item.get("rating", "")

                    if title:
                        entry = {
                            "stock": name,
                            "code": code,
                            "title": title,
                            "date": date,
                            "type": type_cn,
                        }
                        if rating:
                            entry["rating"] = rating
                        news_items.append(entry)

            except Exception as e:
                _logger.info(f"[morning_news] {name} 搜索失败: {e}")

    except Exception as e:
        _logger.warning(f"[morning_news] MX 搜索不可用: {e}")

    return news_items


def _get_weekly_buy_count() -> int:
    """从结构化交易事件统计本周主动买入次数。"""
    try:
        summary = load_activity_summary("week", scope="cn_a_system")
        return int(summary.get("weekly_buy_count", summary.get("buy_count", 0)) or 0)
    except Exception as e:
        _logger.warning(f"[weekly_buy_count] 统计失败: {e}")
        return 0


def _build_discord_data(market_data: dict, positions: list, core_items: list,
                        weekly_bought: int, weekday: str) -> dict:
    """构造 Discord 推送所需的 data 字典"""
    market_indices = market_data.get("indices") or market_data.get("market") or {}

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
    for name, info in market_indices.items():
        if not isinstance(info, dict) or info.get("error"):
            continue
        discord_market[name] = {
            "price": info.get("close", info.get("price", 0)),
            "chg_pct": info.get("change_pct", info.get("chg_pct", 0)),
            "ma20_pct": info.get("ma20_pct", 0),
            "ma60_pct": info.get("ma60_pct", 0),
            "ma60_days": info.get("below_ma60_days", info.get("ma60_days", 0)),
            "signal": info.get("signal", ""),
        }

    today = datetime.now().strftime("%Y-%m-%d")
    return {
        "date": today,
        "weekday": weekday,
        "market_signal": market_data.get("signal", market_data.get("market_signal", "")),
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
    today_str = datetime.now().strftime("%Y-%m-%d")

    _logger.info(f"[MORNING] 盘前流程 {datetime.now().strftime('%Y-%m-%d %H:%M')} ({weekday})")

    try:
        vault = ObsidianVault()
        engine = DataEngine()
        get_strategy()

        _logger.info(">> 大盘数据")
        market_data = load_market_snapshot()
        market_indices = market_data.get("indices") or market_data.get("market") or {}
        for name, info in market_indices.items():
            if not isinstance(info, dict):
                _logger.warning(f"  {name}: 数据不可用")
                continue
            if info.get("error"):
                _logger.warning(f"  {name}: {info.get('error', '数据不可用')}")
                continue
            _logger.info(
                f"  {name}: {info.get('close', info.get('price', 0)):.2f} "
                f"({info.get('change_pct', info.get('chg_pct', 0)):+.2f}%) "
                f"MA20:{info.get('ma20_pct'):+.2f}% "
                f"MA60:{info.get('ma60_pct'):+.2f}% "
                f"[{info.get('signal', '')}]"
            )
        _logger.info(f"  → 信号: {market_data.get('signal', market_data.get('market_signal', 'UNKNOWN'))}")

        _logger.info(">> 持仓状态")
        positions = _get_portfolio_positions()
        if positions:
            for pos in positions:
                _logger.info(f"  {pos['name']} {pos['shares']}股 @ ¥{pos['price']:.2f}")
        else:
            _logger.info("  空仓")

        _logger.info(">> 核心池异动检查")
        core_items = _get_core_pool_status(engine)
        for item in core_items:
            _logger.info(f"  {item['name']}: {item.get('status', 'OK')}")

        # 4. 核心池+持仓相关新闻（MX 资讯搜索）
        _logger.info(">> 盘前资讯")
        news_items = _get_morning_news(core_items, positions)
        for news in news_items:
            _logger.info(f"  [{news['stock']}] {news['title']}")

        weekly_bought = _get_weekly_buy_count()
        _logger.info(f">> 本周买入: {weekly_bought}/2")

        discord_data = _build_discord_data(market_data, positions, core_items, weekly_bought, weekday)
        discord_data["news"] = news_items
        ok, err = send_morning_summary(discord_data)
        if ok:
            _logger.info(">> Discord 推送成功")
        else:
            _logger.warning(f">> Discord 推送失败: {err}")

        _logger.info("[MORNING] 盘前流程完成")

        try:
            from scripts.pipeline.shadow_trade import check_stop_signals
            shadow_results = check_stop_signals(dry_run=True)
            for r in shadow_results:
                if r.get("action") != "持有":
                    _logger.info(f"  ⚠️ 模拟盘 {r['name']}: {r['action']} — {r['reason']}")
                else:
                    _logger.info(f"  模拟盘 {r['name']}: {r.get('reason', '持有')}")
        except Exception as e:
            _logger.info(f">> 影子交易预览跳过: {e}")

        update_pipeline_state(
            "morning",
            "warning" if not ok else "success",
            {
                "market_signal": market_data.get("signal", market_data.get("market_signal", "")),
                "positions_count": len(positions),
                "core_pool_count": len(core_items),
                "weekly_bought": weekly_bought,
                "discord_ok": ok,
                "discord_error": err,
            },
            today_str,
        )

        return {
            "market_data": market_data,
            "positions": positions,
            "core_pool": core_items,
            "news": news_items,
            "weekly_bought": weekly_bought,
            "discord_data": discord_data,
        }
    except Exception as e:
        update_pipeline_state(
            "morning",
            "error",
            {"error": str(e)},
            today_str,
        )
        raise


if __name__ == "__main__":
    import pandas as pd  # 确保 hist 字段可用
    result = run()
    print(f"\n盘前摘要已推送 Discord")
