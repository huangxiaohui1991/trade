#!/usr/bin/env python3
"""
pipeline/evening.py — 收盘流程（15:35 执行）

职责：
  1. 更新持仓最新价格（写 portfolio.md）
  2. 重算止损/止盈价（写 portfolio.md）
  3. 检查是否触发止损/止盈
  4. 跑 core_pool_scoring.py（批量评分 → 写 Obsidian）
  5. 生成明日计划（写明天的日志 MD）
  6. 格式化收盘摘要 → Discord 推送

用法（CLI）：
  python -m scripts.pipeline.evening

用法（导入）：
  from scripts.pipeline.evening import run
  result = run()
"""

import os
import sys
import re
import shutil
import warnings
from datetime import datetime, timedelta
from pathlib import Path

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

import pandas as pd
from scripts.engine.data_engine import DataEngine
from scripts.utils.obsidian import ObsidianVault
from scripts.utils.discord_push import send_evening_report
from scripts.utils.config_loader import get_strategy
from scripts.utils.logger import get_logger
from scripts.utils.runtime_state import update_pipeline_state

_logger = get_logger("pipeline.evening")


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def _next_trading_day(date=None) -> datetime:
    """返回下一个交易日"""
    if date is None:
        date = datetime.now()
    next_day = date + timedelta(days=1)
    while next_day.weekday() >= 5:  # 0=周一, 5=周六, 6=周日
        next_day += timedelta(days=1)
    return next_day


def _is_trading_day(date=None) -> bool:
    """简单判断是否交易日（排除周末）"""
    if date is None:
        date = datetime.now()
    return date.weekday() < 5


def _get_stop_loss_and_take_profit(cost: float, ma20: float = 0) -> dict:
    """计算止损止盈价格（从 strategy.yaml 读取参数）"""
    strategy = get_strategy()
    risk = strategy.get("risk", {})
    stop_loss_pct = risk.get("stop_loss", 0.04)
    absolute_stop_pct = risk.get("absolute_stop", 0.07)
    tp = risk.get("take_profit", {})
    t1_pct = tp.get("t1_pct", 0.15)

    stop_loss = round(cost * (1 - stop_loss_pct), 2)
    absolute_stop = round(cost * (1 - absolute_stop_pct), 2)
    t1_price = round(cost * (1 + t1_pct), 2)

    return {
        "stop_loss": stop_loss,
        "absolute_stop": absolute_stop,
        "batch_1_price": t1_price,
    }


def _update_portfolio_prices(vault: ObsidianVault, engine: DataEngine) -> list:
    """
    更新 portfolio.md 中所有持仓的最新价格

    Returns:
        list of dict，每个元素包含 {name, code, old_price, new_price, pct_change, triggered}
    """
    changes = []
    portfolio = vault.read_portfolio()
    holdings = portfolio.get("holdings", [])

    active = [
        h for h in holdings
        if str(h.get("代码", "")).strip() not in ["", "—"]
        and str(h.get("股票", "")).strip() not in ["", "—", "空仓"]
        and int(float(h.get("持有股数", 0) or 0)) > 0
    ]

    if not active:
        _logger.info("无持仓，无需更新价格")
        return changes

    codes = [str(h.get("代码", "")).strip() for h in active]
    rt = engine.get_realtime(codes)

    updated_content = vault.read(vault.portfolio_path)

    for h in active:
        code = str(h.get("代码", "")).strip()
        name = str(h.get("股票", "")).strip()
        old_price = float(h.get("最新价", h.get("平均成本", 0)) or 0)
        cost = float(h.get("平均成本", 0) or 0)
        shares = int(float(h.get("持有股数", 0) or 0))

        # 获取最新价格
        stock_data = rt.get("data", {}).get(code, {})
        new_price = stock_data.get("price", old_price)
        chg_pct = stock_data.get("change_pct", 0)

        # 重算止损止盈
        stops = _get_stop_loss_and_take_profit(cost)

        # 判定是否触发止损/止盈
        triggered = []
        if new_price <= stops["stop_loss"] and stops["stop_loss"] > 0:
            triggered.append("止损")
        if new_price <= stops["absolute_stop"] and stops["absolute_stop"] > 0:
            triggered.append("绝对止损")
        if new_price >= stops["batch_1_price"] and stops["batch_1_price"] > 0:
            triggered.append("止盈1")

        # 计算市值变化
        new_value = round(new_price * shares, 2)
        pct_change = ((new_price / old_price) - 1) * 100 if old_price > 0 else 0

        changes.append({
            "name": name,
            "code": code,
            "shares": shares,
            "old_price": old_price,
            "new_price": new_price,
            "chg_pct": chg_pct,
            "new_value": new_value,
            "stop_loss": stops["stop_loss"],
            "absolute_stop": stops["absolute_stop"],
            "t1_price": stops["batch_1_price"],
            "triggered": triggered,
        })

        # 更新 portfolio.md 中的该行
        # 格式: | 名称 | 代码 | 首次买入价 | 加仓次数 | 平均成本 | 持有股数 | 市值 | 止损价 | 绝对止损 | 止盈1 | 状态 |
        new_row = (
            f"| {name} | {code} | "
            f"{h.get('首次买入价', h.get('平均成本', '—'))} | "
            f"{h.get('加仓次数', 0)} | "
            f"{cost} | {shares} | "
            f"¥{new_value:,.0f} | "
            f"{stops['stop_loss']} | {stops['absolute_stop']} | "
            f"{stops['batch_1_price']} | "
            f"{'条件单挂出' if triggered else '持有中'} |"
        )

        # 匹配该股票的持仓行
        pattern = rf'\| {re.escape(name)} \|[^\n]+\|'
        if re.search(pattern, updated_content):
            updated_content = re.sub(pattern, new_row, updated_content)
            _logger.info(
                f"  {name}: ¥{old_price:.2f} → ¥{new_price:.2f} ({pct_change:+.2f}%) "
                f"{'⚠️ ' + ','.join(triggered) if triggered else '✅'}"
            )

    # 更新日期戳
    today = datetime.now().strftime("%Y-%m-%d")
    updated_content = re.sub(
        r'(updated_at|date): \d{4}-\d{2}-\d{2}',
        f'\\1: {today}',
        updated_content
    )

    # 写回（带备份）
    vault.write(vault.portfolio_path, updated_content)
    return changes


def _generate_tomorrow_plan(vault: ObsidianVault, engine: DataEngine,
                             market_data: dict, position_changes: list) -> str:
    """生成明日计划 markdown 内容"""
    strategy = get_strategy()
    risk_cfg = strategy.get("risk", {})
    market_timer_cfg = strategy.get("market_timer", {})

    sh_info = market_data.get("market", {}).get("上证指数", {})
    cy_info = market_data.get("market", {}).get("创业板指", {})

    signal = market_data.get("market_signal", "CLEAR")
    green_days = market_timer_cfg.get("green_days", 3)
    red_days = market_timer_cfg.get("red_days", 5)

    lines = ["## 📋 明日计划（收盘自动生成）", ""]

    # 大盘状态
    lines.append("### 大盘状态")
    sh_above = sh_info.get("ma20_pct", 0) >= 0
    cy_above = cy_info.get("ma20_pct", 0) >= 0
    def _f(v, fmt=".2f"):
        """安全格式化数字，失败返回 '—'"""
        try:
            return format(float(v), fmt)
        except (TypeError, ValueError):
            return "—"

    sh_ma60_days = sh_info.get("ma60_days", 0)
    sh_price = sh_info.get("price", "—")
    lines.append(
        f"- 上证：{_f(sh_price)} "
        f"{'✅' if sh_above else '❌'}（vs MA20 {_f(sh_info.get('ma20_pct', 0), '+.2f')}% / "
        f"MA60 {_f(sh_info.get('ma60_pct', 0), '+.2f')}%）| MA60下方{sh_ma60_days}日"
    )
    cy_price = cy_info.get("price", "—")
    lines.append(
        f"- 创业板：{_f(cy_price)} "
        f"{'✅' if cy_above else '❌'}（vs MA20 {_f(cy_info.get('ma20_pct', 0), '+.2f')}% / "
        f"MA60 {_f(cy_info.get('ma60_pct', 0), '+.2f')}%）"
    )
    emoji = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴", "CLEAR": "⚪"}.get(signal, signal)
    lines.append(f"- **判定：{emoji} {signal}**（连续{green_days}日站上MA20→GREEN / "
                 f"连续{red_days}日跌破MA20→RED）")
    lines.append("")

    # 条件单清单
    lines.append("### 明日条件单清单")
    lines.append("")
    if position_changes:
        lines.append("| 股票 | 类型 | 触发价 | 数量 | 说明 |")
        lines.append("|------|------|--------|------|------|")
        for change in position_changes:
            if not change.get("new_price"):
                continue
            name = change["name"]
            shares = change.get("shares", "")
            lines.append(
                f"| {name} | 止损 | {change.get('stop_loss', '—')} | {shares} | 动态止损 |"
            )
            lines.append(
                f"| {name} | 绝对止损 | {change.get('absolute_stop', '—')} | {shares} | -7%无条件 |"
            )
            lines.append(
                f"| {name} | 止盈(第一批) | {change.get('t1_price', '—')} | 1/3仓 | +15%卖1/3 |"
            )
    else:
        lines.append("无持仓，无需挂条件单。")
    lines.append("")

    # 明日重点
    lines.append("### 明日重点")
    if signal == "RED":
        lines.append("1. 🔴 大盘清仓信号，不买入，只观察")
    elif signal == "YELLOW":
        lines.append("1. 🟡 大盘谨慎状态，如买入需减半金额")
    else:
        lines.append("1. 🟢 大盘可操作，关注核心池买入机会")
    lines.append("2. 盘前照条件单清单挂单，确认后打勾")
    lines.append("3. 买入后更新 portfolio.md")

    return "\n".join(lines)


def _create_tomorrow_journal(vault: ObsidianVault, tomorrow_date: str, plan_content: str) -> None:
    """创建明日日志文件"""
    journal_rel_path = vault.get_journal_path(tomorrow_date)
    journal_full_path = Path(vault.vault_path) / journal_rel_path

    if journal_full_path.exists():
        # 文件已存在，只更新明日计划部分
        with open(journal_full_path, 'r', encoding='utf-8') as f:
            content = f.read()
        marker = "## 📋 明日计划"
        if marker in content:
            idx = content.index(marker)
            content = content[:idx] + plan_content
        else:
            content += "\n---\n\n" + plan_content
        with open(journal_full_path, 'w', encoding='utf-8') as f:
            f.write(content)
        _logger.info(f"  已更新: {journal_full_path.name}")
        return

    # 创建新文件
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    dt = datetime.strptime(tomorrow_date, "%Y-%m-%d")
    weekday = weekday_names[dt.weekday()]

    journal_content = f"""---
date: {tomorrow_date}
type: journal
tags: [交易日志]
total_asset:
daily_pnl:
trades: 0
buy_count_this_week:
mood:
market_status:
---

# {tomorrow_date}（{weekday}）交易日志

## 盘前确认（8:25-9:15）

> 以下内容由昨日收盘的"明日计划"自动生成，你只需确认执行。

- [ ] 看隔夜新闻标题（5分钟，只看核心池+观察池相关）
- [ ] 朗读心理检查表
- [ ] 确认所有条件单已挂好（参照下方明日计划的条件单清单）
- [ ] 确认条件单价格正确
- [ ] 如有异常情况，记录在下方

**盘前备注：**


## 午休检查（11:55-12:00）

- [ ] 查看条件单成交情况
- [ ] 如有加仓机会且本周还有次数，挂限价单（不追高）

**午休备注：**


## 收盘记录（15:00-15:30）

**账户总资产：** ¥
**今日盈亏：** ¥
**今日交易：**

| 时间 | 股票 | 操作 | 价格 | 数量 | 金额 | 类型 |
|------|------|------|------|------|------|------|
|  |  |  |  |  |  | 首次买入/加仓/止盈/止损 |

## 今日一句话

> 今日执行是否严格？情绪如何？

---

{plan_content}
"""
    with open(journal_full_path, 'w', encoding='utf-8') as f:
        f.write(journal_content)
    _logger.info(f"  已创建: {journal_full_path.name}")


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def run() -> dict:
    """执行收盘流程"""
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    weekday = weekday_names[datetime.now().weekday()]
    today_str = datetime.now().strftime("%Y-%m-%d")

    _logger.info(f"[EVENING] 收盘流程 {today_str} ({weekday})")
    try:
        vault = ObsidianVault()
        engine = DataEngine()
        strategy_cfg = get_strategy()

        _logger.info(">> 大盘数据")
        from scripts.pipeline.morning import _get_market_data
        market_data = _get_market_data(engine)
        for name, info in market_data.get("market", {}).items():
            _logger.info(f"  {name}: {info.get('price'):.2f} ({info.get('chg_pct'):+.2f}%) [{info.get('signal', '')}]")
        _logger.info(f"  → 信号: {market_data.get('market_signal', 'UNKNOWN')}")

        _logger.info(">> 更新持仓价格...")
        position_changes = _update_portfolio_prices(vault, engine)

        _logger.info(">> 生成明日计划...")
        tomorrow = _next_trading_day()
        tomorrow_str = tomorrow.strftime("%Y-%m-%d")
        plan_content = _generate_tomorrow_plan(vault, engine, market_data, position_changes)

        _logger.info(f">> 创建明日日志: {tomorrow_str}.md")
        _create_tomorrow_journal(vault, tomorrow_str, plan_content)

        discord_positions = []
        total_value = 0
        for change in position_changes:
            new_value = change.get("new_value", 0)
            total_value += new_value
            discord_positions.append({
                "name": change["name"],
                "shares": change.get("shares", 0),
                "value": new_value,
                "currency": "¥",
                "status": "持有中",
            })

        alerts = []
        for change in position_changes:
            for t in change.get("triggered", []):
                alerts.append(f"{change['name']}: {t}@{change.get('new_price', '?')}")

        portfolio = vault.read_portfolio()
        holdings = portfolio.get("holdings", [])
        weekly_bought = sum(
            int(float(h.get("加仓次数", 0) or 0)) + 1
            for h in holdings
            if str(h.get("股票", "")).strip() not in ["", "—", "空仓"]
            and int(float(h.get("持有股数", 0) or 0)) > 0
        )
        weekly_limit = strategy_cfg.get("risk", {}).get("position", {}).get("weekly_max", 2)

        core_pool = vault.read_core_pool()
        discord_core = []
        for item in core_pool:
            name = str(item.get("股票", ""))
            raw_score = str(item.get("四维总分", item.get("总分", 0))).replace("**", "").strip()
            try:
                score = float(raw_score) if raw_score else 0.0
            except (TypeError, ValueError):
                score = 0.0
            note = str(item.get("备注", ""))
            if name and name not in ["", "—"]:
                discord_core.append({"name": name, "score": score, "note": note})

        tomorrow_plan = []
        signal = market_data.get("market_signal", "")
        if signal == "GREEN":
            tomorrow_plan.append("🟢 GREEN信号，可正常买入")
        elif signal == "YELLOW":
            tomorrow_plan.append("🟡 YELLOW信号，如买入需减半金额")
        else:
            tomorrow_plan.append("🔴 RED/CLEAR信号，不买入，只观察")
        tomorrow_plan.append(f"本周买入: {weekly_bought}/{weekly_limit}")

        discord_data = {
            "date": today_str,
            "weekday": weekday,
            "market": {
                name: {
                    "price": info.get("price", 0),
                    "chg_pct": info.get("chg_pct", 0),
                    "signal": info.get("signal", ""),
                }
                for name, info in market_data.get("market", {}).items()
            },
            "positions": discord_positions,
            "total_value": total_value,
            "currency": "¥",
            "alerts": alerts,
            "core_pool": discord_core,
            "tomorrow_plan": tomorrow_plan,
        }

        ok, err = send_evening_report(discord_data)
        if ok:
            _logger.info(">> Discord 推送成功")
        else:
            _logger.warning(f">> Discord 推送失败: {err}")

        _logger.info(f"[EVENING] 收盘流程完成 → 明日日志: {tomorrow_str}.md")

        try:
            from scripts.pipeline.shadow_trade import check_stop_signals
            shadow_results = check_stop_signals()
            triggered = [r for r in shadow_results if r.get("action") != "持有"]
            if triggered:
                _logger.info(f">> 影子交易: {len(triggered)} 只触发信号")
        except Exception as e:
            _logger.warning(f">> 影子交易检查失败: {e}")

        update_pipeline_state(
            "evening",
            "warning" if not ok else "success",
            {
                "market_signal": market_data.get("market_signal", ""),
                "position_change_count": len(position_changes),
                "tomorrow_date": tomorrow_str,
                "discord_ok": ok,
                "discord_error": err,
            },
            today_str,
        )

        return {
            "market_data": market_data,
            "position_changes": position_changes,
            "tomorrow_plan": plan_content,
            "tomorrow_date": tomorrow_str,
            "discord_data": discord_data,
        }
    except Exception as e:
        update_pipeline_state(
            "evening",
            "error",
            {"error": str(e)},
            today_str,
        )
        raise


if __name__ == "__main__":
    import pandas as pd
    result = run()
    print(f"\n收盘报告已推送 Discord")
    print(f"明日日志: {result['tomorrow_date']}.md")
