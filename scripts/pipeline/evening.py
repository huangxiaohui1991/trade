#!/usr/bin/env python3
"""
pipeline/evening.py — 收盘流程（15:35 执行）

职责：
  6  1. 更新持仓最新价格（写 portfolio.md）
  7  2. 重算止损/止盈价（写 portfolio.md）
  8  3. 检查是否触发止损/止盈
  9  4. 跑 core_pool_scoring.py（批量评分 → 写 Obsidian）
 10  5. 生成明日计划（写明天的日志 MD）
 11  6. 格式化收盘摘要 → Discord 推送
 12  7. 收盘 Enrichment → 追加到今日日志（持仓逻辑、明日关注点、教训洞察）

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
from scripts.state import load_activity_summary, load_market_snapshot, load_pool_snapshot, load_portfolio_snapshot
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
    snapshot = load_portfolio_snapshot(scope="cn_a_system")
    active = snapshot.get("positions", [])
    projection_rows = {}
    try:
        projection_rows = {
            str(row.get("代码", "")).strip(): row
            for row in vault.read_portfolio().get("holdings", [])
        }
    except Exception:
        projection_rows = {}

    if not active:
        _logger.info("无持仓，无需更新价格")
        return changes

    codes = [str(h.get("code", "")).strip() for h in active]
    rt = engine.get_realtime(codes)

    updated_content = vault.read(vault.portfolio_path)

    for h in active:
        code = str(h.get("code", "")).strip()
        name = str(h.get("name", "")).strip()
        old_price = float(h.get("current_price", h.get("avg_cost", 0)) or 0)
        cost = float(h.get("avg_cost", 0) or 0)
        shares = int(float(h.get("shares", 0) or 0))
        projection_row = projection_rows.get(code, {})

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
            "cost_price": cost,
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
            f"{projection_row.get('首次买入价', projection_row.get('平均成本', cost))} | "
            f"{projection_row.get('加仓次数', 0)} | "
            f"{cost} | {shares} | "
            f"¥{new_value:,.0f} | "
            f"{stops['stop_loss']} | {stops['absolute_stop']} | "
            f"{stops['batch_1_price']} | "
            f"{'条件单挂出' if triggered else '持有中'} |"
        )

        # 匹配该股票的持仓行
        pattern = rf'^\| {re.escape(name)} \| {re.escape(code)} \|[^\n]+\|$'
        if re.search(pattern, updated_content, flags=re.MULTILINE):
            updated_content = re.sub(pattern, new_row, updated_content, flags=re.MULTILINE)
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
    vault.sync_portfolio_state()
    return changes

# ─────────────────────────────────────────────────────────────────────────────
def _enrich_today_journal(vault: ObsidianVault, today_str: str,
                           market_data: dict, position_changes: list,
                           shadow_results: list) -> None:
    """收盘后追加 enrichment 内容到今日日志"""
    vault_path = Path(vault.vault_path)

    # 构造路径：vault/交易日志/YYYY-MM-DD.md
    journal_path = vault_path / "交易日志" / f"{today_str}.md"
    if not journal_path.exists():
        _logger.warning(f"  今日日志不存在，跳过 enrichment: {journal_path.name}")
        return

    # ── A. 持仓逻辑复盘 ─────────────────────────────────────────────────────
    position_lines = []
    for chg in position_changes:
        name = chg.get("name", "")
        shares = chg.get("shares", 0)
        new_price = chg.get("new_price", 0)
        cost = chg.get("cost_price", 0)
        pnl_pct = ((new_price - cost) / cost * 100) if cost > 0 else 0
        triggered = chg.get("triggered", [])
        position_lines.append(
            f"- **{name}** {shares}股 @{new_price:.2f}（成本{cost:.2f}, {pnl_pct:+.2f}%）"
            + (f" → 触发: {'/'.join(triggered)}" if triggered else "")
        )

    # ── B. 影子交易摘要 ─────────────────────────────────────────────────────
    shadow_lines = []
    for r in shadow_results:
        line = f"- **{r['name']}**({r['code']}): {r['action']} — {r.get('reason', '')}"
        if r.get("advisory_signals") and r.get("action") == "持有":
            line += f" | 提示: {r.get('advisory_summary', '')}"
        shadow_lines.append(line)

    # ── C. 大盘信号 ──────────────────────────────────────────────────────────
    market_indices = market_data.get("indices") or market_data.get("market") or {}
    market_lines = []
    for name, info in market_indices.items():
        if not isinstance(info, dict) or info.get("error"):
            continue
        close = info.get("close", info.get("price", 0))
        chg = info.get("change_pct", info.get("chg_pct", 0))
        sig = info.get("signal", "")
        market_lines.append(f"- {name}: {close:.2f} ({chg:+.2f}%) [{sig}]")

    signal = market_data.get("signal", market_data.get("market_signal", "CLEAR"))
    signal_map = {"GREEN": "🟢 偏强", "YELLOW": "🟡 震荡", "RED": "🔴 转弱", "CLEAR": "⚪ 观望"}
    signal_text = signal_map.get(signal, signal)

    # ── D. 组装 enrichment block ─────────────────────────────────────────────
    enrichment = f"""
---

## 🧠 收盘 Enrichment（系统自动追加）

### 大盘收盘
{signal_text}

""" + "\n".join(market_lines) + """

### 持仓状态
"""
    if position_lines:
        enrichment += "\n".join(position_lines) + "\n"
    else:
        enrichment += "（今日无持仓变化）\n"

    enrichment += """
### 影子池表现
"""
    if shadow_lines:
        enrichment += "\n".join(shadow_lines) + "\n"
    else:
        enrichment += "（今日无影子交易信号）\n"

    enrichment += """
### 今日教训/洞察

> 记录今日执行中的问题或发现（例如：追高了、止损执行慢了、条件单价格有误）

- [ ]

---
"""

    # ── E. 追加写入（插在 `---` 之前，不要重复追加）────────────────────────
    with open(journal_path, 'r', encoding='utf-8') as f:
        content = f.read()

    marker = "## 🧠 收盘 Enrichment（系统自动追加）"
    if marker in content:
        # 已追加过，更新整个 block
        import re
        pattern = re.escape(marker) + r".*?(?=\n---\n|$)"
        content = re.sub(pattern, enrichment.strip(), content, flags=re.DOTALL)
        _logger.info(f"  已更新 enrichment: {journal_path.name}")
    else:
        # 插在文件末尾（最后一个 --- 之后）
        if content.rstrip().endswith("---"):
            content = content.rstrip() + "\n" + enrichment.lstrip()
        else:
            content += "\n" + enrichment
        _logger.info(f"  已追加 enrichment: {journal_path.name}")

    with open(journal_path, 'w', encoding='utf-8') as f:
        f.write(content)


# ─────────────────────────────────────────────────────────────────────────────
def _generate_tomorrow_plan(vault: ObsidianVault, engine: DataEngine,
                             market_data: dict, position_changes: list) -> str:
    """生成明日计划 markdown 内容"""
    strategy = get_strategy()
    market_timer_cfg = strategy.get("market_timer", {})

    market_indices = market_data.get("indices") or market_data.get("market") or {}
    sh_info = market_indices.get("上证指数", {})
    cy_info = market_indices.get("创业板指", {})

    signal = market_data.get("signal", market_data.get("market_signal", "CLEAR"))
    green_days = market_timer_cfg.get("green_days", 3)
    red_days = market_timer_cfg.get("red_days", 5)

    lines = ["## 📋 明日计划（收盘自动生成）", ""]

    # 大盘状态
    lines.append("### 大盘状态")
    sh_above = sh_info.get("above_ma20", False)
    cy_above = cy_info.get("above_ma20", False)
    def _f(v, fmt=".2f"):
        """安全格式化数字，失败返回 '—'"""
        try:
            return format(float(v), fmt)
        except (TypeError, ValueError):
            return "—"

    sh_ma60_days = sh_info.get("below_ma60_days", sh_info.get("ma60_days", 0))
    sh_price = sh_info.get("close", sh_info.get("price", "—"))
    lines.append(
        f"- 上证：{_f(sh_price)} "
        f"{'✅' if sh_above else '❌'}（vs MA20 {_f(sh_info.get('ma20_pct', 0), '+.2f')}% / "
        f"MA60 {_f(sh_info.get('ma60_pct', 0), '+.2f')}%）| MA60下方{sh_ma60_days}日"
    )
    cy_price = cy_info.get("close", cy_info.get("price", "—"))
    lines.append(
        f"- 创业板：{_f(cy_price)} "
        f"{'✅' if cy_above else '❌'}（vs MA20 {_f(cy_info.get('ma20_pct', 0), '+.2f')}% / "
        f"MA60 {_f(cy_info.get('ma60_pct', 0), '+.2f')}%）"
    )
    emoji = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴", "CLEAR": "⚪"}.get(signal, signal)
    signal_cn = {"GREEN": "偏强", "YELLOW": "震荡", "RED": "转弱", "CLEAR": "观望"}.get(signal, signal)
    lines.append(f"- **判定：{emoji} {signal_cn}**（连续{green_days}日站上MA20→偏强 / "
                 f"连续{red_days}日跌破MA20→转弱）")
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
        lines.append("1. 🔴 大盘转弱，不买入，只观察")
    elif signal == "YELLOW":
        lines.append("1. 🟡 大盘震荡，如买入需减半金额")
    elif signal == "CLEAR":
        lines.append("1. ⚪ 大盘观望，不抄底")
    else:
        lines.append("1. 🟢 大盘偏强，关注核心池买入机会")
    lines.append("2. 盘前照条件单清单挂单，确认后打勾")
    lines.append("3. 买入后更新 portfolio.md")

    return "\n".join(lines)


def _backfill_today_trades(vault: ObsidianVault, today_str: str) -> int:
    """
    从结构化 ledger 读取今日交易事件，回填到今日交易日志的"收盘记录"表格。
    返回回填的交易条数。
    """
    try:
        from scripts.state.service import _connect, _ensure_bootstrapped, _normalize_code
    except Exception:
        return 0

    journal_rel_path = vault.get_journal_path(today_str)
    journal_full_path = Path(vault.vault_path) / journal_rel_path
    if not journal_full_path.exists():
        return 0

    # 从 ledger 读取今日交易事件
    try:
        with _connect() as conn:
            _ensure_bootstrapped(conn)
            cursor = conn.execute(
                "SELECT code, name, side, shares, price, amount, reason_code, reason_text, created_at "
                "FROM trade_events WHERE event_date = ? ORDER BY created_at",
                (today_str,),
            )
            rows = cursor.fetchall()
    except Exception:
        return 0

    if not rows:
        return 0

    # 构建交易表格行
    trade_lines = []
    for row in rows:
        code, name, side, shares, price, amount, reason_code, reason_text, created_at = row
        time_str = str(created_at or "")[:16].split("T")[-1].split(" ")[-1] if created_at else ""
        if not time_str:
            time_str = "—"
        action = "买入" if "buy" in str(side).lower() else "卖出" if "sell" in str(side).lower() else str(side)
        amount_val = float(amount or 0) or round(float(price or 0) * int(shares or 0), 2)
        trade_type = str(reason_code or reason_text or "—")
        trade_lines.append(
            f"| {time_str} | {name or code} | {action} | ¥{float(price or 0):.2f} "
            f"| {int(shares or 0)} | ¥{amount_val:,.0f} | {trade_type} |"
        )

    if not trade_lines:
        return 0

    # 读取当前日志内容
    content = journal_full_path.read_text(encoding="utf-8")

    # 查找空的交易表格并替换
    empty_row = "|  |  |  |  |  |  | 首次买入/加仓/止盈/止损 |"
    if empty_row in content:
        content = content.replace(empty_row, "\n".join(trade_lines))
        journal_full_path.write_text(content, encoding="utf-8")
        _logger.info(f"  已回填 {len(trade_lines)} 条交易到 {today_str}.md")
        return len(trade_lines)

    # 如果表格已有内容但不是空模板，追加到表格末尾
    table_header = "| 时间 | 股票 | 操作 | 价格 | 数量 | 金额 | 类型 |"
    if table_header in content:
        # 找到表格分隔行后的位置
        header_idx = content.index(table_header)
        # 找到分隔行
        sep_line = "|------|------|------|------|------|------|------|"
        if sep_line in content[header_idx:]:
            sep_idx = content.index(sep_line, header_idx)
            after_sep = sep_idx + len(sep_line)
            # 在分隔行后插入（替换已有内容到下一个空行或 ## 标记）
            rest = content[after_sep:]
            # 找到下一个 section
            next_section = rest.find("\n## ")
            if next_section == -1:
                next_section = len(rest)
            content = content[:after_sep] + "\n" + "\n".join(trade_lines) + "\n" + rest[next_section:]
            journal_full_path.write_text(content, encoding="utf-8")
            _logger.info(f"  已回填 {len(trade_lines)} 条交易到 {today_str}.md")
            return len(trade_lines)

    return 0


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
        market_data = load_market_snapshot(refresh=True)
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
                f"[{info.get('signal', '')}]"
            )
        _signal = market_data.get('signal', market_data.get('market_signal', 'UNKNOWN'))
        _signal_map = {"GREEN": "偏强", "YELLOW": "震荡", "RED": "转弱", "CLEAR": "观望"}
        _logger.info(f"  → 信号: {_signal_map.get(_signal, _signal)}")

        _logger.info(">> 更新持仓价格...")
        position_changes = _update_portfolio_prices(vault, engine)

        _logger.info(">> 回填今日交易到日志...")
        backfill_count = _backfill_today_trades(vault, today_str)
        if backfill_count:
            _logger.info(f"  回填 {backfill_count} 条交易记录")

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

        activity = load_activity_summary("week", scope="cn_a_system")
        weekly_bought = int(activity.get("weekly_buy_count", activity.get("buy_count", 0)) or 0)
        weekly_limit = strategy_cfg.get("risk", {}).get("position", {}).get("weekly_max", 2)

        core_pool = load_pool_snapshot().get("core_pool", [])
        discord_core = []
        for item in core_pool:
            name = str(item.get("name", item.get("股票", "")))
            raw_score = str(item.get("total_score", item.get("四维总分", item.get("总分", 0)))).replace("**", "").strip()
            try:
                score = float(raw_score) if raw_score else 0.0
            except (TypeError, ValueError):
                score = 0.0
            note = str(item.get("note", item.get("备注", "")))
            if name and name not in ["", "—"]:
                discord_core.append({"name": name, "score": score, "note": note})

        tomorrow_plan = []
        signal = market_data.get("signal", market_data.get("market_signal", ""))
        if signal == "GREEN":
            tomorrow_plan.append("🟢 偏强，可正常买入")
        elif signal == "YELLOW":
            tomorrow_plan.append("🟡 震荡，如买入需减半金额")
        elif signal == "CLEAR":
            tomorrow_plan.append("⚪ 观望，不抄底")
        else:
            tomorrow_plan.append("🔴 转弱，不买入，只观察")
        tomorrow_plan.append(f"本周买入: {weekly_bought}/{weekly_limit}")

        discord_data = {
            "date": today_str,
            "weekday": weekday,
            "market": {
                name: {
                    "price": info.get("close", info.get("price", 0)),
                    "chg_pct": info.get("change_pct", info.get("chg_pct", 0)),
                    "signal": info.get("signal", ""),
                }
                for name, info in market_indices.items()
                if isinstance(info, dict) and not info.get("error")
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
            from scripts.pipeline.shadow_trade import check_stop_signals, generate_report as generate_shadow_report
            shadow_results = check_stop_signals()
            triggered = [r for r in shadow_results if r.get("action") != "持有"]
            advisories = [
                r for r in shadow_results
                if r.get("advisory_signals") and r.get("action") == "持有"
            ]
            if triggered:
                _logger.info(f">> 影子交易: {len(triggered)} 只触发信号")
                for r in triggered:
                    _logger.info(f"  {r['name']}({r['code']}): {r['action']} — {r['reason']}")
            if advisories:
                _logger.info(f">> 影子交易 advisory: {len(advisories)} 只")
                for r in advisories:
                    _logger.info(f"  {r['name']}({r['code']}): {r.get('advisory_summary', '')}")

            # 生成/更新模拟盘日报（拉最新持仓和价格）
            try:
                report_path = generate_shadow_report()
                _logger.info(f">> 模拟盘报告已更新: {report_path}")
            except Exception as re:
                _logger.warning(f">> 模拟盘报告生成失败: {re}")
        except Exception as e:
            _logger.warning(f">> 影子交易检查失败: {e}")

        # ── 7. 收盘 Enrichment → 追加到今日日志 ──────────────────────────────
        _enrich_today_journal(vault, today_str, market_data, position_changes, shadow_results)

        update_pipeline_state(
            "evening",
            "warning" if not ok else "success",
            {
                "market_signal": market_data.get("signal", market_data.get("market_signal", "")),
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
