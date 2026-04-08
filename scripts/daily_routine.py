#!/usr/bin/env python3
"""
A股交易系统 v1.3 - 每日自动化流程
用法:
  python daily_routine.py evening    收盘流程（15:30）— 生成明日计划
  python daily_routine.py morning    盘前流程（8:30）— 补充实时数据
  python daily_routine.py noon       午休检查（12:00）
  python daily_routine.py weekly     周复盘（周日）
  python daily_routine.py record buy <名称> <代码> <价格> <股数> [首次买入/加仓]
  python daily_routine.py record sell <名称> <代码> <价格> <股数> [止盈/止损/时间止损/清仓]
"""

import sys
import os
import json
import re
import warnings
from datetime import datetime, timedelta
from pathlib import Path

# 屏蔽 akshare 的 tqdm 进度条和警告
os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore", category=DeprecationWarning)

# 脚本目录和数据目录
SCRIPT_DIR = Path("/Users/hxh/Documents/a-stock-trading/scripts")
DATA_DIR = SCRIPT_DIR.parent / "data"
JOURNAL_DIR = DATA_DIR / "02-日志"
PORTFOLIO_PATH = DATA_DIR / "01-持仓" / "portfolio.md"
CORE_POOL_PATH = DATA_DIR / "04-选股" / "核心池.md"
TEMPLATE_DIR = DATA_DIR / "00-系统" / "模板"

# 导入同目录下的模块
sys.path.insert(0, str(SCRIPT_DIR))
from akshare_data import get_market_status, get_technical, get_fund_flow
from calculator import (
    calc_stop_loss, calc_take_profit, calc_position,
    calc_add_position, calc_time_stop, calc_market_stop,
    TOTAL_CAPITAL, MAX_BUY_PER_WEEK, MAX_HOLDING_STOCKS
)
from parser import parse_frontmatter, parse_md_table, parse_portfolio


def is_trading_day(date=None):
    """判断是否交易日（使用节假日列表）"""
    try:
        from holidays import is_trading_day as _is_trading_day
        return _is_trading_day(date)
    except ImportError:
        # fallback: 只排除周末
        if date is None:
            date = datetime.now()
        return date.weekday() < 5


def next_trading_day(date=None):
    """获取下一个交易日"""
    try:
        from holidays import next_trading_day as _next_trading_day
        return _next_trading_day(date)
    except ImportError:
        if date is None:
            date = datetime.now()
        next_day = date + timedelta(days=1)
        while next_day.weekday() >= 5:
            next_day += timedelta(days=1)
        return next_day


def load_portfolio_data():
    """加载持仓数据"""
    if not PORTFOLIO_PATH.exists():
        return {"holdings": [], "meta": {}}
    return parse_portfolio(str(PORTFOLIO_PATH))


def update_portfolio_prices():
    """
    自动更新 portfolio.md 中持仓的最新价格和止损止盈价
    只更新"持仓明细"表格中有效持仓的市值和止损价
    """
    if not PORTFOLIO_PATH.exists():
        return

    with open(PORTFOLIO_PATH, 'r', encoding='utf-8') as f:
        content = f.read()

    portfolio = load_portfolio_data()
    holdings = portfolio.get("holdings", [])
    active = [h for h in holdings
              if str(h.get("股票", "")).strip() not in ["", "—", "空仓"]
              and str(h.get("代码", "")).strip() not in ["", "—"]]

    if not active:
        # 更新日期即可
        today = datetime.now().strftime("%Y-%m-%d")
        content = re.sub(r'updated_at: \d{4}-\d{2}-\d{2}', f'updated_at: {today}', content)
        with open(PORTFOLIO_PATH, 'w', encoding='utf-8') as f:
            f.write(content)
        return

    import time

    for h in active:
        code = str(h.get("代码", "")).strip()
        name = str(h.get("股票", "")).strip()
        avg_cost = h.get("平均成本", 0)
        shares = h.get("持有股数", 0)
        first_buy = h.get("首次买入价", 0)

        try:
            avg_cost = float(avg_cost) if avg_cost else 0
            shares = int(float(shares)) if shares else 0
            first_buy = float(first_buy) if first_buy else 0
        except (ValueError, TypeError):
            continue

        if not code or shares <= 0:
            continue

        # 获取最新价格
        tech = get_technical(code, 10)
        time.sleep(0.5)

        if "error" in tech or not tech.get("current_price"):
            continue

        current_price = tech["current_price"]
        ma20 = tech.get("ma", {}).get("MA20")

        # 计算新市值
        new_value = round(current_price * shares, 2)

        # 重算止损价
        stops = calc_stop_loss(avg_cost, ma20)

        # 重算止盈价
        tp = calc_take_profit(first_buy if first_buy > 0 else avg_cost)

        # 在 content 中找到这只股票的行并替换
        # 匹配持仓明细表格中包含该股票名称的行
        pattern = rf'\| {re.escape(name)} \|[^\n]*\|'
        match = re.search(pattern, content)
        if match:
            new_row = (f"| {name} | {code} | {first_buy if first_buy > 0 else '—'} | "
                      f"{h.get('加仓次数', 0)} | {avg_cost} | {shares} | "
                      f"¥{new_value:,.0f} | {stops['stop_loss']} | "
                      f"{stops['absolute_stop']} | {tp['batch_1_price']} | 持有中 |")
            content = content[:match.start()] + new_row + content[match.end():]

        print(f"  {name}: {current_price} → 市值¥{new_value:,.0f} | 止损{stops['stop_loss']} | 止盈{tp['batch_1_price']}")

    # 更新日期
    today = datetime.now().strftime("%Y-%m-%d")
    content = re.sub(r'updated_at: \d{4}-\d{2}-\d{2}', f'updated_at: {today}', content)
    content = re.sub(r'^date: \d{4}-\d{2}-\d{2}', f'date: {today}', content, flags=re.MULTILINE)

    with open(PORTFOLIO_PATH, 'w', encoding='utf-8') as f:
        f.write(content)


def load_core_pool():
    """加载核心池数据"""
    if not CORE_POOL_PATH.exists():
        return []
    with open(CORE_POOL_PATH, 'r', encoding='utf-8') as f:
        content = f.read()
    tables = parse_md_table(content)
    stocks = []
    for table in tables:
        for row in table["rows"]:
            code = row.get("代码", "").strip()
            name = row.get("股票", "").strip()
            if code and code != "—" and len(code) == 6:
                stocks.append({"code": code, "name": name, "raw": row})
    return stocks


def get_weekly_buy_count():
    """获取本周买入次数（从日志统计）"""
    if not JOURNAL_DIR.exists():
        return 0
    now = datetime.now()
    monday = now - timedelta(days=now.weekday())
    count = 0
    for f in JOURNAL_DIR.iterdir():
        if not f.name.endswith('.md'):
            continue
        m = re.match(r'(\d{4}-\d{2}-\d{2})', f.name)
        if not m:
            continue
        fdate = datetime.strptime(m.group(1), '%Y-%m-%d')
        if fdate >= monday:
            with open(f, 'r', encoding='utf-8') as fh:
                fm = parse_frontmatter(fh.read())
                count += fm.get("trades", 0)
    return count


# ============================================================
# 收盘流程 evening_routine — 最核心的自动化流程
# ============================================================

def evening_routine():
    """
    收盘流程（15:30 执行）
    1. 获取大盘收盘数据
    2. 获取持仓/核心池最新技术面
    3. 重算止损止盈
    4. 检查时间止损
    5. 检查核心池异常
    6. 生成明日计划
    7. 创建明日日志文件
    """
    SEP = "-" * 52
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    print(SEP)
    print(f"[EVENING] 收盘流程 {ts}")
    print(SEP)

    # 1. 大盘状态
    print("\n>> 大盘数据")
    market = get_market_status()
    sh = market.get("上证指数", {})
    cy = market.get("创业板指", {})
    summary = market.get("_summary", {})

    rt_tag = "[实时]" if sh.get("realtime") else "[昨日]"
    sh_chg = f"{sh['change_pct']:+.2f}%" if sh.get("change_pct") is not None else "—"
    cy_chg = f"{cy['change_pct']:+.2f}%" if cy.get("change_pct") is not None else "—"

    sh_ma = "OK" if sh.get("above_MA20") else "NO"
    cy_ma = "OK" if cy.get("above_MA20") else "NO"

    print(f"  上证 {rt_tag} {sh.get('close')} ({sh_chg}) | MA20={sh.get('MA20')} [{sh_ma}] | MA60下方{sh.get('below_MA60_days', 0)}天")
    print(f"  创业板 {rt_tag} {cy.get('close')} ({cy_chg}) | MA20={cy.get('MA20')} [{cy_ma}] | MA60下方{cy.get('below_MA60_days', 0)}天")
    print(f"  -> 判定: {summary.get('status', 'UNKNOWN')}")

    # 2. 更新持仓价格
    print("\n>> 更新持仓价格...")
    update_portfolio_prices()

    # 3. 加载持仓数据（更新后重新读取）
    print("\n>> 加载持仓数据...")
    portfolio = load_portfolio_data()
    holdings = portfolio.get("holdings", [])
    print(f"  持仓数: {len(holdings)}")

    # 3. 核心池数据
    print("\n>> 加载核心池...")
    core_pool = load_core_pool()
    print(f"  核心池: {len(core_pool)} 只")

    # 4. 获取核心池技术面
    print("\n>> 核心池技术面...")
    core_tech = {}
    for stock in core_pool:
        code = stock["code"]
        tech = get_technical(code, 60)
        core_tech[code] = tech
        if "error" not in tech:
            price = tech.get("current_price", 0)
            ma20 = tech.get("ma", {}).get("MA20", 0)
            above = tech.get("above_ma20", False)
            tag = "[OK]" if above else "[NO]"
            print(f"  {stock['name']}({code}): {price} | MA20={ma20} {tag}")

    # 5. 风控状态
    weekly_buys = get_weekly_buy_count()
    holding_count = len([h for h in holdings
                        if str(h.get("股票", "")).strip() not in ["", "—", "空仓"]
                        and str(h.get("代码", "")).strip() not in ["", "—"]])

    # 6. 生成明日计划
    print("\n>> 生成明日计划...")
    tomorrow = next_trading_day()
    tomorrow_str = tomorrow.strftime("%Y-%m-%d")

    plan = generate_tomorrow_plan(
        market=market,
        holdings=holdings,
        core_pool=core_pool,
        core_tech=core_tech,
        weekly_buys=weekly_buys,
        holding_count=holding_count,
    )

    # 7. 创建明日日志文件
    print(f"\n>> 创建明日日志: {tomorrow_str}.md")
    create_tomorrow_journal(tomorrow_str, plan)

    print(f"\n{SEP}")
    print(f"[DONE] 收盘流程完成 -> 02-日志/{tomorrow_str}.md")
    print(SEP)

    return plan


def generate_tomorrow_plan(market, holdings, core_pool, core_tech, weekly_buys, holding_count):
    """生成明日计划内容"""
    sh = market.get("上证指数", {})
    cy = market.get("创业板指", {})
    summary = market.get("_summary", {})

    lines = []
    lines.append("## 📋 明日计划（收盘后自动生成）")
    lines.append("")

    # 大盘状态
    lines.append("### 大盘状态")
    sh_status = f"{'✅' if sh.get('above_MA20') else '❌'}"
    cy_status = f"{'✅' if cy.get('above_MA20') else '❌'}"
    lines.append(f"- 上证：{sh.get('close', '—')} {sh_status}（vs 20日线 {sh.get('MA20', '—')} / 60日线 {sh.get('MA60', '—')}）| 60日线下方{sh.get('below_MA60_days', 0)}天")
    lines.append(f"- 创业板：{cy.get('close', '—')} {cy_status}（vs 20日线 {cy.get('MA20', '—')} / 60日线 {cy.get('MA60', '—')}）| 60日线下方{cy.get('below_MA60_days', 0)}天")
    status = summary.get('status', '未知')
    emoji = "🟢" if status == "可买入" else "🟡" if "谨慎" in status else "🔴"
    lines.append(f"- **判定：{emoji} {status}**")
    lines.append("")

    # 风控状态
    lines.append("### 风控状态")
    lines.append("| 项目 | 状态 |")
    lines.append("|------|------|")
    buy_ok = "✅" if weekly_buys < MAX_BUY_PER_WEEK else "❌ 已满"
    hold_ok = "✅" if holding_count < MAX_HOLDING_STOCKS else "❌ 已满"
    lines.append(f"| 本周买入次数 | {weekly_buys}/{MAX_BUY_PER_WEEK} {buy_ok} |")
    lines.append(f"| 持仓数 | {holding_count}/{MAX_HOLDING_STOCKS} {hold_ok} |")
    lines.append(f"| 冷却中 | 否 |")
    lines.append(f"| 大盘60日线下方天数 | 上证{sh.get('below_MA60_days', 0)}天 / 创业板{cy.get('below_MA60_days', 0)}天 |")
    lines.append("")

    # 条件单清单
    lines.append("### 明日条件单清单")
    lines.append("")
    active_holdings = [h for h in holdings
                      if str(h.get("股票", "")).strip() not in ["", "—", "空仓"]
                      and str(h.get("代码", "")).strip() not in ["", "—"]]
    if active_holdings:
        lines.append("| 股票 | 类型 | 触发价 | 数量 | 说明 |")
        lines.append("|------|------|--------|------|------|")
        for h in active_holdings:
            name = h.get("股票", "")
            stop = h.get("止损价", "")
            abs_stop = h.get("绝对止损", "")
            tp1 = h.get("止盈1(8%)", "")
            shares = h.get("持有股数", "")
            lines.append(f"| {name} | 止损 | {stop} | {shares} | 动态止损 |")
            lines.append(f"| {name} | 绝对止损 | {abs_stop} | {shares} | -7%无条件 |")
            lines.append(f"| {name} | 止盈(第一批) | {tp1} | 1/3仓位 | +8%卖1/3 |")
    else:
        lines.append("无持仓，无需挂条件单。")
    lines.append("")

    # 核心池买入条件检查
    lines.append("### 核心池买入条件检查")
    lines.append("")
    if core_pool:
        lines.append("| 股票 | 代码 | 今日收盘 | 20日线 | 站上20线 | 60线方向 | 成交量 | 是否可操作 |")
        lines.append("|------|------|---------|--------|---------|---------|--------|-----------|")
        can_buy = summary.get("can_buy", False)
        for stock in core_pool:
            code = stock["code"]
            name = stock["name"]
            tech = core_tech.get(code, {})
            if "error" in tech:
                lines.append(f"| {name} | {code} | 数据获取失败 | — | — | — | — | ❌ |")
                continue
            price = tech.get("current_price", "—")
            ma20 = tech.get("ma", {}).get("MA20", "—")
            above = tech.get("above_ma20", False)
            ma60_dir = tech.get("ma60_direction", "—")
            vol_score = tech.get("volume_analysis", {}).get("score", 0)

            if not can_buy:
                operable = "🔴 大盘不允许"
            elif not above:
                operable = "🔴 未站上20日线"
            else:
                operable = "🟡 等回踩买入"

            lines.append(f"| {name} | {code} | {price} | {ma20} | {'✅' if above else '❌'} | {ma60_dir} | {vol_score} | {operable} |")
    else:
        lines.append("核心池为空，请先筛选。")
    lines.append("")

    # 持仓关注事项
    lines.append("### 持仓关注事项")
    lines.append("")
    if active_holdings:
        lines.append("| 股票 | 关注点 | 说明 |")
        lines.append("|------|--------|------|")
        for h in active_holdings:
            name = h.get("股票", "")
            lines.append(f"| {name} | 止损/止盈 | 参照上方条件单清单 |")
    else:
        lines.append("无持仓。")
    lines.append("")

    # 明日重点
    lines.append("### 明日重点")
    points = []
    if "清仓" in status:
        points.append("大盘清仓信号，不买入，只观察")
    elif "谨慎" in status:
        points.append("大盘谨慎状态，如买入需减半金额")
    else:
        points.append("大盘可操作，关注核心池买入机会")

    if weekly_buys >= MAX_BUY_PER_WEEK:
        points.append("本周买入次数已满，不可再买")

    points.append("盘前照条件单清单挂单，确认后打勾")

    for i, p in enumerate(points, 1):
        lines.append(f"{i}. {p}")

    return "\n".join(lines)


def create_tomorrow_journal(date_str, plan_content):
    """创建明日日志文件"""
    JOURNAL_DIR.mkdir(parents=True, exist_ok=True)
    filepath = JOURNAL_DIR / f"{date_str}.md"

    # 如果文件已存在，只更新明日计划部分
    if filepath.exists():
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        # 替换明日计划部分
        marker = "## 📋 明日计划"
        if marker in content:
            idx = content.index(marker)
            content = content[:idx] + plan_content
        else:
            content += "\n---\n\n" + plan_content
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  已更新: {filepath.name}")
        return

    # 创建新文件
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    weekday = weekday_names[dt.weekday()]

    journal = f"""---
date: {date_str}
type: journal
tags: [交易日志]
total_asset: 
daily_pnl: 
trades: 0
buy_count_this_week: 
mood: 
market_status: 
---

# {date_str}（{weekday}）交易日志

## 盘前确认（8:30-9:15）

> 以下内容由昨日收盘的"明日计划"自动生成，你只需确认执行。

- [ ] 看隔夜新闻标题（5分钟，只看核心池+观察池相关）
- [ ] 朗读心理检查表
- [ ] 确认所有条件单已挂好（参照下方明日计划的条件单清单）
- [ ] 确认条件单价格正确
- [ ] 如有异常情况，记录在下方

**盘前备注：**


## 午休检查（12:00-12:30）

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
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(journal)
    print(f"  已创建: {filepath.name}")


# ============================================================
# 盘前流程 morning_routine
# ============================================================

def morning_routine():
    """
    盘前流程（8:30 执行）
    读取昨晚生成的今日日志，补充实时大盘数据，输出盘前摘要
    """
    SEP = "-" * 52
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    print(SEP)
    print(f"[MORNING] 盘前流程 {ts}")
    print(SEP)

    today_str = datetime.now().strftime("%Y-%m-%d")
    journal_path = JOURNAL_DIR / f"{today_str}.md"

    if not journal_path.exists():
        print(f"[WARN] 今日日志不存在: {today_str}.md")
        print("  正在补充执行 evening 流程...")
        evening_routine()
        return

    # 获取实时大盘数据
    print("\n>> 大盘数据")
    market = get_market_status()
    sh = market.get("上证指数", {})
    cy = market.get("创业板指", {})
    summary = market.get("_summary", {})

    rt_tag = "[实时]" if sh.get("realtime") else "[昨日]"
    sh_chg = f"{sh['change_pct']:+.2f}%" if sh.get("change_pct") is not None else "—"
    cy_chg = f"{cy['change_pct']:+.2f}%" if cy.get("change_pct") is not None else "—"
    sh_ma = "OK" if sh.get("above_MA20") else "NO"
    cy_ma = "OK" if cy.get("above_MA20") else "NO"

    print(f"  上证 {rt_tag} {sh.get('close')} ({sh_chg}) | MA20=[{sh_ma}] | {sh.get('below_MA60_days', 0)}天<MA60")
    print(f"  创业板 {rt_tag} {cy.get('close')} ({cy_chg}) | MA20=[{cy_ma}] | {cy.get('below_MA60_days', 0)}天<MA60")
    print(f"  -> 判定: {summary.get('status', 'UNKNOWN')}")

    # 风控
    weekly_buys = get_weekly_buy_count()
    print(f"\n>> 风控状态")
    print(f"  本周买入: {weekly_buys}/{MAX_BUY_PER_WEEK}")

    print(f"\n>> 待办")
    print(f"  1. 打开 Obsidian 查看今日日志底部的明日计划")
    print(f"  2. 照条件单清单挂单，确认后打勾")

    print(f"\n{SEP}")
    print(f"[DONE] 盘前流程完成")
    print(SEP)


# ============================================================
# 午休检查 noon_check
# ============================================================

def noon_check():
    """午休检查（12:00 执行）"""
    SEP = "-" * 52
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    print(SEP)
    print(f"[NOON] 午休检查 {ts}")
    print(SEP)

    weekly_buys = get_weekly_buy_count()
    remaining = MAX_BUY_PER_WEEK - weekly_buys

    print(f"\n>> 风控")
    print(f"  本周剩余买入: {remaining}/{MAX_BUY_PER_WEEK}  {'[FULL]' if remaining <= 0 else '[OK]'}")

    # 检查持仓加仓条件
    portfolio = load_portfolio_data()
    holdings = [h for h in portfolio.get("holdings", [])
                if str(h.get("股票", "")).strip() not in ["", "—", "空仓"]
                and str(h.get("代码", "")).strip() not in ["", "—"]]

    if holdings:
        print(f"\n>> 持仓({len(holdings)}只)加仓检查")
        for h in holdings:
            print(f"  - {h.get('股票', '')}: 参照明日计划持仓关注事项")
    else:
        print("\n>> 持仓: 空仓，午休无需操作")

    print(f"\n{SEP}")
    print(f"[DONE] 午休检查完成")
    print(SEP)


# ============================================================
# 周复盘 weekly_review
# ============================================================

def weekly_review():
    """周复盘（周日执行）"""
    print("=" * 60)
    print(f"📝 周复盘 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    from parser import parse_journal_dir

    # 统计本周数据
    print("\n→ 统计本周交易数据...")
    stats = parse_journal_dir(str(JOURNAL_DIR), 7)

    print(f"  交易日: {stats['count']} 天")
    print(f"  总盈亏: ¥{stats['total_pnl']}")
    print(f"  盈利天数: {stats['win_days']} | 亏损天数: {stats['loss_days']}")
    print(f"  胜率: {stats['win_rate']}")
    print(f"  总交易次数: {stats['total_trades']}")

    # 生成周复盘 MD 文件
    now = datetime.now()
    week_num = now.isocalendar()[1]
    year = now.isocalendar()[0]
    week_str = f"{year}-W{week_num:02d}"
    monday = now - timedelta(days=now.weekday())
    friday = monday + timedelta(days=4)

    review_dir = DATA_DIR / "03-复盘" / "周"
    review_dir.mkdir(parents=True, exist_ok=True)
    review_path = review_dir / f"{week_str}.md"

    print(f"\n→ 生成周复盘文件: {week_str}.md")

    # 加载核心池
    core_pool = load_core_pool()

    # 刷新核心池技术面
    print("\n→ 刷新核心池技术面...")
    core_alerts = []
    core_rows = []
    for stock in core_pool:
        code = stock["code"]
        name = stock["name"]
        tech = get_technical(code, 60)
        if "error" not in tech:
            above = tech.get("above_ma20", False)
            ma60_dir = tech.get("ma60_direction", "—")
            price = tech.get("current_price", "—")
            print(f"  {name}({code}): {'✅' if above else '❌'}20日线 | 60日线{ma60_dir}")
            if not above:
                core_alerts.append(f"⚠️ {name} 已跌破20日线，考虑剔除")
            core_rows.append(f"| {name} | {code} | — | — | {'✅' if above else '❌'}20日线, 60日线{ma60_dir} |")
        else:
            core_rows.append(f"| {name} | {code} | — | — | 数据获取失败 |")

    # 写入周复盘文件
    review_content = f"""---
date: {now.strftime('%Y-%m-%d')}
type: weekly_review
tags: [周复盘]
week: {week_str}
total_asset_start: 
total_asset_end: 
weekly_pnl: {stats['total_pnl']}
buy_count: {stats['total_trades']}
first_buy_count: 
add_count: 
win_count: {stats['win_days']}
loss_count: {stats['loss_days']}
---

# {week_str} 周复盘（{monday.strftime('%m/%d')} - {friday.strftime('%m/%d')}）

## 本周概览（自动统计）

| 项目 | 数据 |
|------|------|
| 周初资产 | ¥（手动填写） |
| 周末资产 | ¥（手动填写） |
| 本周盈亏 | ¥{stats['total_pnl']}（自动统计） |
| 收益率 | %（自动计算，需填资产后） |
| 主动买入次数 | {stats['total_trades']} 次（自动统计） |
| 盈利天数 / 亏损天数 | {stats['win_days']} / {stats['loss_days']}（自动统计） |
| 胜率 | {stats['win_rate']}（自动统计） |

> 周初/周末资产需手动填写（券商APP截图），其余由系统从日志自动统计。

## 本周交易明细（自动从日志提取）

| 日期 | 股票 | 操作 | 价格 | 数量 | 盈亏 | 备注 |
|------|------|------|------|------|------|------|
"""

    # 从日志中提取交易记录
    if stats.get("journals"):
        has_trades = False
        for j in stats["journals"]:
            if j.get("trades", 0) > 0:
                has_trades = True
                review_content += f"| {j.get('_date', '')} | — | — | — | — | ¥{j.get('daily_pnl', 0)} | 详见日志 |\n"
        if not has_trades:
            review_content += "| — | — | — | — | — | — | 本周无交易 |\n"
    else:
        review_content += "| — | — | — | — | — | — | 本周无交易 |\n"

    review_content += f"""
## 规则执行检查

- [ ] 是否遵守了所有买入规则？
- [ ] 是否遵守了止损纪律？
- [ ] 加仓操作是否合理？
- [ ] 是否触发过冷却机制？
- [ ] 是否有情绪化交易？

**违反规则记录：**


## 下周计划（自动从核心池提取）

### 核心池（{len(core_pool)}只）

| 股票 | 代码 | 打分 | 计划操作 | 技术面状态 |
|------|------|------|----------|-----------|
"""

    if core_rows:
        review_content += "\n".join(core_rows) + "\n"
    else:
        review_content += "| — | — | — | — | 核心池为空 |\n"

    if core_alerts:
        review_content += "\n**核心池预警：**\n"
        for alert in core_alerts:
            review_content += f"- {alert}\n"

    review_content += f"""
### 下周买入计划（最多2次）

1. 
2. 

## 下周改进点（只写1条）

> 

"""

    with open(review_path, 'w', encoding='utf-8') as f:
        f.write(review_content)

    print(f"\n  ✅ 周复盘已生成: 03-复盘/周/{week_str}.md")

    print("\n" + "=" * 60)
    print("✅ 周复盘完成")
    print("  → 请在 Obsidian 中填写周初/周末资产和改进点")
    print("  → 对 Kiro 说'帮我筛选下周核心池'更新选股")
    print("=" * 60)


# ============================================================
# 交易记录 record_trade
# ============================================================

def record_trade(action, name, code, price, shares, trade_type=""):
    """
    记录交易并自动更新 portfolio.md 和当日日志

    参数:
        action: "buy" 或 "sell"
        name: 股票名称
        code: 股票代码
        price: 成交价格
        shares: 成交股数
        trade_type: 买入类型(首次买入/加仓) 或 卖出类型(止盈/止损/时间止损/清仓)
    """
    price = float(price)
    shares = int(shares)
    amount = round(price * shares, 2)
    today = datetime.now().strftime("%Y-%m-%d")
    now_time = datetime.now().strftime("%H:%M")

    print("=" * 60)
    print(f"📝 记录交易 {today} {now_time}")
    print("=" * 60)

    if not trade_type:
        trade_type = "首次买入" if action == "buy" else "止盈"

    print(f"\n  {'买入' if action == 'buy' else '卖出'}: {name}({code})")
    print(f"  价格: {price} | 股数: {shares} | 金额: ¥{amount:,.0f}")
    print(f"  类型: {trade_type}")

    # 读取 portfolio
    if not PORTFOLIO_PATH.exists():
        print("  ❌ portfolio.md 不存在")
        return

    with open(PORTFOLIO_PATH, 'r', encoding='utf-8') as f:
        content = f.read()

    portfolio = load_portfolio_data()
    holdings = portfolio.get("holdings", [])
    meta = portfolio.get("meta", {})
    total_capital = float(meta.get("total_capital", TOTAL_CAPITAL))

    if action == "buy":
        content = _record_buy(content, holdings, name, code, price, shares, amount, trade_type, total_capital)
    elif action == "sell":
        content = _record_sell(content, holdings, name, code, price, shares, amount, trade_type, total_capital)

    # 更新日期
    content = re.sub(r'updated_at: \d{4}-\d{2}-\d{2}', f'updated_at: {today}', content)
    content = re.sub(r'^date: \d{4}-\d{2}-\d{2}', f'date: {today}', content, flags=re.MULTILINE)

    # 追加交易记录到本周交易记录表
    trade_row = f"| {today} | {'买入' if action == 'buy' else '卖出'} | {name} | {price} | {shares} | ¥{amount:,.0f} | {trade_type} | — |"

    # 找到"本周交易记录"表格，在最后一行数据后插入
    if "本周无交易" in content:
        content = content.replace(
            f"| — | — | — | — | — | — | — | 本周无交易 |",
            trade_row
        )
    else:
        # 在交易记录表的最后一行后追加
        lines = content.split('\n')
        insert_idx = None
        in_trade_table = False
        for i, line in enumerate(lines):
            if "本周交易记录" in line:
                in_trade_table = True
            if in_trade_table and line.startswith('|') and not line.startswith('|---') and '日期' not in line:
                insert_idx = i
        if insert_idx:
            lines.insert(insert_idx + 1, trade_row)
            content = '\n'.join(lines)

    # 更新买入次数
    weekly_buys = get_weekly_buy_count()
    if action == "buy":
        weekly_buys += 1
    content = re.sub(r'\*\*本周买入次数：\*\* \d+/2', f'**本周买入次数：** {weekly_buys}/2', content)

    # 写回
    with open(PORTFOLIO_PATH, 'w', encoding='utf-8') as f:
        f.write(content)

    # 追加到当日日志
    _append_to_journal(today, now_time, name, code, action, price, shares, amount, trade_type)

    print(f"\n  ✅ portfolio.md 已更新")
    print(f"  ✅ 当日日志已追加")
    print("=" * 60)


def _record_buy(content, holdings, name, code, price, shares, amount, trade_type, total_capital):
    """处理买入记录"""
    # 检查是否已有持仓
    existing = None
    for h in holdings:
        h_code = str(h.get("代码", "")).strip()
        if h_code.isdigit():
            h_code = h_code.zfill(6)
        if h_code == code:
            existing = h
            break

    if existing and trade_type == "加仓":
        # 加仓：更新平均成本和股数
        old_cost = float(existing.get("平均成本", 0) or 0)
        old_shares = int(float(existing.get("持有股数", 0) or 0))
        add_count = int(float(existing.get("加仓次数", 0) or 0)) + 1

        new_shares = old_shares + shares
        new_cost = round((old_cost * old_shares + price * shares) / new_shares, 2)
        new_value = round(price * new_shares, 2)
        first_buy = float(existing.get("首次买入价", 0) or 0)
        if first_buy <= 0:
            first_buy = old_cost

        # 重算止损止盈
        stops = calc_stop_loss(new_cost)
        tp = calc_take_profit(first_buy)

        # 替换持仓行
        old_name = str(existing.get("股票", "")).strip()
        pattern = rf'\| {re.escape(old_name)} \|[^\n]*\|'
        new_row = (f"| {name} | {code} | {first_buy} | {add_count} | {new_cost} | "
                  f"{new_shares} | ¥{new_value:,.0f} | {stops['stop_loss']} | "
                  f"{stops['absolute_stop']} | {tp['batch_1_price']} | 持有中 |")
        content = re.sub(pattern, new_row, content)

        print(f"\n  加仓更新: 均价 {old_cost}→{new_cost} | 股数 {old_shares}→{new_shares}")
        print(f"  新止损: {stops['stop_loss']} | 绝对止损: {stops['absolute_stop']}")

    else:
        # 首次买入：新增持仓行
        stops = calc_stop_loss(price)
        tp = calc_take_profit(price)

        new_row = (f"| {name} | {code} | {price} | 0 | {price} | "
                  f"{shares} | ¥{amount:,.0f} | {stops['stop_loss']} | "
                  f"{stops['absolute_stop']} | {tp['batch_1_price']} | 持有中 |")

        # 替换空仓占位行或在表格末尾追加
        if "| — | — | — | — | — | — | — | — | — | — | 空仓 |" in content:
            content = content.replace("| — | — | — | — | — | — | — | — | — | — | 空仓 |", new_row)
        else:
            # 在持仓明细表最后一行后追加
            lines = content.split('\n')
            in_detail = False
            insert_idx = None
            for i, line in enumerate(lines):
                if "持仓明细" in line:
                    in_detail = True
                if in_detail and line.startswith('|') and not line.startswith('|---') and '股票' not in line:
                    insert_idx = i
            if insert_idx:
                lines.insert(insert_idx + 1, new_row)
                content = '\n'.join(lines)

        print(f"\n  新建持仓: {name}({code}) {shares}股 @ {price}")
        print(f"  止损: {stops['stop_loss']} | 绝对止损: {stops['absolute_stop']} | 止盈: {tp['batch_1_price']}")

    return content


def _record_sell(content, holdings, name, code, price, shares, amount, trade_type, total_capital):
    """处理卖出记录"""
    existing = None
    for h in holdings:
        h_code = str(h.get("代码", "")).strip()
        # 处理 int 类型的代码（如 2487 → "002487"）
        if h_code.isdigit():
            h_code = h_code.zfill(6)
        if h_code == code:
            existing = h
            break

    if not existing:
        print(f"  ⚠️ 未找到 {name}({code}) 的持仓记录")
        return content

    old_shares = int(float(existing.get("持有股数", 0) or 0))
    remaining = old_shares - shares
    old_cost = float(existing.get("平均成本", 0) or 0)

    # 计算盈亏
    pnl = round((price - old_cost) * shares, 2)
    pnl_pct = round((price - old_cost) / old_cost * 100, 2) if old_cost > 0 else 0
    print(f"\n  卖出盈亏: ¥{pnl:+,.0f} ({pnl_pct:+.1f}%)")

    old_name = str(existing.get("股票", "")).strip()
    pattern = rf'\| {re.escape(old_name)} \|[^\n]*\|'

    if remaining <= 0:
        # 全部卖出，标记清仓
        new_row = "| — | — | — | — | — | — | — | — | — | — | 空仓 |"
        content = re.sub(pattern, new_row, content)
        print(f"  已清仓 {name}")
    else:
        # 部分卖出，更新股数和市值
        new_value = round(price * remaining, 2)
        first_buy = float(existing.get("首次买入价", 0) or 0)
        add_count = existing.get("加仓次数", 0)
        stops = calc_stop_loss(old_cost)
        tp = calc_take_profit(first_buy if first_buy > 0 else old_cost)

        new_row = (f"| {name} | {code} | {first_buy if first_buy > 0 else '—'} | {add_count} | {old_cost} | "
                  f"{remaining} | ¥{new_value:,.0f} | {stops['stop_loss']} | "
                  f"{stops['absolute_stop']} | {tp['batch_1_price']} | 持有中 |")
        content = re.sub(pattern, new_row, content)
        print(f"  剩余: {remaining}股 | 市值: ¥{new_value:,.0f}")

    return content


def _append_to_journal(date_str, time_str, name, code, action, price, shares, amount, trade_type):
    """追加交易记录到当日日志"""
    journal_path = JOURNAL_DIR / f"{date_str}.md"
    if not journal_path.exists():
        return

    with open(journal_path, 'r', encoding='utf-8') as f:
        content = f.read()

    action_text = "买入" if action == "buy" else "卖出"
    trade_row = f"| {time_str} | {name} | {action_text} | {price} | {shares} | ¥{amount:,.0f} | {trade_type} |"

    # 找到今日交易表格，替换空行或追加
    if content.count("|  |  |  |  |  |  |") > 0:
        content = content.replace("|  |  |  |  |  |  |", trade_row, 1)
    else:
        # 在交易表格最后追加
        lines = content.split('\n')
        in_trade = False
        insert_idx = None
        for i, line in enumerate(lines):
            if "今日交易" in line:
                in_trade = True
            if in_trade and line.startswith('|') and not line.startswith('|---') and '时间' not in line:
                insert_idx = i
        if insert_idx:
            lines.insert(insert_idx + 1, trade_row)
            content = '\n'.join(lines)

    # 更新 trades 计数
    fm_match = re.search(r'trades: (\d+)', content)
    if fm_match:
        old_count = int(fm_match.group(1))
        content = content.replace(f'trades: {old_count}', f'trades: {old_count + 1}')

    with open(journal_path, 'w', encoding='utf-8') as f:
        f.write(content)


# ============================================================
# 主入口
# ============================================================

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == "evening":
        evening_routine()
    elif command == "morning":
        morning_routine()
    elif command == "noon":
        noon_check()
    elif command == "weekly":
        weekly_review()
    elif command == "record":
        if len(sys.argv) < 6:
            print("用法: python daily_routine.py record buy/sell <名称> <代码> <价格> <股数> [类型]")
            sys.exit(1)
        action = sys.argv[2]  # buy / sell
        name = sys.argv[3]
        code = sys.argv[4]
        price = sys.argv[5]
        shares = sys.argv[6]
        trade_type = sys.argv[7] if len(sys.argv) > 7 else ""
        record_trade(action, name, code, price, shares, trade_type)
    else:
        print(f"未知命令: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
