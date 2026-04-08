#!/usr/bin/env python3
"""
A股交易系统 v1.4 - 历史回测引擎

用法:
  python backtest.py <股票代码> [开始日期] [结束日期] [--aggressive]
  python backtest.py 002487 20250101 20260401              # 保守模式（默认）
  python backtest.py 002487 20250101 20260401 --aggressive  # 激进模式
  python backtest.py 002487                                 # 默认回测最近1年

模式说明:
  保守模式（默认）：首次25000，加仓25000，初始资金75000
  激进模式：首次25000，加仓35000/45000金字塔，初始资金125000
"""

import sys
import json
import warnings
from datetime import datetime, timedelta
from dataclasses import dataclass, field

warnings.filterwarnings("ignore")

try:
    import akshare as ak
    import pandas as pd
except ImportError:
    print("请先安装: pip install akshare pandas")
    sys.exit(1)

# 导入计算模块
sys.path.insert(0, str(__import__('pathlib').Path(__file__).parent))
from calculator import TOTAL_CAPITAL, FIRST_BUY_MIN, FIRST_BUY_MAX


# ============ 配置 ============

BUY_AMOUNT = 25000          # 首次买入金额
BUY_AMOUNT_ADD1 = 35000     # 激进模式：加仓1金额
BUY_AMOUNT_ADD2 = 45000     # 激进模式：加仓2金额
STOP_LOSS_PCT = 0.04        # 4% 止损
ABSOLUTE_STOP_PCT = 0.07    # 7% 绝对止损
# 建议A：+15%卖1/4，回撤5%卖1/3，回撤8%清仓
TAKE_PROFIT_1_PCT = 0.15     # 15% 第一批止盈（原10%，放宽）
TAKE_PROFIT_2_DRAWDOWN = 0.05  # 从最高点回撤5% 卖1/3
TAKE_PROFIT_3_DRAWDOWN = 0.08  # 从最高点回撤8% 清仓
TIME_STOP_DAYS = 5          # 时间止损天数
TIME_STOP_AMPLITUDE = 0.04  # 时间止损振幅阈值
ADD_TRIGGER_1 = 0.07        # 第一次加仓触发涨幅
ADD_TRIGGER_2 = 0.12        # 第二次加仓触发涨幅
BUY_CONFIRM_DAYS = 2        # 买入确认天数
COOLDOWN_DAYS = 3           # 清仓后3个交易日内不再买入
CONSECUTIVE_STOP_LIMIT = 2  # 连续止损N次后进入长冷却
LONG_COOLDOWN_DAYS = 15     # 连续止损后冷却天数
MAX_ABOVE_MA20_PCT = 0.08   # 买入时离20日线最大距离8%（从5%放宽）
MAX_DAY_GAIN = 0.05         # 当日涨幅超过5%不买
MAX_3DAY_GAIN = 0.10        # 近3日累计涨幅超过10%不买
LIMIT_UP_PCT = 0.095        # 主板涨停判定阈值（默认，会根据代码自动调整）


def get_limit_up_pct(code: str) -> float:
    """根据股票代码判断涨停阈值"""
    code = str(code).strip()
    if code.startswith("300") or code.startswith("301"):  # 创业板
        return 0.195
    elif code.startswith("688") or code.startswith("689"):  # 科创板
        return 0.195
    else:  # 主板
        return 0.095


@dataclass
class Position:
    """持仓"""
    code: str
    name: str
    first_buy_price: float
    avg_cost: float
    shares: int
    buy_date: str
    add_count: int = 0
    highest_price: float = 0
    sold_batch_1: bool = False
    sold_batch_2: bool = False
    _reduced: bool = False  # 大盘20日线减仓标记


@dataclass
class Trade:
    """交易记录"""
    date: str
    action: str       # buy / sell
    price: float
    shares: int
    amount: float
    trade_type: str   # 首次买入/加仓/止盈1/止盈2/止盈3/止损/绝对止损/时间止损/大盘止损
    pnl: float = 0
    pnl_pct: float = 0


@dataclass
class BacktestResult:
    """回测结果"""
    code: str
    name: str
    start_date: str
    end_date: str
    total_trades: int = 0
    win_trades: int = 0
    loss_trades: int = 0
    total_pnl: float = 0
    max_drawdown: float = 0
    max_profit: float = 0
    trades: list = field(default_factory=list)
    equity_curve: list = field(default_factory=list)


def get_backtest_data(code: str, start_date: str, end_date: str) -> tuple:
    """获取回测所需的股票和指数数据"""
    # 提前多取60天用于计算均线
    start_dt = datetime.strptime(start_date, "%Y%m%d") - timedelta(days=90)
    early_start = start_dt.strftime("%Y%m%d")

    print(f"→ 获取 {code} 日线数据...")
    stock_df = None
    try:
        stock_df = ak.stock_zh_a_hist(
            symbol=code, period="daily",
            start_date=early_start, end_date=end_date, adjust="qfq"
        )
    except Exception:
        pass

    if stock_df is None or stock_df.empty:
        # fallback 到新浪接口
        try:
            import time
            time.sleep(0.5)
            prefix = "sh" if code.startswith("6") else "sz"
            stock_df = ak.stock_zh_a_daily(symbol=f"{prefix}{code}", adjust="qfq")
            if stock_df is not None:
                col_map = {"date": "日期", "open": "开盘", "close": "收盘",
                           "high": "最高", "low": "最低", "volume": "成交量"}
                stock_df = stock_df.rename(columns=col_map)
                stock_df["涨跌幅"] = stock_df["收盘"].pct_change() * 100
                stock_df["日期"] = pd.to_datetime(stock_df["日期"])
                start_dt2 = pd.to_datetime(early_start)
                end_dt2 = pd.to_datetime(end_date)
                stock_df = stock_df[(stock_df["日期"] >= start_dt2) & (stock_df["日期"] <= end_dt2)]
                stock_df["日期"] = stock_df["日期"].dt.strftime("%Y-%m-%d")
        except Exception as e:
            print(f"  ❌ 无法获取股票数据: {e}")
            return None, None

    if stock_df is None or stock_df.empty:
        return None, None

    # 计算均线
    stock_df["MA20"] = stock_df["收盘"].rolling(20).mean()
    stock_df["MA60"] = stock_df["收盘"].rolling(60).mean()
    stock_df["VOL_MA20"] = stock_df["成交量"].rolling(20).mean()

    # 获取上证指数
    print(f"→ 获取上证指数数据...")
    index_df = None
    try:
        index_df = ak.stock_zh_index_daily(symbol="sh000001")
        if index_df is not None:
            index_df = index_df.sort_values("date")
            index_df["MA20"] = index_df["close"].rolling(20).mean()
            index_df["MA60"] = index_df["close"].rolling(60).mean()
            # 连续在60日线下方天数
            below_60 = []
            count = 0
            for _, row in index_df.iterrows():
                if pd.notna(row["MA60"]) and row["close"] < row["MA60"]:
                    count += 1
                else:
                    count = 0
                below_60.append(count)
            index_df["below_MA60_days"] = below_60
            index_df["date"] = pd.to_datetime(index_df["date"]).dt.strftime("%Y-%m-%d")
    except Exception as e:
        print(f"  ⚠️ 无法获取指数数据: {e}")

    return stock_df, index_df


def run_backtest(code: str, start_date: str, end_date: str, aggressive: bool = False) -> BacktestResult:
    """运行回测
    aggressive: True=激进模式（金字塔加仓+更高资金利用率）
    """
    stock_df, index_df = get_backtest_data(code, start_date, end_date)
    if stock_df is None:
        print("❌ 无法获取数据，回测终止")
        return None

    # 过滤到回测区间
    stock_df["日期_dt"] = pd.to_datetime(stock_df["日期"])
    start_dt = pd.to_datetime(start_date)
    bt_df = stock_df[stock_df["日期_dt"] >= start_dt].copy()

    if bt_df.empty:
        print("❌ 回测区间无数据")
        return None

    # 构建指数查找字典
    index_dict = {}
    if index_df is not None:
        for _, row in index_df.iterrows():
            index_dict[row["date"]] = {
                "close": row["close"],
                "MA20": row["MA20"],
                "MA60": row["MA60"],
                "above_MA20": row["close"] > row["MA20"] if pd.notna(row["MA20"]) else True,
                "below_MA60_days": row.get("below_MA60_days", 0),
            }

    name = code  # 简化，不查名称
    result = BacktestResult(code=code, name=name, start_date=start_date, end_date=end_date)
    limit_up_pct = get_limit_up_pct(code)  # 根据代码判断涨停阈值

    position = None
    init_multiplier = 5 if aggressive else 3
    cash = float(BUY_AMOUNT * init_multiplier)
    initial_cash = cash
    peak_equity = cash
    last_sell_date = None
    consecutive_stops = 0
    long_cooldown_until = None

    mode_str = "激进" if aggressive else "保守"
    print(f"\n→ 开始回测 {code}: {start_date} ~ {end_date} [{mode_str}模式]")
    print(f"  初始资金: ¥{cash:,.0f}")
    print("-" * 60)

    for i in range(len(bt_df)):
        row = bt_df.iloc[i]
        date = str(row["日期"])
        close = float(row["收盘"])
        high = float(row["最高"])
        low = float(row["最低"])
        ma20 = float(row["MA20"]) if pd.notna(row["MA20"]) else None
        ma60 = float(row["MA60"]) if pd.notna(row["MA60"]) else None
        vol = float(row["成交量"]) if pd.notna(row["成交量"]) else 0
        vol_ma20 = float(row["VOL_MA20"]) if pd.notna(row["VOL_MA20"]) else 0

        # 60日线方向（对比5天前）
        ma60_up = True
        if i >= 5 and ma60:
            prev_ma60 = bt_df.iloc[i - 5]["MA60"]
            if pd.notna(prev_ma60):
                ma60_up = ma60 >= float(prev_ma60)

        # 大盘状态
        idx_data = index_dict.get(date, {})
        market_ok = idx_data.get("above_MA20", True)
        market_clear = idx_data.get("below_MA60_days", 0) >= 3

        # 当前权益
        equity = cash + (position.shares * close if position else 0)
        result.equity_curve.append({"date": date, "equity": round(equity, 2)})

        # 最大回撤
        if equity > peak_equity:
            peak_equity = equity
        drawdown = (peak_equity - equity) / peak_equity if peak_equity > 0 else 0
        if drawdown > result.max_drawdown:
            result.max_drawdown = drawdown

        # === 有持仓时的检查 ===
        if position:
            # 更新最高价
            if high > position.highest_price:
                position.highest_price = high

            # 1. 大盘清仓信号（连续3日在60日线下方）
            if market_clear:
                pnl = (close - position.avg_cost) * position.shares
                trade = Trade(date=date, action="sell", price=close, shares=position.shares,
                             amount=close * position.shares, trade_type="大盘止损(60日线)",
                             pnl=pnl, pnl_pct=(close - position.avg_cost) / position.avg_cost * 100)
                result.trades.append(trade)
                cash += close * position.shares
                position = None
                last_sell_date = date  # 记录清仓日期
                consecutive_stops += 1  # 大盘止损计入连续止损
                if consecutive_stops >= CONSECUTIVE_STOP_LIMIT:
                    long_cooldown_until = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=LONG_COOLDOWN_DAYS)).strftime("%Y-%m-%d")
                continue

            # 1.5 大盘跌破20日线 → 减仓到50%（卖一半）
            if not market_ok and position.shares > 0 and not getattr(position, '_reduced', False):
                sell_shares = position.shares // 2
                if sell_shares > 0:
                    pnl = (close - position.avg_cost) * sell_shares
                    trade = Trade(date=date, action="sell", price=close, shares=sell_shares,
                                 amount=close * sell_shares, trade_type="大盘减仓(20日线)",
                                 pnl=pnl, pnl_pct=(close - position.avg_cost) / position.avg_cost * 100)
                    result.trades.append(trade)
                    cash += close * sell_shares
                    position.shares -= sell_shares
                    position._reduced = True  # 标记已减仓，避免重复
                    if position.shares <= 0:
                        position = None
                        continue

            # 2. 绝对止损 -7%
            abs_stop = position.avg_cost * (1 - ABSOLUTE_STOP_PCT)
            if low <= abs_stop:
                sell_price = abs_stop
                pnl = (sell_price - position.avg_cost) * position.shares
                trade = Trade(date=date, action="sell", price=sell_price, shares=position.shares,
                             amount=sell_price * position.shares, trade_type="绝对止损",
                             pnl=pnl, pnl_pct=-ABSOLUTE_STOP_PCT * 100)
                result.trades.append(trade)
                cash += sell_price * position.shares
                position = None
                last_sell_date = date
                consecutive_stops += 1
                if consecutive_stops >= CONSECUTIVE_STOP_LIMIT:
                    long_cooldown_until = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=LONG_COOLDOWN_DAYS)).strftime("%Y-%m-%d")
                continue

            # 3. 动态止损 -5% 或 均线止损
            cost_stop = position.avg_cost * (1 - STOP_LOSS_PCT)
            ma_stop = ma20 * 0.97 if ma20 else 0
            dynamic_stop = max(cost_stop, ma_stop)
            if low <= dynamic_stop:
                sell_price = dynamic_stop
                pnl = (sell_price - position.avg_cost) * position.shares
                trade = Trade(date=date, action="sell", price=sell_price, shares=position.shares,
                             amount=sell_price * position.shares, trade_type="动态止损",
                             pnl=pnl, pnl_pct=(sell_price - position.avg_cost) / position.avg_cost * 100)
                result.trades.append(trade)
                cash += sell_price * position.shares
                position = None
                last_sell_date = date
                consecutive_stops += 1
                if consecutive_stops >= CONSECUTIVE_STOP_LIMIT:
                    long_cooldown_until = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=LONG_COOLDOWN_DAYS)).strftime("%Y-%m-%d")
                continue

            # 4. 涨停判断 — 涨停不卖，等打开再说（根据板块自动判断阈值）
            day_change = float(row.get("涨跌幅", 0) or 0) / 100
            is_limit_up = day_change >= limit_up_pct

            # 4.5 强势不卖判断：用均线发散度（MA5-MA60)/MA60 > 5% 才算趋势股
            is_strong_trend = False
            if i >= 5 and ma60 and ma60 > 0:
                ma5_val = sum(float(bt_df.iloc[i - k]["收盘"]) for k in range(5)) / 5
                spread = (ma5_val - ma60) / ma60
                if spread > 0.05:
                    is_strong_trend = True

            # 5. 止盈第一批 +15% 卖1/4（涨停不卖，趋势股强势不卖，但+30%强制卖）
            tp1_price = position.first_buy_price * (1 + TAKE_PROFIT_1_PCT)
            force_tp_price = position.first_buy_price * 1.30
            if not position.sold_batch_1 and not is_limit_up:
                if high >= force_tp_price:
                    tp1_price = force_tp_price
                    sell_shares = position.shares // 4
                elif high >= tp1_price and not is_strong_trend:
                    sell_shares = position.shares // 4
                else:
                    sell_shares = 0

                if sell_shares > 0:
                    tp_type = "止盈1(+30%强制)" if high >= force_tp_price else "止盈1(+15%卖1/4)"
                    pnl = (tp1_price - position.avg_cost) * sell_shares
                    trade = Trade(date=date, action="sell", price=tp1_price, shares=sell_shares,
                                 amount=tp1_price * sell_shares, trade_type=tp_type,
                                 pnl=pnl, pnl_pct=(tp1_price - position.avg_cost) / position.avg_cost * 100)
                    result.trades.append(trade)
                    cash += tp1_price * sell_shares
                    position.shares -= sell_shares
                    position.sold_batch_1 = True

            # 6. 止盈第二批（从最高点回撤5% 卖1/3，涨停不卖，强势不卖）
            if position and position.sold_batch_1 and not position.sold_batch_2 and not is_limit_up and not is_strong_trend:
                tp2_trigger = position.highest_price * (1 - TAKE_PROFIT_2_DRAWDOWN)
                if low <= tp2_trigger:
                    sell_shares = position.shares // 3
                    if sell_shares > 0:
                        pnl = (tp2_trigger - position.avg_cost) * sell_shares
                        trade = Trade(date=date, action="sell", price=tp2_trigger, shares=sell_shares,
                                     amount=tp2_trigger * sell_shares, trade_type="止盈2(回撤5%卖1/3)",
                                     pnl=pnl, pnl_pct=(tp2_trigger - position.avg_cost) / position.avg_cost * 100)
                        result.trades.append(trade)
                        cash += tp2_trigger * sell_shares
                        position.shares -= sell_shares
                        position.sold_batch_2 = True

            # 7. 止盈第三批（从最高点回撤8%或跌破20日线 清仓，涨停不卖）
            if position and position.sold_batch_2 and not is_limit_up:
                tp3_trigger = position.highest_price * (1 - TAKE_PROFIT_3_DRAWDOWN)
                below_ma20 = ma20 and close < ma20
                if low <= tp3_trigger or below_ma20:
                    sell_price = min(tp3_trigger, close) if low <= tp3_trigger else close
                    pnl = (sell_price - position.avg_cost) * position.shares
                    trade = Trade(date=date, action="sell", price=sell_price, shares=position.shares,
                                 amount=sell_price * position.shares, trade_type="止盈3(清仓)",
                                 pnl=pnl, pnl_pct=(sell_price - position.avg_cost) / position.avg_cost * 100)
                    result.trades.append(trade)
                    cash += sell_price * position.shares
                    position = None
                    last_sell_date = date
                    consecutive_stops = 0  # 止盈清仓重置连续止损计数
                    continue

            # 7. 时间止损
            if position:
                buy_dt = datetime.strptime(position.buy_date, "%Y-%m-%d")
                curr_dt = datetime.strptime(date, "%Y-%m-%d")
                hold_days = (curr_dt - buy_dt).days
                if hold_days >= TIME_STOP_DAYS:
                    # 计算最近5天振幅
                    start_idx = max(0, i - TIME_STOP_DAYS + 1)
                    recent = bt_df.iloc[start_idx:i + 1]
                    if len(recent) >= TIME_STOP_DAYS:
                        amplitude = (recent["最高"].max() - recent["最低"].min()) / position.first_buy_price
                        if amplitude < TIME_STOP_AMPLITUDE:
                            pnl = (close - position.avg_cost) * position.shares
                            trade = Trade(date=date, action="sell", price=close, shares=position.shares,
                                         amount=close * position.shares, trade_type="时间止损",
                                         pnl=pnl, pnl_pct=(close - position.avg_cost) / position.avg_cost * 100)
                            result.trades.append(trade)
                            cash += close * position.shares
                            position = None
                            last_sell_date = date
                            consecutive_stops += 1
                            if consecutive_stops >= CONSECUTIVE_STOP_LIMIT:
                                long_cooldown_until = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=LONG_COOLDOWN_DAYS)).strftime("%Y-%m-%d")
                            continue

            # 8. 加仓检查（激进模式：金字塔加仓）
            if position and position.add_count < 2:
                gain = (close - position.first_buy_price) / position.first_buy_price
                trigger = ADD_TRIGGER_1 if position.add_count == 0 else ADD_TRIGGER_2
                if gain >= trigger and ma20 and close > ma20:
                    if aggressive:
                        add_amount = BUY_AMOUNT_ADD1 if position.add_count == 0 else BUY_AMOUNT_ADD2
                    else:
                        add_amount = BUY_AMOUNT
                    add_shares = int(add_amount / close / 100) * 100
                    if add_shares > 0 and cash >= add_shares * close:
                        add_cost = add_shares * close
                        new_total = position.shares + add_shares
                        position.avg_cost = round((position.avg_cost * position.shares + close * add_shares) / new_total, 2)
                        position.shares = new_total
                        position.add_count += 1
                        cash -= add_cost
                        trade = Trade(date=date, action="buy", price=close, shares=add_shares,
                                     amount=add_cost, trade_type=f"加仓{position.add_count}")
                        result.trades.append(trade)

        # === 无持仓时的买入检查 ===
        elif not position:
            # 清仓冷却期检查
            in_cooldown = False
            if last_sell_date:
                sell_dt = datetime.strptime(last_sell_date, "%Y-%m-%d")
                curr_dt = datetime.strptime(date, "%Y-%m-%d")
                if (curr_dt - sell_dt).days < COOLDOWN_DAYS:
                    in_cooldown = True
            # 连续止损长冷却检查
            if long_cooldown_until:
                if date < long_cooldown_until:
                    in_cooldown = True

            if not in_cooldown:
                # 买入条件：连续2天收盘在20日线上方 + 大盘OK + 成交量确认
                above_ma20_days = 0
                if ma20 and close > ma20:
                    for j in range(1, BUY_CONFIRM_DAYS + 1):
                        if i - j >= 0:
                            prev = bt_df.iloc[i - j]
                            prev_ma20 = prev["MA20"]
                            if pd.notna(prev_ma20) and float(prev["收盘"]) > float(prev_ma20):
                                above_ma20_days += 1
                            else:
                                break

                # 离均线距离检查（趋势股放宽限制）
                above_ma20_pct = (close - ma20) / ma20 if ma20 and ma20 > 0 else 0
                # 计算当前均线发散度判断是否趋势股
                is_trend_for_buy = False
                if i >= 5 and ma60 and ma60 > 0:
                    ma5_buy = sum(float(bt_df.iloc[i - k]["收盘"]) for k in range(5)) / 5
                    spread_buy = (ma5_buy - ma60) / ma60
                    if spread_buy > 0.05:
                        is_trend_for_buy = True
                # 趋势股：追涨限制放宽到15%；非趋势股：8%
                max_above = 0.15 if is_trend_for_buy else MAX_ABOVE_MA20_PCT
                too_far_from_ma20 = above_ma20_pct > max_above

                # 当日涨幅检查
                day_gain_buy = float(row.get("涨跌幅", 0) or 0) / 100
                too_hot_today = abs(day_gain_buy) > MAX_DAY_GAIN

                # 近3日累计涨幅检查
                gain_3d = 0
                if i >= 3:
                    close_3d_ago = float(bt_df.iloc[i - 3]["收盘"])
                    gain_3d = (close - close_3d_ago) / close_3d_ago
                too_hot_3d = gain_3d > MAX_3DAY_GAIN

                if (above_ma20_days >= BUY_CONFIRM_DAYS and
                    ma60_up and
                    market_ok and not market_clear and
                    vol > vol_ma20 * 0.8 and
                    not too_far_from_ma20 and
                    not too_hot_today and
                    not too_hot_3d):

                    buy_shares = int(BUY_AMOUNT / close / 100) * 100
                    if buy_shares > 0 and cash >= buy_shares * close:
                        buy_cost = buy_shares * close
                        position = Position(
                            code=code, name=name,
                            first_buy_price=close, avg_cost=close,
                            shares=buy_shares, buy_date=date,
                            highest_price=high
                        )
                        cash -= buy_cost
                        trade = Trade(date=date, action="buy", price=close, shares=buy_shares,
                                     amount=buy_cost, trade_type="首次买入")
                        result.trades.append(trade)

    # 回测结束，如果还有持仓，按最后收盘价平仓
    if position:
        last_close = float(bt_df.iloc[-1]["收盘"])
        last_date = str(bt_df.iloc[-1]["日期"])
        pnl = (last_close - position.avg_cost) * position.shares
        trade = Trade(date=last_date, action="sell", price=last_close, shares=position.shares,
                     amount=last_close * position.shares, trade_type="回测结束平仓",
                     pnl=pnl, pnl_pct=(last_close - position.avg_cost) / position.avg_cost * 100)
        result.trades.append(trade)
        cash += last_close * position.shares
        position = None

    # 统计结果
    sell_trades = [t for t in result.trades if t.action == "sell"]
    result.total_trades = len(sell_trades)
    result.win_trades = len([t for t in sell_trades if t.pnl > 0])
    result.loss_trades = len([t for t in sell_trades if t.pnl <= 0])
    result.total_pnl = sum(t.pnl for t in sell_trades)
    result.max_profit = max((t.pnl for t in sell_trades), default=0)

    return result, cash, initial_cash


def print_report(result, final_cash, initial_cash):
    """打印回测报告"""
    print("\n" + "=" * 60)
    print(f"📊 回测报告: {result.code}")
    print(f"   区间: {result.start_date} ~ {result.end_date}")
    print("=" * 60)

    total_return = (final_cash - initial_cash) / initial_cash * 100
    win_rate = result.win_trades / result.total_trades * 100 if result.total_trades > 0 else 0

    # 盈亏比
    wins = [t.pnl for t in result.trades if t.action == "sell" and t.pnl > 0]
    losses = [abs(t.pnl) for t in result.trades if t.action == "sell" and t.pnl <= 0]
    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = sum(losses) / len(losses) if losses else 1
    profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else float('inf')

    print(f"\n  初始资金: ¥{initial_cash:,.0f}")
    print(f"  最终资金: ¥{final_cash:,.0f}")
    print(f"  总收益: ¥{result.total_pnl:+,.0f} ({total_return:+.1f}%)")
    print(f"  最大回撤: {result.max_drawdown:.1%}")
    print(f"  总卖出次数: {result.total_trades}")
    print(f"  盈利次数: {result.win_trades} | 亏损次数: {result.loss_trades}")
    print(f"  胜率: {win_rate:.1f}%（目标 40-60%）")
    print(f"  盈亏比: {profit_loss_ratio:.2f}（目标 >2）")
    print(f"  平均盈利: ¥{avg_win:,.0f} | 平均亏损: ¥{avg_loss:,.0f}")

    print(f"\n  交易明细:")
    print(f"  {'日期':<12} {'操作':<6} {'价格':>8} {'股数':>6} {'类型':<12} {'盈亏':>10}")
    print(f"  {'-'*60}")
    for t in result.trades:
        pnl_str = f"¥{t.pnl:+,.0f}" if t.action == "sell" else ""
        print(f"  {t.date:<12} {t.action:<6} {t.price:>8.2f} {t.shares:>6} {t.trade_type:<12} {pnl_str:>10}")

    # 按卖出类型统计
    print(f"\n  卖出类型统计:")
    type_stats = {}
    for t in result.trades:
        if t.action == "sell":
            tp = t.trade_type
            if tp not in type_stats:
                type_stats[tp] = {"count": 0, "pnl": 0}
            type_stats[tp]["count"] += 1
            type_stats[tp]["pnl"] += t.pnl
    for tp, stats in sorted(type_stats.items(), key=lambda x: x[1]["count"], reverse=True):
        print(f"    {tp}: {stats['count']}次 | 盈亏 ¥{stats['pnl']:+,.0f}")

    print("\n" + "=" * 60)

    # 输出 JSON 结果
    return {
        "code": result.code,
        "period": f"{result.start_date}~{result.end_date}",
        "initial_cash": initial_cash,
        "final_cash": round(final_cash, 2),
        "total_return": f"{total_return:+.1f}%",
        "total_pnl": round(result.total_pnl, 2),
        "max_drawdown": f"{result.max_drawdown:.1%}",
        "total_trades": result.total_trades,
        "win_rate": f"{win_rate:.1f}%",
        "profit_loss_ratio": round(profit_loss_ratio, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
    }


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    # 解析参数
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    aggressive = '--aggressive' in sys.argv

    code = args[0]
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")

    if len(args) > 1:
        start_date = args[1]
    if len(args) > 2:
        end_date = args[2]

    result_tuple = run_backtest(code, start_date, end_date, aggressive=aggressive)
    if result_tuple:
        result, final_cash, initial_cash = result_tuple
        report = print_report(result, final_cash, initial_cash)
        print(f"\nJSON: {json.dumps(report, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
