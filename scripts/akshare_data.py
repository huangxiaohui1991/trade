#!/usr/bin/env python3
import os
os.environ["TQDM_DISABLE"] = "1"

import warnings
warnings.filterwarnings("ignore")

"""
A股交易系统 v1.3 - AKShare 数据获取引擎
依赖: pip install akshare

用法:
  python akshare_data.py stock_info <代码>           个股基本信息
  python akshare_data.py fundamental <代码>          基本面数据（ROE/营收/现金流）
  python akshare_data.py technical <代码> [天数]      技术面数据（均线/成交量）
  python akshare_data.py market_status               大盘状态（上证/创业板 vs 均线）
  python akshare_data.py northbound [天数]            北向资金流向
  python akshare_data.py fund_flow <代码> [天数]      个股主力资金流向
  python akshare_data.py score <代码>                 自动打分（综合以上数据）
  python akshare_data.py realtime <代码>              实时行情
  python akshare_data.py batch_score <代码1,代码2,...> 批量打分
"""

import sys
import json
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")

try:
    import akshare as ak
    import pandas as pd
except ImportError:
    print(json.dumps({"error": "请先安装依赖: pip install akshare pandas"}, ensure_ascii=False))
    sys.exit(1)


def normalize_code(code: str) -> str:
    """标准化股票代码（去掉前缀）"""
    code = code.strip()
    for prefix in ["sh", "sz", "SH", "SZ", "bj", "BJ"]:
        if code.startswith(prefix):
            code = code[len(prefix):]
    return code


def to_sina_symbol(code: str) -> str:
    """转换为新浪格式 sh600001 / sz000001"""
    code = normalize_code(code)
    if code.startswith("6") or code.startswith("9"):
        return f"sh{code}"
    else:
        return f"sz{code}"


# 股票名称缓存
_name_cache = {}


def get_stock_name(code: str) -> str:
    """获取股票名称（带缓存，多接口 fallback）"""
    code = normalize_code(code)
    if code in _name_cache:
        return _name_cache[code]

    # 方法1: em 接口
    try:
        df = ak.stock_individual_info_em(symbol=code)
        name_row = df[df["item"] == "股票简称"]
        if not name_row.empty:
            name = str(name_row.iloc[0]["value"])
            _name_cache[code] = name
            return name
    except Exception:
        pass

    # 方法2: 从新浪日线数据的 DataFrame 无法直接获取名称，返回代码
    _name_cache[code] = code
    return code


def get_hist_data(code: str, days: int = 120) -> pd.DataFrame:
    """
    获取历史日线数据（多接口 fallback）
    优先 em 接口，失败则用新浪接口
    返回统一格式 DataFrame: 日期,开盘,收盘,最高,最低,成交量,涨跌幅
    """
    code = normalize_code(code)
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")

    # 方法1: em 接口（东方财富）
    try:
        df = ak.stock_zh_a_hist(
            symbol=code, period="daily",
            start_date=start_date, end_date=end_date,
            adjust="qfq"
        )
        if df is not None and not df.empty:
            return df
    except Exception:
        pass

    # 方法2: 新浪接口（更稳定，尤其非交易时间）
    try:
        import time
        time.sleep(0.3)  # 避免限流
        sina_symbol = to_sina_symbol(code)
        df = ak.stock_zh_a_daily(symbol=sina_symbol, adjust="qfq")
        if df is not None and not df.empty:
            # 统一列名为 em 格式
            col_map = {
                "date": "日期", "open": "开盘", "close": "收盘",
                "high": "最高", "low": "最低", "volume": "成交量",
            }
            df = df.rename(columns=col_map)
            # 计算涨跌幅
            df["涨跌幅"] = df["收盘"].pct_change() * 100
            # 过滤日期范围
            df["日期"] = pd.to_datetime(df["日期"])
            start_dt = pd.to_datetime(start_date)
            df = df[df["日期"] >= start_dt].copy()
            df["日期"] = df["日期"].dt.strftime("%Y-%m-%d")
            return df
    except Exception:
        pass

    return pd.DataFrame()


def get_stock_info(code: str) -> dict:
    """获取个股基本信息"""
    code = normalize_code(code)
    try:
        df = ak.stock_individual_info_em(symbol=code)
        info = {}
        for _, row in df.iterrows():
            info[row["item"]] = row["value"]
        return {"code": code, "info": info}
    except Exception as e:
        return {"code": code, "error": str(e)}


def get_fundamental(code: str) -> dict:
    """
    获取基本面数据
    - ROE（近两年）
    - 营收增长（近两季）
    - 经营现金流
    """
    code = normalize_code(code)
    result = {"code": code, "name": get_stock_name(code)}

    try:
        # 获取财务指标
        df = ak.stock_financial_analysis_indicator(symbol=code, start_year="2024")
        if df is not None and not df.empty:
            # ROE（净资产收益率）
            roe_col = None
            for col in df.columns:
                if "净资产收益率" in str(col) or "roe" in str(col).lower():
                    roe_col = col
                    break

            if roe_col:
                roe_values = df[roe_col].dropna().head(4).tolist()
                result["roe_recent"] = roe_values
            else:
                result["roe_recent"] = "未找到ROE列"

            result["financial_columns"] = df.columns.tolist()[:10]
    except Exception as e:
        result["financial_error"] = str(e)

    try:
        # 获取利润表数据（营收）
        df_profit = ak.stock_profit_sheet_by_report_em(symbol=code)
        if df_profit is not None and not df_profit.empty:
            # 查找营业收入列
            revenue_col = None
            for col in df_profit.columns:
                if "营业总收入" in str(col) or "营业收入" in str(col):
                    revenue_col = col
                    break

            if revenue_col:
                recent = df_profit.head(4)
                revenues = recent[revenue_col].tolist()
                result["revenue_recent_quarters"] = revenues

                # 计算增长率
                if len(revenues) >= 2:
                    growths = []
                    for i in range(len(revenues) - 1):
                        if revenues[i + 1] and revenues[i + 1] != 0:
                            growth = (revenues[i] - revenues[i + 1]) / abs(revenues[i + 1])
                            growths.append(f"{growth:.1%}")
                        else:
                            growths.append("N/A")
                    result["revenue_growth"] = growths
    except Exception as e:
        result["profit_error"] = str(e)

    try:
        # 获取现金流量表
        df_cash = ak.stock_cash_flow_sheet_by_report_em(symbol=code)
        if df_cash is not None and not df_cash.empty:
            cash_col = None
            for col in df_cash.columns:
                if "经营活动" in str(col) and "现金流" in str(col):
                    cash_col = col
                    break

            if cash_col:
                result["operating_cash_flow"] = df_cash[cash_col].head(2).tolist()
                result["cash_flow_positive"] = float(df_cash[cash_col].iloc[0]) > 0 if not df_cash[cash_col].empty else None
    except Exception as e:
        result["cash_error"] = str(e)

    return result


def get_technical(code: str, days: int = 60) -> dict:
    """
    获取技术面数据
    - 均线位置（5/10/20/60日）
    - 成交量分析
    - 当前价格 vs 均线
    """
    code = normalize_code(code)

    try:
        df = get_hist_data(code, days + 60)

        if df is None or df.empty:
            return {"code": code, "error": "无法获取行情数据"}

        # 计算均线
        df["MA5"] = df["收盘"].rolling(5).mean()
        df["MA10"] = df["收盘"].rolling(10).mean()
        df["MA20"] = df["收盘"].rolling(20).mean()
        df["MA60"] = df["收盘"].rolling(60).mean()

        # 成交量均线
        df["VOL_MA5"] = df["成交量"].rolling(5).mean()
        df["VOL_MA20"] = df["成交量"].rolling(20).mean()

        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest

        current_price = float(latest["收盘"])
        ma5 = float(latest["MA5"]) if pd.notna(latest["MA5"]) else None
        ma10 = float(latest["MA10"]) if pd.notna(latest["MA10"]) else None
        ma20 = float(latest["MA20"]) if pd.notna(latest["MA20"]) else None
        ma60 = float(latest["MA60"]) if pd.notna(latest["MA60"]) else None

        # 60日均线方向（对比5天前）
        ma60_5d_ago = float(df.iloc[-6]["MA60"]) if len(df) > 5 and pd.notna(df.iloc[-6]["MA60"]) else None
        ma60_direction = None
        if ma60 and ma60_5d_ago:
            if ma60 > ma60_5d_ago * 1.001:
                ma60_direction = "向上"
            elif ma60 < ma60_5d_ago * 0.999:
                ma60_direction = "向下"
            else:
                ma60_direction = "走平"

        # 成交量分析
        vol_today = float(latest["成交量"])
        vol_ma5 = float(latest["VOL_MA5"]) if pd.notna(latest["VOL_MA5"]) else None
        vol_ma20 = float(latest["VOL_MA20"]) if pd.notna(latest["VOL_MA20"]) else None

        # 成交量评分
        volume_score = 0
        if vol_ma20 and vol_today > vol_ma20:
            volume_score = 1.0
        elif vol_ma20:
            # 检查近3日是否曾放量
            recent_3d = df.tail(3)
            if any(recent_3d["成交量"] > vol_ma20):
                volume_score = 0.5

        # 突破量加分
        volume_breakout = vol_ma5 and vol_today > vol_ma5 * 1.5

        # 近5日振幅（用于时间止损判断）
        recent_5d = df.tail(5)
        high_5d = float(recent_5d["最高"].max())
        low_5d = float(recent_5d["最低"].min())
        amplitude_5d = (high_5d - low_5d) / current_price

        result = {
            "code": code,
            "name": get_stock_name(code),
            "date": str(latest["日期"]),
            "current_price": round(current_price, 2),
            "change_pct": round(float(latest["涨跌幅"]), 2),
            "volume": int(vol_today),
            "ma": {
                "MA5": round(ma5, 2) if ma5 else None,
                "MA10": round(ma10, 2) if ma10 else None,
                "MA20": round(ma20, 2) if ma20 else None,
                "MA60": round(ma60, 2) if ma60 else None,
            },
            "above_ma20": current_price > ma20 if ma20 else None,
            "above_ma60": current_price > ma60 if ma60 else None,
            "ma60_direction": ma60_direction,
            "volume_analysis": {
                "today": int(vol_today),
                "MA5": int(vol_ma5) if vol_ma5 else None,
                "MA20": int(vol_ma20) if vol_ma20 else None,
                "above_ma20": vol_today > vol_ma20 if vol_ma20 else None,
                "score": volume_score,
                "breakout_1_5x": volume_breakout,
            },
            "amplitude_5d": f"{amplitude_5d:.2%}",
            "high_5d": round(high_5d, 2),
            "low_5d": round(low_5d, 2),
        }
        return result

    except Exception as e:
        return {"code": code, "error": str(e)}


def get_market_status() -> dict:
    """
    获取大盘状态（支持盘中实时数据）
    - 交易时间用 stock_zh_index_spot_sina（实时价格）
    - 盘后/非交易日用 stock_zh_index_daily（历史收盘价）
    - MA20/MA60 始终从历史数据计算
    """
    from datetime import datetime
    result = {}

    indices = {
        "上证指数": "sh000001",
        "创业板指": "sz399006",
    }

    from datetime import time
    now = datetime.now()
    # 交易时间段：9:30-11:30, 13:00-15:30（收盘后30分钟内仍用实时数据）
    current_time = now.time()
    is_market_hours = (
        now.weekday() < 5 and (
            time(9, 30) <= current_time <= time(11, 30) or
            time(13, 0) <= current_time <= time(15, 30)
        )
    )

    # 盘中实时数据（东方财富 Sina 实时接口）
    spot_data = {}
    try:
        spot_df = ak.stock_zh_index_spot_sina()
        spot_df = spot_df.set_index("代码")
        for name, symbol in indices.items():
            if symbol in spot_df.index:
                row = spot_df.loc[symbol]
                spot_data[symbol] = {
                    "last": float(row["最新价"]),
                    "prev_close": float(row["昨收"]),
                    "open": float(row["今开"]),
                    "high": float(row["最高"]),
                    "low": float(row["最低"]),
                    "change_pct": float(row["涨跌幅"]),
                    "volume": int(row["成交量"]),
                }
    except Exception:
        pass  # 实时获取失败，降级到历史数据

    for name, symbol in indices.items():
        try:
            df = ak.stock_zh_index_daily(symbol=symbol)

            if df is None or df.empty:
                result[name] = {"error": "无法获取数据"}
                continue

            df = df.sort_values("date").tail(80)
            df["MA20"] = df["close"].rolling(20).mean()
            df["MA60"] = df["close"].rolling(60).mean()

            hist_latest = df.iloc[-1]
            ma20 = float(hist_latest["MA20"]) if pd.notna(hist_latest["MA20"]) else None
            ma60 = float(hist_latest["MA60"]) if pd.notna(hist_latest["MA60"]) else None

            # 始终优先用 spot 数据（盘中=实时价，收盘后=今日收盘价）
            # is_realtime 仅用于标记：盘中为 True，盘后/历史为 False
            if symbol in spot_data:
                sd = spot_data[symbol]
                close_price = sd["last"]
                date_str = now.strftime("%Y-%m-%d")
                change_pct = sd["change_pct"]
                is_realtime = is_market_hours
            else:
                close_price = float(hist_latest["close"])
                date_str = str(hist_latest["date"])
                change_pct = None
                is_realtime = False

            # 连续在60日线下方的天数（始终用历史数据）
            below_ma60_days = 0
            if ma60:
                for i in range(len(df) - 1, -1, -1):
                    row = df.iloc[i]
                    if pd.notna(row["MA60"]) and float(row["close"]) < float(row["MA60"]):
                        below_ma60_days += 1
                    else:
                        break

            above_ma20 = close_price > ma20 if ma20 else None
            above_ma60 = close_price > ma60 if ma60 else None

            result[name] = {
                "date": date_str,
                "close": round(close_price, 2),
                "MA20": round(ma20, 2) if ma20 else None,
                "MA60": round(ma60, 2) if ma60 else None,
                "above_MA20": above_ma20,
                "above_MA60": above_ma60,
                "below_MA60_days": below_ma60_days,
                "change_pct": round(change_pct, 2) if change_pct is not None else None,
                "realtime": is_realtime,
            }
        except Exception as e:
            result[name] = {"error": str(e)}

    sh = result.get("上证指数", {})
    cy = result.get("创业板指", {})

    market_ok = (sh.get("above_MA20", False) or cy.get("above_MA20", False))
    market_danger = (sh.get("below_MA60_days", 0) >= 3 or cy.get("below_MA60_days", 0) >= 3)

    result["_summary"] = {
        "can_buy": market_ok,
        "should_clear": market_danger,
        "status": "CLEAR" if market_danger else ("BUY" if market_ok else "WARY"),
    }

    return result


def get_northbound(days: int = 10) -> dict:
    """获取北向资金流向"""
    try:
        # 获取沪股通+深股通历史数据
        df_sh = ak.stock_hsgt_hist_em(symbol="沪股通")
        df_sz = ak.stock_hsgt_hist_em(symbol="深股通")

        if df_sh is None or df_sz is None:
            return {"error": "无法获取北向资金数据"}

        # 取最近N天
        df_sh = df_sh.tail(days)
        df_sz = df_sz.tail(days)

        flows = []
        for i in range(len(df_sh)):
            sh_row = df_sh.iloc[i]
            sz_row = df_sz.iloc[i] if i < len(df_sz) else None

            sh_net = sh_row.get("当日成交净买额", 0)
            sz_net = sz_row.get("当日成交净买额", 0) if sz_row is not None else 0

            # 处理 NaN
            sh_net = float(sh_net) if pd.notna(sh_net) else 0
            sz_net = float(sz_net) if pd.notna(sz_net) else 0

            total = sh_net + sz_net
            flows.append({
                "date": str(sh_row.get("日期", "")),
                "net_flow": round(total / 1e8, 2),  # 转为亿元
            })

        # 近5日净流入
        last_5 = flows[-5:] if len(flows) >= 5 else flows
        net_5d = sum(f["net_flow"] for f in last_5)

        # 如果全是0（数据缺失），标记为不确定
        all_zero = all(f["net_flow"] == 0 for f in last_5)

        return {
            "recent_flows": flows,
            "net_5d": round(net_5d, 2),
            "net_5d_positive": net_5d > 0 if not all_zero else None,
            "data_available": not all_zero,
            "unit": "亿元",
        }
    except Exception as e:
        return {"error": str(e), "net_5d_positive": None}


def get_fund_flow(code: str, days: int = 5) -> dict:
    """获取个股主力资金流向"""
    code = normalize_code(code)
    try:
        df = ak.stock_individual_fund_flow(stock=code, market="sh" if code.startswith("6") else "sz")
        if df is None or df.empty:
            return {"code": code, "error": "无法获取资金流向数据"}

        recent = df.tail(days)
        flows = []
        major_outflow_streak = 0

        for _, row in recent.iterrows():
            # 主力净流入
            main_net = None
            for col in row.index:
                if "主力" in str(col) and "净流入" in str(col) and "净额" in str(col):
                    main_net = float(row[col]) if pd.notna(row[col]) else 0
                    break

            if main_net is None:
                # 尝试其他列名
                for col in row.index:
                    if "主力" in str(col) and "净" in str(col):
                        main_net = float(row[col]) if pd.notna(row[col]) else 0
                        break

            flows.append({
                "date": str(row.get("日期", "")),
                "main_net_flow": main_net,
            })

        # 检查连续主力大额流出
        for f in reversed(flows):
            if f["main_net_flow"] is not None and f["main_net_flow"] < -5000000:  # 500万以上算大额
                major_outflow_streak += 1
            else:
                break

        return {
            "code": code,
            "name": get_stock_name(code),
            "recent_flows": flows,
            "major_outflow_streak": major_outflow_streak,
            "no_major_outflow": major_outflow_streak < 3,
            "columns_available": df.columns.tolist()[:10],
        }
    except Exception as e:
        return {"code": code, "error": str(e)}


def get_realtime(code: str) -> dict:
    """获取实时/最新行情（交易时间用实时接口，非交易时间用最近日线）"""
    code = normalize_code(code)

    # 先尝试实时接口
    try:
        df = ak.stock_zh_a_spot_em()
        row = df[df["代码"] == code]
        if not row.empty:
            r = row.iloc[0]
            return {
                "code": code,
                "name": str(r.get("名称", "")),
                "price": float(r.get("最新价", 0)),
                "change_pct": float(r.get("涨跌幅", 0)),
                "change_amount": float(r.get("涨跌额", 0)),
                "volume": int(r.get("成交量", 0)),
                "amount": float(r.get("成交额", 0)),
                "high": float(r.get("最高", 0)),
                "low": float(r.get("最低", 0)),
                "open": float(r.get("今开", 0)),
                "prev_close": float(r.get("昨收", 0)),
                "turnover_rate": float(r.get("换手率", 0)),
                "pe": float(r.get("市盈率-动态", 0)) if pd.notna(r.get("市盈率-动态")) else None,
                "pb": float(r.get("市净率", 0)) if pd.notna(r.get("市净率")) else None,
                "total_mv": float(r.get("总市值", 0)),
                "circ_mv": float(r.get("流通市值", 0)),
                "source": "realtime",
            }
    except Exception:
        pass

    # fallback: 用最近日线数据
    try:
        df = get_hist_data(code, 10)
        if df is not None and not df.empty:
            r = df.iloc[-1]
            return {
                "code": code,
                "name": get_stock_name(code),
                "price": float(r["收盘"]),
                "change_pct": float(r["涨跌幅"]),
                "volume": int(r["成交量"]),
                "amount": float(r["成交额"]),
                "high": float(r["最高"]),
                "low": float(r["最低"]),
                "open": float(r["开盘"]),
                "date": str(r["日期"]),
                "source": "daily_hist_fallback",
            }
    except Exception as e2:
        return {"code": code, "error": f"实时和日线接口均失败: {e2}"}


def track_pool() -> dict:
    """
    跟踪核心池和观察池标的：
    - 解析 markdown 文件获取标的列表
    - 获取每只股票的 MA20/MA60 技术状态
    - 返回 {core: [...], observe: [...], updated_at: ...}
    """
    from datetime import datetime
    import os

    pools = {"core": {"name": "核心池", "path": os.path.join(os.path.dirname(__file__), "..", "data", "04-选股", "核心池.md")},
             "observe": {"name": "观察池", "path": os.path.join(os.path.dirname(__file__), "..", "data", "04-选股", "观察池.md")}}

    result = {}
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    for pool_key, pool_info in pools.items():
        pool_path = pool_info["path"]
        pool_name = pool_info["name"]
        stocks = []

        if not os.path.exists(pool_path):
            result[pool_key] = {"name": pool_name, "stocks": [], "note": "文件不存在"}
            continue

        try:
            with open(pool_path, "r", encoding="utf-8") as f:
                content = f.read()

            in_table = False
            for line in content.split("\n"):
                line = line.strip()
                # 进入当前核心池表格
                if "## 当前" in line and pool_name[:2] in line:
                    in_table = True
                    continue
                # 遇到其他 ## 标题就停止
                if in_table and line.startswith("##"):
                    break
                if in_table and line.startswith("|"):
                    if "股票" in line and "代码" in line:
                        continue  # 跳过表头
                    if "---" in line or "--" in line:
                        continue
                    parts = [p.strip() for p in line.split("|")]
                    if len(parts) >= 3:
                        name = parts[2].strip()
                        code = parts[3].strip() if len(parts) > 3 else ""
                        if name and name not in ["股票", ""] and code.isdigit():
                            stocks.append({"name": name, "code": code})
        except Exception as e:
            result[pool_key] = {"name": pool_name, "stocks": [], "error": str(e)}
            continue

        # 获取每只股票的技术数据
        enriched = []
        for stk in stocks:
            try:
                tech = get_technical(stk["code"], 60)
                rt = get_realtime(stk["code"])
                price = rt.get("price", tech.get("current_price"))
                ma20 = tech.get("ma20")
                ma60 = tech.get("ma60")
                ma60_dir = tech.get("ma60_direction", "")
                above_ma20 = tech.get("above_ma20")

                # 信号判断
                signal = ""
                signal_emoji = ""
                if above_ma20 is True:
                    signal = "站上MA20"
                    signal_emoji = "🟢"
                elif above_ma20 is False:
                    signal = "跌破MA20"
                    signal_emoji = "🔴"

                enriched.append({
                    "name": stk["name"],
                    "code": stk["code"],
                    "price": price,
                    "ma20": ma20,
                    "ma60": ma60,
                    "ma60_dir": ma60_dir,
                    "above_ma20": above_ma20,
                    "signal": signal,
                    "signal_emoji": signal_emoji,
                })
            except Exception:
                enriched.append({"name": stk["name"], "code": stk["code"], "error": "数据获取失败"})

        result[pool_key] = {"name": pool_name, "stocks": enriched}

    result["updated_at"] = now
    return result


def auto_score(code: str) -> dict:
    """
    自动打分 — 综合基本面、技术面、资金面数据
    返回完整打分结果 + 原始数据
    """
    code = normalize_code(code)
    name = get_stock_name(code)

    print(f"正在获取 {name}({code}) 数据...", file=sys.stderr)

    # 1. 获取技术面数据
    print("  → 技术面数据...", file=sys.stderr)
    tech = get_technical(code, 60)

    # 2. 获取基本面数据
    print("  → 基本面数据...", file=sys.stderr)
    fund = get_fundamental(code)

    # 3. 获取资金流向
    print("  → 资金流向...", file=sys.stderr)
    flow = get_fund_flow(code, 5)

    # 4. 获取北向资金
    print("  → 北向资金...", file=sys.stderr)
    north = get_northbound(5)

    # 构建打分输入
    score_input = {
        "stock_name": name,
        "stock_code": code,
        # 行业和催化需要人工判断，默认标记为待确认
        "is_main_sector": None,  # 需人工确认
        "has_catalyst": None,    # 需人工确认
        # 基本面（从数据推断）
        "roe_pass": None,        # 需要从财务数据判断
        "revenue_growth": None,  # 需要从财务数据判断
        "cash_flow_positive": fund.get("cash_flow_positive"),
        # 技术面（自动判断）
        "above_ma20": tech.get("above_ma20"),
        "ma60_up_or_flat": tech.get("ma60_direction") in ["向上", "走平"] if tech.get("ma60_direction") else None,
        # 成交量（自动判断）
        "volume_score": tech.get("volume_analysis", {}).get("score", 0),
        "volume_breakout": tech.get("volume_analysis", {}).get("breakout_1_5x", False),
        # 资金面
        "northbound_inflow": north.get("net_5d_positive"),
        "no_major_outflow": flow.get("no_major_outflow"),
    }

    # 自动评分（能自动判断的项）
    auto_items = {}
    manual_items = {}

    for key, value in score_input.items():
        if key in ("stock_name", "stock_code"):
            continue
        if value is None:
            manual_items[key] = "⚠️ 需人工确认"
        else:
            auto_items[key] = value

    # 计算已知项得分
    auto_score_val = 0
    auto_details = []

    scoring_map = {
        "is_main_sector": ("属于当前主线板块", 1.0),
        "has_catalyst": ("有政策或产业催化", 1.0),
        "roe_pass": ("ROE≥8%（连续两年）", 1.0),
        "revenue_growth": ("营收连续两季度增长", 1.0),
        "cash_flow_positive": ("经营现金流为正", 1.0),
        "above_ma20": ("股价站上20日均线 ⚠️一票否决", 1.0),
        "ma60_up_or_flat": ("60日均线向上或走平", 1.0),
        "northbound_inflow": ("北向资金近5日净流入", 1.0),
        "no_major_outflow": ("无连续主力大额卖出", 1.0),
    }

    for key, (desc, score) in scoring_map.items():
        value = score_input.get(key)
        if value is True:
            auto_score_val += score
            auto_details.append(f"✅ {desc} (+{score})")
        elif value is False:
            auto_details.append(f"❌ {desc} (0)")
        else:
            auto_details.append(f"⚠️ {desc} (待确认)")

    # 成交量
    vol_score = score_input.get("volume_score", 0)
    auto_score_val += vol_score
    auto_details.append(f"{'✅' if vol_score >= 0.5 else '❌'} 成交量条件 (+{vol_score})")

    # 加分项
    if score_input.get("volume_breakout"):
        auto_score_val += 0.5
        auto_details.append("✅ 突破量加分 (+0.5)")

    result = {
        "code": code,
        "name": name,
        "auto_score": round(auto_score_val, 1),
        "max_possible_score": 10.5,
        "auto_details": auto_details,
        "manual_items_needed": manual_items,
        "score_input": score_input,
        "raw_data": {
            "technical": tech,
            "fundamental": fund,
            "fund_flow": flow,
            "northbound": north,
        },
        "suggestion": (
            f"自动得分 {auto_score_val}/10.5，"
            f"还有 {len(manual_items)} 项需人工确认。"
            f"{'技术面通过✅' if score_input.get('above_ma20') else '⚠️ 未站上20日线（一票否决）'}"
        ),
    }
    return result


def batch_score(codes: list) -> dict:
    """批量打分"""
    results = []
    for code in codes:
        try:
            r = auto_score(code.strip())
            results.append({
                "code": r["code"],
                "name": r["name"],
                "auto_score": r["auto_score"],
                "above_ma20": r["score_input"].get("above_ma20"),
                "manual_needed": len(r["manual_items_needed"]),
                "suggestion": r["suggestion"],
            })
        except Exception as e:
            results.append({"code": code, "error": str(e)})

    # 按得分排序
    results.sort(key=lambda x: x.get("auto_score", 0), reverse=True)
    return {"results": results, "count": len(results)}


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == "stock_info":
        code = sys.argv[2] if len(sys.argv) > 2 else ""
        result = get_stock_info(code)

    elif command == "fundamental":
        code = sys.argv[2] if len(sys.argv) > 2 else ""
        result = get_fundamental(code)

    elif command == "technical":
        code = sys.argv[2] if len(sys.argv) > 2 else ""
        days = int(sys.argv[3]) if len(sys.argv) > 3 else 60
        result = get_technical(code, days)

    elif command == "market_status":
        result = get_market_status()

    elif command == "northbound":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        result = get_northbound(days)

    elif command == "fund_flow":
        code = sys.argv[2] if len(sys.argv) > 2 else ""
        days = int(sys.argv[3]) if len(sys.argv) > 3 else 5
        result = get_fund_flow(code, days)

    elif command == "realtime":
        code = sys.argv[2] if len(sys.argv) > 2 else ""
        result = get_realtime(code)

    elif command == "score":
        code = sys.argv[2] if len(sys.argv) > 2 else ""
        result = auto_score(code)

    elif command == "batch_score":
        codes_str = sys.argv[2] if len(sys.argv) > 2 else ""
        codes = codes_str.split(",")
        result = batch_score(codes)

    else:
        print(f"未知命令: {command}")
        print(__doc__)
        sys.exit(1)

    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
