"""
backtest/drawdown.py — 单票历史回撤分析

功能：
  - 通过 baostock 拉取 A 股历史行情（前复权）
  - 计算最大回撤、滚动高点、回撤率序列
  - 输出月度统计、Top-5 回撤区间
  - 支持多代码批量分析

CLI 入口（通过 trade.py backtest drawdown）：
  trade backtest drawdown --code 601869 --start 2025-01-01 --end 2026-04-10
  trade backtest drawdown --codes 601869,603803 --days 365
"""

from __future__ import annotations

import os
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pandas as pd

# baostock 为可选依赖
try:
    import baostock as bs
    _BAOSTOCK_AVAILABLE = True
except ImportError:
    _BAOSTOCK_AVAILABLE = False
    bs = None


# ---------------------------------------------------------------------------
# 代码规范化
# ---------------------------------------------------------------------------

def _ensure_baostock_code(code: str) -> str:
    """
    将常见格式转换为 baostock 格式。

    600000  → sh.600000
    000001  → sz.000001
    601869  → sh.601869
    sh.601869 → sh.601869
    """
    c = code.strip().lower().replace(".sh", "").replace(".sz", "")
    if c.startswith("sh") or c.startswith("sz"):
        return c
    if c.startswith("6"):
        return f"sh.{c}"
    if c.startswith(("0", "3")):
        return f"sz.{c}"
    return c


# ---------------------------------------------------------------------------
# 数据拉取
# ---------------------------------------------------------------------------

def fetch_drawdown_data(
    code: str,
    start: str | None = None,
    end: str | None = None,
    *,
    days: int = 365,
    adjustflag: str = "2",  # 2=前复权, 1=后复权, 3=不复权
) -> pd.DataFrame:
    """
    通过 baostock 拉取单票历史行情DataFrame。

    Args:
        code:      股票代码，如 "601869" 或 "sh.601869"
        start:     开始日期 YYYY-MM-DD，默认为 days 指定的天数前
        end:       结束日期 YYYY-MM-DD，默认为今天
        days:      当 start 未指定时，向前取多少天
        adjustflag: 复权方式 2=前复权 1=后复权 3=不复权

    Returns:
        DataFrame，列: date, code, open, high, low, close, volume, amount, turn
        按 date 升序排列
    """
    if not _BAOSTOCK_AVAILABLE:
        raise ImportError(
            "baostock 未安装，请先运行: pip install baostock"
        )

    bs_code = _ensure_baostock_code(code)
    code_name = _normalize_code_to_name(code)

    end_date = end or datetime.now().strftime("%Y-%m-%d")
    if start:
        start_date = start
    else:
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"baostock login failed: {lg.error_msg}")

    try:
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,code,open,high,low,close,volume,amount,turn",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag=adjustflag,
        )
        if rs.error_code != "0":
            raise RuntimeError(f"baostock query failed: {rs.error_msg}")

        data_list: list[dict[str, Any]] = []
        while rs.next():
            row = rs.get_row_data()
            data_list.append({
                "date": row[0],
                "code": row[1],
                "open": row[2],
                "high": row[3],
                "low": row[4],
                "close": row[5],
                "volume": row[6],
                "amount": row[7],
                "turn": row[8],
            })
    finally:
        bs.logout()

    df = pd.DataFrame(data_list)
    if df.empty:
        return df

    for col in ["open", "high", "low", "close", "volume", "amount", "turn"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


# ---------------------------------------------------------------------------
# 回撤计算
# ---------------------------------------------------------------------------

def compute_drawdown(df: pd.DataFrame) -> pd.DataFrame:
    """
    在 df 上追加回撤列（原地，复制传入）。

    新增列：
      peak        — 运行最高价
      drawdown    — 回撤额（close - peak）
      drawdown_pct — 回撤率 %（负值，-40 表示回撤 40%）
    """
    df = df.copy()
    df["peak"] = df["close"].cummax()
    df["drawdown"] = df["close"] - df["peak"]
    df["drawdown_pct"] = (df["close"] / df["peak"] - 1) * 100
    return df


# ---------------------------------------------------------------------------
# 指标提取
# ---------------------------------------------------------------------------

def extract_drawdown_metrics(df: pd.DataFrame) -> dict[str, Any]:
    """
    从带回撤列的 df 中提取关键指标。
    """
    if df.empty or "drawdown_pct" not in df.columns:
        return {}

    # 最大回撤
    max_dd_idx = df["drawdown_pct"].idxmin()
    max_dd_row = df.loc[max_dd_idx]

    # Top-5 回撤
    top5 = (
        df.nsmallest(5, "drawdown_pct")[
            ["date", "close", "peak", "drawdown", "drawdown_pct"]
        ]
        .to_dict("records")
    )

    # 月度统计
    df = df.copy()
    df["ym"] = df["date"].dt.to_period("M")
    monthly = df.groupby("ym").agg(
        open=("close", "first"),
        close=("close", "last"),
        high=("high", "max"),
        low=("low", "min"),
        vol=("volume", "sum"),
    )
    monthly["month_return"] = (monthly["close"] / monthly["open"] - 1) * 100

    # 月度日内最大回撤
    monthly_mdd = []
    for ym, grp in df.groupby("ym"):
        grp = grp.sort_values("date")
        grp["peak"] = grp["close"].cummax()
        grp["dd"] = (grp["close"] / grp["peak"] - 1) * 100
        monthly_mdd.append({"ym": ym, "month_max_dd": grp["dd"].min()})
    mdd_df = pd.DataFrame(monthly_mdd).set_index("ym")
    monthly = monthly.join(mdd_df)

    # 区间统计
    def _period_stats(sub, label):
        if len(sub) < 2:
            return {}
        return {
            f"{label}_start": sub["date"].iloc[0].strftime("%Y-%m-%d"),
            f"{label}_end": sub["date"].iloc[-1].strftime("%Y-%m-%d"),
            f"{label}_high": round(sub["high"].max(), 2),
            f"{label}_high_date": sub.loc[sub["high"].idxmax(), "date"].strftime("%Y-%m-%d"),
            f"{label}_low": round(sub["low"].min(), 2),
            f"{label}_low_date": sub.loc[sub["low"].idxmin(), "date"].strftime("%Y-%m-%d"),
            f"{label}_max_dd": round(sub["drawdown_pct"].min(), 2),
            f"{label}_return": round((sub["close"].iloc[-1] / sub["close"].iloc[0] - 1) * 100, 2),
            f"{label}_avg_vol": round(sub["volume"].astype(float).mean() / 1e4, 1),
        }

    periods = {}
    for label, n in [("m1", 21), ("m3", 63), ("m6", 126), ("full", len(df))]:
        sub = df.tail(n)
        periods.update(_period_stats(sub, label))

    return {
        "start_date": df["date"].iloc[0].strftime("%Y-%m-%d"),
        "end_date": df["date"].iloc[-1].strftime("%Y-%m-%d"),
        "trading_days": len(df),
        "start_price": round(float(df["close"].iloc[0]), 2),
        "end_price": round(float(df["close"].iloc[-1]), 2),
        "total_return_pct": round((float(df["close"].iloc[-1]) / float(df["close"].iloc[0]) - 1) * 100, 2),
        "max_drawdown_pct": round(float(max_dd_row["drawdown_pct"]), 2),
        "max_drawdown_date": max_dd_row["date"].strftime("%Y-%m-%d"),
        "max_drawdown_price": round(float(max_dd_row["close"]), 2),
        "max_drawdown_peak": round(float(max_dd_row["peak"]), 2),
        "max_drawdown_amount": round(float(max_dd_row["drawdown"]), 2),
        "top5_drawdowns": [
            {
                "date": str(r["date"]),
                "close": round(float(r["close"]), 2),
                "peak": round(float(r["peak"]), 2),
                "drawdown": round(float(r["drawdown"]), 2),
                "drawdown_pct": round(float(r["drawdown_pct"]), 2),
            }
            for r in top5
        ],
        "monthly": [
            {
                "ym": str(ym),
                "open": round(float(row["open"]), 2),
                "close": round(float(row["close"]), 2),
                "month_return": round(float(row["month_return"]), 2),
                "month_max_dd": round(float(row["month_max_dd"]), 2),
                "vol_wan": round(float(row["vol"]) / 1e4, 1),
            }
            for ym, row in monthly.iterrows()
        ],
        **periods,
    }


# ---------------------------------------------------------------------------
# 持久化
# ---------------------------------------------------------------------------

def _default_report_dir() -> Path:
    root = _PROJECT_ROOT / "data" / "backtest"
    root.mkdir(parents=True, exist_ok=True)
    return root


def persist_drawdown_csv(df: pd.DataFrame, code: str, end_date: str) -> str:
    """保存完整回撤序列为 CSV。"""
    out = df[["date", "open", "high", "low", "close", "volume", "peak", "drawdown", "drawdown_pct"]].copy()
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    path = _default_report_dir() / f"drawdown_{code}_{end_date}.csv"
    out.to_csv(path, index=False, encoding="utf-8-sig")
    return str(path)


def persist_drawdown_json(metrics: dict, code: str, name: str, end_date: str) -> str:
    """保存指标为 JSON。"""
    path = _default_report_dir() / f"drawdown_{code}_{end_date}.json"
    payload = {
        "command": "backtest",
        "action": "drawdown",
        "code": code,
        "name": name,
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        **metrics,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


# ---------------------------------------------------------------------------
# 格式化输出（供 CLI 打印）
# ---------------------------------------------------------------------------

def _safe(v) -> str:
    if v is None:
        return "N/A"
    if isinstance(v, float):
        return f"{v:.2f}"
    return str(v)


def render_drawdown_console(code: str, name: str, df: pd.DataFrame, metrics: dict) -> str:
    """生成控制台友好的文字报告。"""
    lines = [
        "",
        "=" * 58,
        f"  回撤分析  {code} {name}",
        f"  区间: {metrics['start_date']} ~ {metrics['end_date']} "
        f"（{metrics['trading_days']} 个交易日）",
        "=" * 58,
        "",
        f"  {'起始价':>8}   {'最新价':>8}   {'区间涨跌':>10}",
        f"  {metrics['start_price']:>8}   {metrics['end_price']:>8}   {metrics['total_return_pct']:>+9.2f}%",
        "",
        "-" * 58,
        f"  最大回撤  {metrics['max_drawdown_pct']:+.2f}%  "
        f"（{metrics['max_drawdown_date']}  峰:{metrics['max_drawdown_peak']} → {metrics['max_drawdown_price']}）",
        "-" * 58,
        "",
        "  Top-5 最大回撤日:",
        f"  {'日期':<12} {'收盘':>8} {'峰值':>8} {'回撤率':>8} {'回撤额':>8}",
    ]
    for r in metrics.get("top5_drawdowns", []):
        lines.append(
            f"  {r['date']:<12} {r['close']:>8.2f} {r['peak']:>8.2f} "
            f"{r['drawdown_pct']:>8.2f}% {r['drawdown']:>8.2f}"
        )

    lines.extend(["", "  区间统计:"])
    for label, key in [("近1月", "m1"), ("近3月", "m3"), ("近6月", "m6"), ("近1年", "full")]:
        prefix = f"  {label:6}"
        k = key
        if f"{k}_return" in metrics:
            lines.append(
                f"  {label:<6} 涨跌幅 {metrics[f'{k}_return']:>+8.2f}%  "
                f"最大回撤 {metrics[f'{k}_max_dd']:>7.2f}%  "
                f"日均成交 {metrics[f'{k}_avg_vol']:>8.1f} 万股"
            )

    lines.extend(["", "  月度表现:"])
    lines.append(f"  {'月份':<10} {'开盘':>8} {'收盘':>8} {'月涨跌':>8} {'月最大回撤':>10} {'成交量(万)':>10}")
    for m in metrics.get("monthly", []):
        ret = f"{m['month_return']:+.2f}%"
        mdd = f"{m['month_max_dd']:.2f}%"
        lines.append(
            f"  {m['ym']:<10} {m['open']:>8.2f} {m['close']:>8.2f} "
            f"{ret:>8} {mdd:>10} {m['vol_wan']:>10.1f}"
        )

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def run_drawdown_analysis(
    codes: list[str],
    start: str | None = None,
    end: str | None = None,
    *,
    days: int = 365,
    save_csv: bool = True,
    save_json: bool = True,
    name_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    对单只或多只股票跑回撤分析。

    Args:
        codes:    股票代码列表，如 ["601869"] 或 ["601869", "603803"]
        start:    开始日期 YYYY-MM-DD（默认: today - days）
        end:      结束日期 YYYY-MM-DD（默认: 今天）
        days:     start 未指定时向前取多少天（默认 365）
        save_csv: 是否保存 CSV 序列文件
        save_json: 是否保存 JSON 报告文件
        name_map: 代码→名称映射，如 {"601869": "长飞光纤"}

    Returns:
        CLI 兼容结果字典，含 results[] 列表，每项对应一只股票
    """
    name_map = name_map or {}
    end_date = end or datetime.now().strftime("%Y-%m-%d")

    results: list[dict[str, Any]] = []

    for code in codes:
        try:
            df = fetch_drawdown_data(code, start, end, days=days)
            if df.empty:
                results.append({
                    "code": code,
                    "name": name_map.get(code, _normalize_code_to_name(code)),
                    "status": "error",
                    "error": "无数据，可能是代码或日期范围有误",
                })
                continue

            df = compute_drawdown(df)
            metrics = extract_drawdown_metrics(df)
            name = name_map.get(code, _normalize_code_to_name(code))

            artifacts: dict[str, str] = {}
            if save_csv:
                artifacts["csv"] = persist_drawdown_csv(df, code, end_date)
            if save_json:
                artifacts["json"] = persist_drawdown_json(metrics, code, name, end_date)

            results.append({
                "command": "backtest",
                "action": "drawdown",
                "status": "ok",
                "code": code,
                "name": name,
                "report": render_drawdown_console(code, name, df, metrics),
                "metrics": metrics,
                "artifacts": artifacts,
            })

        except Exception as exc:
            results.append({
                "command": "backtest",
                "action": "drawdown",
                "status": "error",
                "code": code,
                "name": name_map.get(code, code),
                "error": str(exc),
            })

    overall_status = (
        "ok" if all(r["status"] == "ok" for r in results)
        else "error" if all(r["status"] == "error" for r in results)
        else "partial"
    )

    return {
        "command": "backtest",
        "action": "drawdown",
        "status": overall_status,
        "count": len(codes),
        "ok_count": sum(1 for r in results if r["status"] == "ok"),
        "results": results,
    }


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------

_CODE_NAME_MAP = {
    "sh.601869": "长飞光纤",
    "sz.000001": "平安银行",
    "sh.600036": "招商银行",
    "sh.600519": "贵州茅台",
    "sz.300750": "宁德时代",
    "sh.601318": "中国平安",
    "sh.600000": "浦发银行",
    "sz.000002": "万科A",
}


def _normalize_code_to_name(code: str) -> str:
    c = _ensure_baostock_code(code)
    return _CODE_NAME_MAP.get(c, c.upper())
