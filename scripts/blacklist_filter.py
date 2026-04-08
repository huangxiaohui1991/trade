#!/usr/bin/env python3
"""
blacklist_filter.py — 黑名单排雷脚本
调用 akshare 对候选股票进行风险筛查，排除：
  1. 质押率过高（>30%）→ 大股东套现风险
  2. 近期解禁比例过高（>5%）→ 抛压风险
  3. 立案调查 / 监管警示 → 合规风险

用法:
  python blacklist_filter.py --codes 002487,002353,300870
  python blacklist_filter.py --file data/04-选股/筛选结果/筛选结果_20260408.csv
  python blacklist_filter.py --codes 002487,002353 --output blacklist_report.md
"""

import os
import sys
import json
import warnings
import argparse
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(os.path.abspath(__file__)).parent
sys.path.insert(0, str(SCRIPT_DIR))

EM_API_KEY = os.environ.get("EM_API_KEY", "")
warnings.filterwarnings("ignore")


# ─────────────────────────────────────────
# 雷区检测函数
# ─────────────────────────────────────────

def check_pledge_ratio(code: str) -> dict:
    """
    检查质押率（akshare: stock_share_pledge_em）
    质押比例 > 30% → 雷
    """
    try:
        import akshare as ak
        df = ak.stock_share_pledge_em(symbol=code)
        if df is None or df.empty:
            return {"status": "unknown", "detail": "无质押数据"}
        
        # 找最新一期质押比例
        pledge_cols = [c for c in df.columns if "质押比例" in c or "质押率" in c or "占总股本" in c]
        if not pledge_cols:
            return {"status": "unknown", "detail": f"质押字段不可用，列:{df.columns.tolist()}"}
        
        latest_pledge = df.iloc[-1]
        pledge_val = None
        for col in pledge_cols:
            try:
                v = float(str(latest_pledge[col]).replace("%", "").replace(",", ""))
                pledge_val = v
                break
            except Exception:
                continue
        
        if pledge_val is None:
            return {"status": "unknown", "detail": "质押比例解析失败"}
        
        return {
            "status": "risk" if pledge_val > 30 else "ok",
            "pledge_ratio": pledge_val,
            "threshold": 30,
            "detail": f"质押比例 {pledge_val:.1f}%{' ⚠️>30%' if pledge_val > 30 else ''}"
        }
    except Exception as e:
        return {"status": "error", "detail": f"查询失败: {str(e)[:50]}"}


def check_unlock_schedule(code: str) -> dict:
    """
    检查解禁表（akshare: stock_locked_em）
    未来30日内解禁市值占总股本 >5% → 雷
    """
    try:
        import akshare as ak
        # 尝试获取解禁数据
        df = ak.stock_locked_em(symbol=code)
        if df is None or df.empty:
            return {"status": "unknown", "detail": "无解禁数据"}
        
        today = datetime.now()
        cutoff = today + timedelta(days=30)
        unlock_cols = [c for c in df.columns if "解禁" in c or "日期" in c or "数量" in c or "市值" in c]
        
        # 筛选近期解禁
        recent_unlock = []
        for _, row in df.iterrows():
            for col in unlock_cols:
                val = str(row.get(col, ""))
                if any(k in col for k in ["日期", "时间"]) and val:
                    try:
                        dt = datetime.strptime(val[:10], "%Y-%m-%d")
                        if today <= dt <= cutoff:
                            recent_unlock.append(row.to_dict())
                    except Exception:
                        continue
        
        if not recent_unlock:
            return {"status": "ok", "detail": f"未来30日无解禁"}
        
        total_ratio = sum(float(str(r.get(col, 0)).replace("%", "").replace(",", ""))
                        for r in recent_unlock for col in unlock_cols
                        if any(k in col for k in ["占总股本", "解禁比例"]))
        
        return {
            "status": "risk" if total_ratio > 5 else "ok",
            "unlock_ratio": total_ratio,
            "threshold": 5,
            "count": len(recent_unlock),
            "detail": f"30日内解禁 {len(recent_unlock)}批次，合计{total_ratio:.1f}%{' ⚠️>5%' if total_ratio > 5 else ''}"
        }
    except Exception as e:
        return {"status": "error", "detail": f"查询失败: {str(e)[:50]}"}


def check_investigation(code: str) -> dict:
    """
    检查立案调查/监管警示（akshare: stock_zh_a_disclosure_st_em）
    有记录 → 雷
    """
    try:
        import akshare as ak
        df = ak.stock_zh_a_disclosure_st_em(symbol=code)
        if df is None or df.empty:
            return {"status": "ok", "detail": "无监管记录"}
        
        # 检查近1年内是否有新增记录
        one_year_ago = datetime.now() - timedelta(days=365)
        recent = []
        for _, row in df.iterrows():
            date_col = next((c for c in df.columns if "日期" in c or "时间" in c), None)
            if date_col:
                try:
                    dt = datetime.strptime(str(row[date_col])[:10], "%Y-%m-%d")
                    if dt >= one_year_ago:
                        recent.append(row.to_dict())
                except Exception:
                    continue
        
        if recent:
            return {
                "status": "risk",
                "recent_count": len(recent),
                "detail": f"近1年有{len(recent)}条监管记录 ⚠️"
            }
        
        return {"status": "ok", "detail": "近1年无新增监管记录"}
    except Exception as e:
        return {"status": "error", "detail": f"查询失败: {str(e)[:50]}"}


def check_financial_warning(code: str) -> dict:
    """
    检查财务警示（*ST、退市风险等）
    有 *ST 或 退市 风险提示 → 雷
    """
    try:
        import akshare as ak
        # 业绩预警
        try:
            df = ak.stock_warning_em()
            if df is not None and not df.empty:
                match = df[df["股票代码"].astype(str).str.contains(code[-6:])]
                if not match.empty:
                    reason = match.iloc[0].get("警告类型", "退市风险")
                    return {
                        "status": "risk",
                        "detail": f"财务警示: {reason} ⚠️"
                    }
        except Exception:
            pass
        
        return {"status": "ok", "detail": "无财务风险"}
    except Exception:
        return {"status": "unknown", "detail": "无法查询财务预警"}


def check_shortable(code: str) -> dict:
    """
    检查融券做空比例（高融券余额 = 潜在做空压力）
    融券余额占流通市值 >1% → 注意
    """
    try:
        import akshare as ak
        df = ak.stock_margin_hgt_em(start_date="最近1月")
        if df is None or df.empty:
            return {"status": "unknown", "detail": "无融资融券数据"}
        match = df[df["股票代码"].astype(str).str.contains(code[-6:])]
        if not match.empty:
            latest = match.iloc[-1]
            margin_ratio = float(str(latest.get("融券余额", 0)).replace(",", "")) / 1e8
            return {
                "status": "caution" if margin_ratio > 1 else "ok",
                "margin_balance": margin_ratio,
                "detail": f"融券余额 {margin_ratio:.2f}亿{' ⚠️注意' if margin_ratio > 1 else ''}"
            }
        return {"status": "unknown", "detail": "无该股融券数据"}
    except Exception:
        return {"status": "unknown", "detail": "融券数据查询失败"}


# ─────────────────────────────────────────
# 主检测函数
# ─────────────────────────────────────────

def check_stock(code: str, name: str = "") -> dict:
    """对单只股票执行全套黑名单检测"""
    print(f"  [{code}] {name}...", end=" ", flush=True)
    
    pledge = check_pledge_ratio(code)
    unlock = check_unlock_schedule(code)
    investigation = check_investigation(code)
    warning = check_financial_warning(code)
    
    all_checks = [pledge, unlock, investigation, warning]
    has_risk = any(c["status"] == "risk" for c in all_checks)
    has_error = all(c["status"] == "error" for c in all_checks)
    
    result = {
        "code": code,
        "name": name,
        "is_blacklist": has_risk,
        "checks": {
            "质押率": pledge,
            "解禁": unlock,
            "立案调查": investigation,
            "财务警示": warning,
        },
        "risk_reasons": [c["detail"] for c in all_checks if c["status"] == "risk"],
        "summary": "🚨 雷" if has_risk else ("⚠️ 数据异常" if has_error else "✅ 通过")
    }
    
    print(result["summary"])
    return result


def check_stocks(codes: list) -> list:
    """批量检测多只股票"""
    results = []
    for item in codes:
        if isinstance(item, dict):
            code, name = item.get("code", ""), item.get("name", "")
        else:
            code, name = str(item), ""
        if code:
            results.append(check_stock(code, name))
    return results


# ─────────────────────────────────────────
# 报告生成
# ─────────────────────────────────────────

def format_report(results: list) -> str:
    """生成 Markdown 排雷报告"""
    today = datetime.now().strftime("%Y-%m-%d")
    risky = [r for r in results if r["is_blacklist"]]
    clean = [r for r in results if not r["is_blacklist"]]
    
    lines = [
        f"# 黑名单排雷报告 — {today}",
        "",
        f"**检测股票**: {len(results)} 只",
        f"**发现雷区**: {len(risky)} 只",
        f"**安全候选**: {len(clean)} 只",
        "",
        "---",
    ]
    
    if risky:
        lines.append("")
        lines.append("## 🚨 雷区股票（建议排除）")
        lines.append("")
        lines.append("| 代码 | 名称 | 风险类型 | 原因 |")
        lines.append("|------|------|---------|------|")
        for r in risky:
            reasons_str = " ".join([f"`{c}`" for c in r["risk_reasons"]])
            checks = r.get("checks", {})
            risk_types = [k for k, v in checks.items() if v.get("status") == "risk"]
            lines.append(f"| {r['code']} | {r['name']} | {', '.join(risk_types)} | {reasons_str} |")
    
    if clean:
        lines.append("")
        lines.append(f"## ✅ 安全候选（{len(clean)} 只）")
        lines.append("")
        for r in clean:
            checks = r.get("checks", {})
            ok_checks = [f"✅ {k}: {v.get('detail','')}" for k, v in checks.items() if v.get("status") == "ok"]
            unknown_checks = [f"⚠️ {k}: {v.get('detail','')}" for k, v in checks.items() if v.get("status") in ("unknown", "error")]
            lines.append(f"### {r['name']}（{r['code']}）")
            for c in ok_checks + unknown_checks:
                lines.append(f"- {c}")
            lines.append("")
    
    return "\n".join(lines)


# ─────────────────────────────────────────
# 主入口
# ─────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="黑名单排雷检测")
    parser.add_argument("--codes", type=str, help="逗号分隔的股票代码，如 002487,002353")
    parser.add_argument("--file", type=str, help="从CSV文件读取股票代码（需要name列）")
    parser.add_argument("--output", "-o", type=str, default=None, help="报告保存路径")
    args = parser.parse_args()
    
    # 加载股票列表
    stocks = []
    if args.codes:
        for item in args.codes.split(","):
            item = item.strip()
            if item:
                stocks.append({"code": item, "name": ""})
    elif args.file:
        if os.path.exists(args.file):
            df = pd.read_csv(args.file)
            for _, row in df.iterrows():
                stocks.append({"code": str(row.get("代码", row.get("code", ""))).strip(), "name": str(row.get("名称", row.get("name", ""))).strip()})
        else:
            print(f"❌ 文件不存在: {args.file}")
            sys.exit(1)
    else:
        print("❌ 请指定 --codes 或 --file")
        sys.exit(1)
    
    if not stocks:
        print("❌ 没有找到股票")
        sys.exit(1)
    
    print(f"\n🔍 开始黑名单排雷检测，共 {len(stocks)} 只股票...")
    print()
    
    results = check_stocks(stocks)
    
    report = format_report(results)
    print()
    print(report)
    
    if args.output:
        out_path = args.output
    else:
        out_dir = SCRIPT_DIR.parent / "data" / "04-选股" / "筛选结果"
        os.makedirs(out_dir, exist_ok=True)
        out_path = out_dir / f"黑名单排雷报告_{datetime.now().strftime('%Y%m%d_%H%M')}.md"
    
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"\n💾 报告已保存: {out_path}")
