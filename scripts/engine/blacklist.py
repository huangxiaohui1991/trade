#!/usr/bin/env python3
"""
engine/blacklist.py — 黑名单排雷模块

职责：
  - check_stock(code) → 是否在黑名单
  - batch_check(codes) → 批量返回
  - filter_blacklist(stocks) → 过滤后返回干净/雷区/警示三组

  排雷维度：
    1. ST/*ST 退市风险
    2. 涨跌停不可追（今日涨停→雷，跌停→警示）
    3. 流动性风险（近5日均成交额<1000万→警示）
    4. PE 过高警示（PE>100）

数据源：akshare（重试机制，每次失败最多重试2次）
"""

import os
import sys
import time
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

import pandas as pd
import akshare as ak

from scripts.utils.logger import get_logger

_logger = get_logger("blacklist")


# ─────────────────────────────────────────
# 数据结构
# ─────────────────────────────────────────

@dataclass
class BlacklistResult:
    code: str
    name: str = ""
    is_blacklist: bool = False    # 🚨 雷区
    is_caution: bool = False      # ⚠️ 警示
    reasons: list = field(default_factory=list)
    details: dict = field(default_factory=dict)

    def summary(self) -> str:
        if self.is_blacklist:
            return f"🚨 雷 [{self.code}] {self.name}: {'; '.join(self.reasons)}"
        elif self.is_caution:
            return f"⚠️ 警示 [{self.code}] {self.name}: {'; '.join(self.reasons)}"
        return f"✅ [{self.code}] {self.name} 无黑名单"


# ─────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────

def _retry(call_fn, fallback=None, retries: int = 2, delay: float = 1.0):
    """akshare 不稳定时重试"""
    for i in range(retries):
        try:
            return call_fn()
        except Exception as e:
            if i == retries - 1:
                _logger.debug(f"重试{retries}次仍失败: {e}")
                return fallback() if fallback else None
            time.sleep(delay)


# ─────────────────────────────────────────
# 1. ST / *ST 检查
# ─────────────────────────────────────────

def check_st_risk(code: str) -> BlacklistResult:
    """检查是否 ST/*ST/退市风险（MX优先 → akshare fallback）"""
    result = BlacklistResult(code=code)

    # MX 优先：查股票名称判断 ST
    try:
        from scripts.mx.mx_data import MXData
        mx = MXData()
        mx_result = mx.query(f"{code} 名称")
        data = mx_result.get("data", {}).get("data", {}).get("searchDataResultDTO", {})
        tags = data.get("entityTagDTOList", [])
        if tags:
            name = tags[0].get("fullName", "")
            if name:
                result.name = name
                if "ST" in name.upper() or "退" in name:
                    result.is_blacklist = True
                    result.reasons.append(f"ST/退市风险: {name}")
                    result.details["st_type"] = name
                return result
    except Exception:
        pass

    # akshare fallback
    def _do():
        df = ak.stock_zh_a_st_em()
        if df is None or df.empty:
            return None
        code_suffix = code[-6:]
        match = df[df["代码"].astype(str).str.endswith(code_suffix)]
        if match.empty:
            return None
        row = match.iloc[-1]
        st_type = str(row.get("类型", "")).strip()
        if not st_type or st_type in ("nan", "正常"):
            return None
        result.is_blacklist = True
        result.reasons.append(f"ST类型: {st_type}")
        result.details["st_type"] = st_type
        result.name = str(row.get("名称", code))
        return True

    try:
        _retry(lambda: _do())
    except Exception as e:
        _logger.debug(f"ST检查失败 {code}: {e}")
    return result


# ─────────────────────────────────────────
# 2. 涨跌停检查
# ─────────────────────────────────────────

def check_limit_up_down(code: str, days: int = 10) -> BlacklistResult:
    """检查近期是否涨停/跌停"""
    result = BlacklistResult(code=code)

    def _do():
        df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
        if df is None or df.empty:
            return None
        df = df.tail(days)
        if df.empty:
            return None

        name = str(df.iloc[-1].get("名称", ""))
        if name == "nan":
            name = code
        result.name = name

        today = df.iloc[-1]
        chg_pct = float(str(today.get("涨跌幅", "0")).replace("%", ""))

        if chg_pct >= 9.5:
            result.is_blacklist = True
            result.reasons.append(f"今日涨停({chg_pct:+.1f}%)")
            result.details["today_chg"] = chg_pct
            return True

        if chg_pct <= -9.5:
            result.is_caution = True
            result.reasons.append(f"今日跌停({chg_pct:+.1f}%)")
            result.details["today_chg"] = chg_pct
            return True

        limit_count = sum(
            1 for _, r in df.iterrows()
            if abs(float(str(r.get("涨跌幅", 0)).replace("%", ""))) >= 9.5
        )
        if limit_count >= 3:
            result.is_caution = True
            result.reasons.append(f"近{days}日{limit_count}次涨停/跌停，波动剧烈")
            result.details["limit_count"] = limit_count
        return True

    try:
        _retry(lambda: _do())
    except Exception as e:
        _logger.debug(f"涨跌停检查失败 {code}: {e}")
    return result


# ─────────────────────────────────────────
# 3. 流动性检查
# ─────────────────────────────────────────

def check_liquidity(code: str, days: int = 5, threshold_wan: float = 1000) -> BlacklistResult:
    """检查近N日平均成交额是否低于 threshold_wan（万元）"""
    result = BlacklistResult(code=code)

    def _do():
        df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
        if df is None or df.empty:
            return None
        df = df.tail(days)
        if df.empty:
            return None

        amount_col = next((c for c in df.columns if "成交额" in c or "金额" in c), None)
        if amount_col is None:
            return None

        avg = pd.to_numeric(df[amount_col], errors="coerce").mean()
        if avg < threshold_wan:
            result.is_caution = True
            result.reasons.append(f"近{days}日均成交额{avg/10000:.1f}万<{threshold_wan/10000}千万，流动性差")
            result.details["avg_amount_wan"] = round(avg, 2)
        return True

    try:
        _retry(lambda: _do())
    except Exception as e:
        _logger.debug(f"流动性检查失败 {code}: {e}")
    return result


# ─────────────────────────────────────────
# 4. PE 过高检查
# ─────────────────────────────────────────

def check_pe_warning(code: str, threshold: float = 100) -> BlacklistResult:
    """PE > threshold 警示（非强制排除）"""
    result = BlacklistResult(code=code)
    try:
        from scripts.engine.data_engine import DataEngine
        engine = DataEngine()
        rt = engine.get_realtime(code)
        pe = rt.get("pe")
        name = rt.get("name", "")
        if name:
            result.name = name
        if pe and pe > threshold:
            result.is_caution = True
            result.reasons.append(f"PE-TTM={pe:.0f}偏高(>{threshold})")
            result.details["pe"] = round(pe, 1)
    except Exception as e:
        _logger.debug(f"PE检查失败 {code}: {e}")
    return result


# ─────────────────────────────────────────
# 汇总
# ─────────────────────────────────────────

def check_stock(code: str, name: str = "") -> BlacklistResult:
    """对单只股票执行全套黑名单检查"""
    result = BlacklistResult(code=code, name=name)

    checks = [
        check_st_risk(code),
        check_limit_up_down(code),
        check_liquidity(code),
        check_pe_warning(code),
    ]

    for chk in checks:
        if chk.is_blacklist:
            result.is_blacklist = True
            for r in chk.reasons:
                if r not in result.reasons:
                    result.reasons.append(r)
        if chk.is_caution:
            result.is_caution = True
            for r in chk.reasons:
                if r not in result.reasons:
                    result.reasons.append(r)
        if chk.name and not result.name:
            result.name = chk.name
        result.details.update(chk.details)

    return result


def batch_check(stocks: list) -> list:
    """批量黑名单检查"""
    results = []
    for item in stocks:
        if isinstance(item, dict):
            code = str(item.get("code", "")).strip()
            name = str(item.get("name", "")).strip()
        else:
            code = str(item).strip()
            name = ""
        if code:
            results.append(check_stock(code, name))
    return results


def filter_blacklist(stocks: list) -> tuple:
    """过滤黑名单，返回 (clean, blacklist, caution)"""
    results = batch_check(stocks)
    clean, blacklist, caution = [], [], []
    for r in results:
        if r.is_blacklist:
            blacklist.append(r)
        elif r.is_caution:
            caution.append(r)
        else:
            clean.append(r)
    return clean, blacklist, caution


# ─────────────────────────────────────────
# 报告格式化
# ─────────────────────────────────────────

def format_report(blacklist: list, caution: list) -> str:
    """生成黑名单报告"""
    today = datetime.now().strftime("%Y-%m-%d")
    lines = [f"# 黑名单排雷报告 — {today}", ""]

    if blacklist:
        lines.append("## 🚨 雷区股票（建议排除）")
        for r in blacklist:
            lines.append(f"- **{r.name}({r.code})**：{'；'.join(r.reasons)}")
        lines.append("")
    else:
        lines.append("## ✅ 无雷区股票")

    if caution:
        lines.append("## ⚠️ 警示股票（注意风险）")
        for r in caution:
            lines.append(f"- **{r.name}({r.code})**：{'；'.join(r.reasons)}")
        lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    test_stocks = [
        {"code": "002487", "name": "大金重工"},
        {"code": "002353", "name": "杰瑞股份"},
        {"code": "300870", "name": "欧陆通"},
    ]
    clean, bl, caution = filter_blacklist(test_stocks)
    for r in bl:
        print(r.summary())
    for r in caution:
        print(r.summary())
    for r in clean:
        print(r.summary())
    print()
    print(format_report(bl, caution))
