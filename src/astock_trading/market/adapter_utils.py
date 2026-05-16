"""Shared helpers for market data adapters."""

from __future__ import annotations

import re

def _to_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        if isinstance(value, str):
            value = value.strip().replace("%", "").replace(",", "")
            if value in {"", "-", "nan", "None"}:
                return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _normalize_a_stock_code(code: str) -> str:
    code = code.strip().lower()
    if code.endswith((".sh", ".sz", ".bj")):
        return code[:6]
    if code.startswith(("sh", "sz", "bj")):
        return code[2:]
    return code


def _normalize_xueqiu_symbol(symbol: str) -> str:
    raw = symbol.strip()
    lower = raw.lower()
    if lower.startswith(("sh", "sz", "bj")) and lower[2:].isdigit():
        return lower[2:]
    return raw


def _extract_a_stock_code(value: object) -> str:
    text = str(value or "")
    match = re.search(r"(?<!\d)(\d{6})(?!\d)", text)
    return match.group(1) if match else ""


def _normalize_opencli_a_stock_symbol(symbol: object, tags: object = "") -> str:
    raw = str(symbol or "").strip()
    lower = raw.lower()
    if lower.startswith(("sh", "sz", "bj")) and lower[2:].isdigit():
        return lower[2:8]
    if raw.isdigit() and len(raw) >= 6:
        return raw[:6]
    return _extract_a_stock_code(tags)


def _xueqiu_symbol(code_or_symbol: str) -> str:
    raw = str(code_or_symbol or "").strip()
    lower = raw.lower()
    if lower.startswith(("sh", "sz", "bj")):
        return raw.upper()
    code = _normalize_a_stock_code(raw)
    if code.isdigit() and len(code) == 6:
        if code.startswith(("6", "9")):
            return f"SH{code}"
        if code.startswith("8"):
            return f"BJ{code}"
        return f"SZ{code}"
    return raw


def _split_tags(value: object) -> list[str]:
    tags = []
    for item in re.split(r"[,，/、\s]+", str(value or "")):
        tag = item.strip()
        if not tag or re.fullmatch(r"\d{6}", tag):
            continue
        tags.append(tag)
    return tags


def _parse_heat_value(value: object) -> int:
    text = str(value or "").replace(",", "").strip()
    if not text:
        return 0
    match = re.search(r"([+-]?\d+(?:\.\d+)?)\s*(亿|万)?", text)
    if not match:
        return _to_int(value)
    number = float(match.group(1))
    unit = match.group(2)
    if unit == "亿":
        number *= 100000000
    elif unit == "万":
        number *= 10000
    return int(number)


def _a_stock_prefix(code: str) -> str:
    code = _normalize_a_stock_code(code)
    if code.startswith(("6", "9")):
        return f"sh{code}"
    if code.startswith("8"):
        return f"bj{code}"
    return f"sz{code}"


def is_hk_code(code: str) -> bool:
    """判断是否为港股代码。

    港股代码规则：
    - 5 位纯数字且以 0 开头（如 09927, 00700, 01810）
    - 或显式带 hk 前缀（如 hk09927）
    """
    code = code.strip().lower()
    if code.startswith("hk"):
        return True
    # 5 位数字且以 0 开头 → 港股
    if len(code) == 5 and code.isdigit() and code.startswith("0"):
        # 排除 A 股深市 00xxx 开头的（深市主板 000xxx 是 6 位）
        # 5 位且 0 开头 → 港股
        return True
    return False


def normalize_hk_code(code: str) -> str:
    """标准化港股代码为 5 位纯数字（去掉 hk 前缀）。"""
    code = code.strip().lower()
    if code.startswith("hk"):
        code = code[2:]
    return code.zfill(5)


# ---------------------------------------------------------------------------
# AkShare 港股 Adapters
