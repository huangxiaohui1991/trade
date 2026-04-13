#!/usr/bin/env python3
"""
utils/common.py — 跨模块公共工具函数

统一所有模块中共用的辅助函数，避免重复定义。
当前包含：
  - _safe_float: 安全转浮点数（带字符串清洗）
  - _safe_int: 安全转整数
"""

import re
from typing import Any


def _safe_float(value: Any, default: float = 0.0) -> float:
    """
    安全将值转换为浮点数。

    处理 None、空字符串、已清洗的数字字符串（¥/%/,/**）。
    用于解析配置文件、市场数据、Obsidian 元数据中的数字字段。

    Args:
        value: 待转换的值（str / int / float / None）
        default: 转换失败时的默认值

    Returns:
        float，转换失败返回 default
    """
    try:
        if value in (None, ""):
            return default
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            # 清洗常见干扰字符：货币符、百分比、逗号、Obsidian bold 标记
            cleaned = (
                value.replace("¥", "")
                .replace("%", "")
                .replace(",", "")
                .replace("**", "")
                .strip()
            )
            if not cleaned:
                return default
            # 特殊：中文 "亏"（亏损）→ 无 "-" 时转为负数
            negative = "亏" in cleaned and "-" not in cleaned
            cleaned = re.sub(r"[^\d.\-]", "", cleaned)
            if cleaned in ("", "-", ".", "-."):
                return default
            number = float(cleaned)
            return -abs(number) if negative else number
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    """
    安全将值转换为整数。

    内部复用 _safe_float，因此同样支持带清洗的字符串。

    Args:
        value: 待转换的值（str / int / float / None）
        default: 转换失败时的默认值

    Returns:
        int，转换失败返回 default
    """
    result = _safe_float(value, None)
    if result is None:
        return default
    return int(result)
