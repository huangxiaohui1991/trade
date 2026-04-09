"""
scripts.pipeline — 每日自动化流水线

各流水线通过 cron 调度执行：
  morning.py   — 盘前流程（8:25，周一～五）
  noon.py     — 午休检查（11:55，周一～五）
  evening.py  — 收盘流程（15:35，周一～五）
  core_pool_scoring.py — 核心池评分（15:40，周一～五）
  weekly_review.py — 周报（周日20:00）

用法（CLI）：
  python -m scripts.pipeline.morning
  python -m scripts.pipeline.noon
  python -m scripts.pipeline.evening
  python -m scripts.pipeline.core_pool_scoring
  python -m scripts.pipeline.weekly_review
"""

from scripts.pipeline.morning import run as morning_run
from scripts.pipeline.noon import run as noon_run
from scripts.pipeline.evening import run as evening_run
from scripts.pipeline.core_pool_scoring import run as scoring_run
from scripts.pipeline.weekly_review import run as weekly_run

__all__ = [
    "morning_run",
    "noon_run",
    "evening_run",
    "scoring_run",
    "weekly_run",
]
