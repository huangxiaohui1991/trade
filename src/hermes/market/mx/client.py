"""
market/mx/client.py — 妙想 API 基础客户端

提供 MXBaseClient（同步 requests）和 env 加载。
从 V1 scripts/mx/client.py 迁移。
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import requests

_logger = logging.getLogger(__name__)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent


def load_env():
    """从项目根目录 .env 加载环境变量（不覆盖已有值）。"""
    env_path = _PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            if key and key not in os.environ:
                os.environ[key] = value


def get_apikey() -> str:
    load_env()
    apikey = os.environ.get("MX_APIKEY", "")
    if not apikey:
        raise ValueError("MX_APIKEY 未配置")
    return apikey


class MXBaseClient:
    """妙想 finskillshub API 基类。"""

    BASE_URL = "https://mkapi2.dfcfs.com/finskillshub"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or get_apikey()

    def _post(self, endpoint: str, data: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{endpoint}"
        headers = {"Content-Type": "application/json", "apikey": self.api_key}
        try:
            resp = requests.post(url, headers=headers, json=data, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            return {"status": -1, "message": "请求超时"}
        except requests.exceptions.RequestException as e:
            return {"status": -1, "message": str(e)}
