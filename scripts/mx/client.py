"""
mx/client.py — 妙想 API 公共客户端基础

提供：
  - load_env(): 从项目 .env 加载环境变量
  - get_apikey(): 获取 MX_APIKEY
  - MXBaseClient: 所有妙想客户端的基类
"""

import os
import requests
from pathlib import Path
from typing import Dict, Any, Optional

from scripts.utils.logger import get_logger

_logger = get_logger("mx.client")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def load_env():
    """从项目根目录 .env 文件加载环境变量（不覆盖已有值）"""
    env_path = _PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            if key and key not in os.environ:
                os.environ[key] = value


def get_apikey() -> str:
    """获取 MX_APIKEY，优先环境变量，其次 .env 文件"""
    load_env()
    apikey = os.environ.get("MX_APIKEY", "")
    if not apikey:
        raise ValueError(
            "MX_APIKEY 未配置。请在项目根目录 .env 文件中设置：\n"
            "MX_APIKEY=your_api_key_here"
        )
    return apikey


class MXBaseClient:
    """妙想 API 基类"""

    BASE_URL = "https://mkapi2.dfcfs.com/finskillshub"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or get_apikey()

    def _post(self, endpoint: str, data: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
        """统一 POST 请求"""
        url = f"{self.BASE_URL}{endpoint}"
        headers = {
            "Content-Type": "application/json",
            "apikey": self.api_key,
        }
        try:
            resp = requests.post(url, headers=headers, json=data, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            _logger.error(f"[MX] 请求超时: {endpoint}")
            return {"status": -1, "message": "请求超时"}
        except requests.exceptions.RequestException as e:
            _logger.error(f"[MX] 请求失败: {endpoint} - {e}")
            return {"status": -1, "message": str(e)}
