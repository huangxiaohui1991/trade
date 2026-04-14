"""iwencai 请求头 — 生成 hexin-v token（Node.js runtime）"""
import subprocess, os

_BUNDLE_PATH = os.path.join(os.path.dirname(__file__), "hexin-v.bundle.js")

def get_token() -> str:
    result = subprocess.run(
        ["node", _BUNDLE_PATH],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        timeout=10,
    )
    return result.stdout.decode().strip()

def build_headers(cookie: str, user_agent: str = None) -> dict:
    if user_agent is None:
        from fake_useragent import UserAgent
        user_agent = UserAgent().random
    return {
        "hexin-v": get_token(),
        "User-Agent": user_agent,
        "cookie": cookie,
    }
