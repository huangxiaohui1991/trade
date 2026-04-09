"""
妙想 Skills 集成模块（东方财富）

基于东方财富妙想 API，提供：
  - MXData: 金融数据查询（行情/财务/关系）
  - MXSearch: 资讯搜索（研报/新闻/公告）
  - MXXuangu: 智能选股
  - MXZixuan: 自选股管理
  - MXMoni: 模拟交易

所有模块共享 MX_APIKEY 环境变量，通过 .env 文件加载。
"""

from scripts.mx.client import get_apikey, load_env
