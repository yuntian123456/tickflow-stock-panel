"""通达信行情(eltdx)内置数据源插件(纯 Python 通达信在线行情客户端)。"""

from app.plugins.eltdx.bridge import availability
from app.plugins.eltdx.provider import EltdxProvider

PROVIDER_NAME = "eltdx"

__all__ = [
    "PROVIDER_NAME",
    "EltdxProvider",
    "availability",
]
