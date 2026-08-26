"""通达信行情(tdxgo)内置数据源插件(基于 injoyai/tdx 的 Go 桥接)。"""

from app.plugins.tdxgo.bridge import TdxGoBridgeError, availability, run_job
from app.plugins.tdxgo.provider import TdxGoProvider

PROVIDER_NAME = "tdxgo"

__all__ = [
    "PROVIDER_NAME",
    "TdxGoProvider",
    "availability",
    "run_job",
    "TdxGoBridgeError",
]
