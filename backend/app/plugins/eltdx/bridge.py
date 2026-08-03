"""eltdx 插件可用性检测。

eltdx 是纯 Python 通达信在线行情客户端, 只需 import 成功即可认为可用
(建连在首次请求时进行, 失败会由 provider 内部按 symbol 降级为日志告警)。
"""

from __future__ import annotations


def availability() -> tuple[bool, str]:
    """返回 (是否可用, 原因)。不抛异常。"""
    try:
        import eltdx  # noqa: F401

        return True, "ok"
    except ImportError:
        return False, "未安装 eltdx, 请点击「安装」或手动执行: pip install eltdx"
