"""tdxgo 内置数据源 provider 契约测试。

不依赖真实网络/Go 二进制: 通过替换 provider 模块内的 run_job 返回预设行,
覆盖插件声明/注册、symbol 转换、各数据集归一化、分钟时间口径、realtime 小数制等。
"""

from __future__ import annotations

from datetime import datetime

import polars as pl
import pytest

from app.data_providers.custom import loader as cs_loader
from app.plugins.tdxgo import provider as tdxgo_provider
from app.plugins.tdxgo import bridge as tdxgo_bridge
from app.plugins.tdxgo.bridge import availability, run_job
from app.plugins.tdxgo.provider import TdxGoProvider, _to_symbol, _to_tdx


# ---- 插件发现与注册 ----


def test_plugin_discovered_in_loader():
    cs_loader._load_builtin_plugins()
    plugins = {p["name"]: p for p in cs_loader.list_plugins()}
    manifest = plugins.get("tdxgo")
    assert manifest is not None
    assert manifest["runtime"] == "none"
    assert manifest["datasets"] == ["daily", "adj_factor", "minute", "realtime"]
    assert "financial" not in manifest["datasets"]


def test_plugin_registered_when_available(monkeypatch):
    entry_map = {
        "app.plugins.tdxgo.provider:TdxGoProvider": TdxGoProvider,
        "app.plugins.tdxgo.bridge:availability": availability,
    }
    monkeypatch.setattr(cs_loader, "_load_entry", lambda ref: entry_map[ref])
    monkeypatch.setattr(cs_loader, "_call_check", lambda ref: (True, "ok"))
    cs_loader._load_builtin_plugins()
    provider = cs_loader.get_provider("tdxgo")
    assert provider is not None
    assert provider.builtin is True
    assert {"daily", "adj_factor", "minute", "realtime"} <= set(provider.config.datasets)


# ---- 可用性检测 ----


def test_availability_ok(monkeypatch, tmp_path):
    # 模拟二进制已存在, 并 mock run_job, 验证 ping 探活路径。
    dummy = tmp_path / "tdxgo"
    dummy.write_bytes(b"")
    monkeypatch.setattr(tdxgo_bridge, "_binary", lambda: dummy)
    monkeypatch.setattr(tdxgo_bridge, "run_job", lambda job: {"status": "ok"})
    ok, reason = availability()
    assert ok is True
    assert reason == "ok"


# ---- symbol 转换 ----


def test_symbol_conversion():
    assert _to_tdx("000001.SZ") == "sz000001"
    assert _to_tdx("600000.SH") == "sh600000"
    assert _to_tdx("510300.SH") == "sh510300"
    assert _to_symbol("sz000001") == "000001.SZ"
    assert _to_symbol("sh600000") == "600000.SH"
    assert _to_symbol("sh510300") == "510300.SH"


# ---- daily ----


def test_get_daily(monkeypatch):
    bridge_rows = [
        {"symbol": "sz000001", "date": "2026-01-02", "open": 9.9, "high": 10.1, "low": 9.8, "close": 10.0, "volume": 1200, "amount": 1.2e6},
        {"symbol": "sz000001", "date": "2026-01-03", "open": 10.0, "high": 10.6, "low": 9.9, "close": 10.5, "volume": 1300, "amount": 1.4e6},
    ]
    monkeypatch.setattr(tdxgo_provider, "run_job", lambda job: bridge_rows)
    provider = TdxGoProvider()
    df = provider.get_daily(["000001.SZ"], datetime(2026, 1, 2), datetime(2026, 1, 3, 23, 59))
    assert df.columns == ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
    assert df["symbol"].to_list() == ["000001.SZ", "000001.SZ"]
    assert df["date"].to_list() == [datetime(2026, 1, 2).date(), datetime(2026, 1, 3).date()]
    assert df["close"].to_list() == [10.0, 10.5]
    assert df["volume"].to_list() == [1200.0, 1300.0]


def test_get_daily_empty(monkeypatch):
    monkeypatch.setattr(tdxgo_provider, "run_job", lambda job: [])
    provider = TdxGoProvider()
    assert provider.get_daily([], None, None).is_empty()


# ---- adj_factor ----


def test_get_adj_factors(monkeypatch):
    """桥接层返回的 ex_factor 已是除权事件因子(最大为 1), 直接归一化。"""
    bridge_rows = [
        {"symbol": "sz000001", "trade_date": "2026-02-03", "ex_factor": 2.0},
        {"symbol": "sz000001", "trade_date": "2026-04-05", "ex_factor": 0.75},
    ]
    monkeypatch.setattr(tdxgo_provider, "run_job", lambda job: bridge_rows)
    provider = TdxGoProvider()
    df = provider.get_adj_factors(["000001.SZ"], None, None)
    assert df.columns == ["symbol", "trade_date", "ex_factor"]
    assert df["symbol"].to_list() == ["000001.SZ", "000001.SZ"]
    assert df["trade_date"].to_list() == [datetime(2026, 2, 3).date(), datetime(2026, 4, 5).date()]
    assert df["ex_factor"].to_list() == [2.0, 0.75]


# ---- minute ----


def test_get_minute(monkeypatch):
    """桥接层分钟 datetime 已是真实 UTC naive(-8h), provider 保持原样并仅过滤窗口。"""
    bridge_rows = [
        {"symbol": "sz000001", "datetime": "2026-01-02T01:31:00Z", "open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0, "volume": 100, "amount": 1.0e5},
        {"symbol": "sz000001", "datetime": "2026-01-02T01:32:00Z", "open": 10.0, "high": 10.2, "low": 10.0, "close": 10.1, "volume": 110, "amount": 1.1e5},
    ]
    monkeypatch.setattr(tdxgo_provider, "run_job", lambda job: bridge_rows)
    provider = TdxGoProvider()
    df = provider.get_minute(["000001.SZ"], None, None, freq="1m")
    assert df.columns == ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    assert df["symbol"].to_list() == ["000001.SZ", "000001.SZ"]
    assert df["datetime"].dtype == pl.Datetime("us")  # naive
    assert df["datetime"].to_list() == [datetime(2026, 1, 2, 1, 31), datetime(2026, 1, 2, 1, 32)]


def test_get_minute_filter_window(monkeypatch):
    """调用方传北京墙钟, 过滤边界需 -8h 后与 UTC naive 比较。"""
    bridge_rows = [
        {"symbol": "sz000001", "datetime": "2026-01-02T01:31:00Z", "open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0, "volume": 100, "amount": 1.0e5},
        {"symbol": "sz000001", "datetime": "2026-01-02T02:00:00Z", "open": 10.2, "high": 10.3, "low": 10.1, "close": 10.2, "volume": 120, "amount": 1.2e5},
    ]
    monkeypatch.setattr(tdxgo_provider, "run_job", lambda job: bridge_rows)
    provider = TdxGoProvider()
    df = provider.get_minute(
        ["000001.SZ"],
        datetime(2026, 1, 2, 9, 31),   # 北京墙钟 09:31 = UTC 01:31
        datetime(2026, 1, 2, 9, 59),   # 北京墙钟 09:59 = UTC 01:59
        freq="1m",
    )
    assert df["datetime"].to_list() == [datetime(2026, 1, 2, 1, 31)]  # 窗口 [01:31, 01:59]: 02:00 被滤除


# ---- realtime ----


def test_get_realtime(monkeypatch):
    """桥接层 change_pct 已是小数制(0.20 / -0.005)。"""
    bridge_rows = [
        {"symbol": "sz000001", "last_price": 12.0, "prev_close": 10.0, "open": 10.2, "high": 12.3, "low": 10.1, "volume": 123456, "amount": 1.5e8, "change_pct": 0.20},
        {"symbol": "sh600000", "last_price": 8.0, "prev_close": 8.0, "open": 8.0, "high": 8.2, "low": 7.9, "volume": 5000, "amount": 4e6, "change_pct": -0.005},
    ]
    monkeypatch.setattr(tdxgo_provider, "run_job", lambda job: bridge_rows)
    provider = TdxGoProvider()
    rows = provider.get_realtime(symbols=["000001.SZ", "600000.SH"])
    assert len(rows) == 2
    assert rows[0]["symbol"] == "000001.SZ"
    assert rows[0]["last_price"] == 12.0
    assert rows[0]["change_pct"] == 0.20
    assert rows[1]["symbol"] == "600000.SH"
    assert rows[1]["change_pct"] == -0.005


# ---- instruments ----


def test_get_instruments(monkeypatch):
    bridge_rows = [
        {"symbol": "sz000001", "name": "平安银行", "code": "000001", "exchange": "sz"},
        {"symbol": "sh600000", "name": "浦发银行", "code": "600000", "exchange": "sh"},
        {"symbol": "sh510300", "name": "沪深300ETF", "code": "510300", "exchange": "sh"},
    ]
    monkeypatch.setattr(tdxgo_provider, "run_job", lambda job: bridge_rows)
    provider = TdxGoProvider()
    rows = provider.get_instruments("stock")
    assert {r["symbol"] for r in rows} == {"000001.SZ", "600000.SH", "510300.SH"}
    by_symbol = {r["symbol"]: r for r in rows}
    assert by_symbol["000001.SZ"]["name"] == "平安银行"
    assert by_symbol["000001.SZ"]["code"] == "000001"
    assert by_symbol["000001.SZ"]["exchange"] == "SZ"
    assert by_symbol["600000.SH"]["exchange"] == "SH"
