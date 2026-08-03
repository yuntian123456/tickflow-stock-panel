"""eltdx 内置数据源 provider 契约测试。

不依赖真实网络/通达信服务器: 通过注入假的 eltdx 模块模拟 TdxClient 响应,
覆盖插件声明/注册、symbol 转换、各数据集归一化、周期映射与单位换算。
"""

from __future__ import annotations

import sys
import types
from datetime import datetime, timezone
from types import SimpleNamespace

import polars as pl
import pytest

from app.data_providers.custom import loader as cs_loader
from app.plugins.eltdx import bridge as eltdx_bridge
from app.plugins.eltdx.provider import EltdxProvider, _to_symbol, _to_tdx


def _bar(time, close, high=None, low=None, volume=100, amount=10000.0):
    return SimpleNamespace(
        time=time,
        open=close - 0.1,
        high=high or close + 0.1,
        low=low or close - 0.2,
        close=close,
        volume_lots=volume,
        amount=amount,
    )


class FakeTdxClient:
    """模拟 eltdx 客户端: 返回预设的 K线/因子/代码表/快照。"""

    def __init__(self, *, series=None, factors=None, codes=None, snapshots=None, period_log=None):
        self._series = series
        self._factors = factors
        self._codes = codes
        self._snapshots = snapshots
        self._period_log = period_log
        self.bars = SimpleNamespace(all=self._bars_all, get=self._bars_get)
        self.quotes = SimpleNamespace(get_snapshots=self._quotes_get_snapshots)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def _bars_all(self, code, **kwargs):
        return self._series

    def _bars_get(self, code, period="day", **kwargs):
        if self._period_log is not None:
            self._period_log.append(period)
        return self._series

    def get_factors(self, code):
        return self._factors

    def get_a_share_codes_all(self):
        return self._codes

    def _quotes_get_snapshots(self, codes):
        return self._snapshots


@pytest.fixture
def fake_eltdx(monkeypatch):
    """注入假的 eltdx 模块: TdxClient() 返回 FakeTdxClient 实例。"""

    def _inject(fake: FakeTdxClient) -> None:
        mod = types.ModuleType("eltdx")
        mod.TdxClient = lambda *a, **k: fake
        monkeypatch.setitem(sys.modules, "eltdx", mod)

    return _inject


# ---- 插件发现与注册 ----


def test_plugin_discovered_in_loader():
    cs_loader._load_builtin_plugins()
    plugins = {p["name"]: p for p in cs_loader.list_plugins()}
    manifest = plugins.get("eltdx")
    assert manifest is not None
    assert manifest["runtime"] == "python"
    assert manifest["datasets"] == ["daily", "adj_factor", "minute", "realtime"]
    assert "financial" not in manifest["datasets"]


def test_plugin_registered_when_available(monkeypatch):
    entry_map = {
        "app.plugins.eltdx.provider:EltdxProvider": EltdxProvider,
        "app.plugins.eltdx.bridge:availability": eltdx_bridge.availability,
    }
    monkeypatch.setattr(cs_loader, "_load_entry", lambda ref: entry_map[ref])
    monkeypatch.setattr(cs_loader, "_call_check", lambda ref: (True, "ok"))
    cs_loader._load_builtin_plugins()
    provider = cs_loader.get_provider("eltdx")
    assert provider is not None
    assert provider.builtin is True
    assert {"daily", "adj_factor", "minute", "realtime"} <= set(provider.config.datasets)


# ---- 可用性检测 ----


def test_availability_import_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "eltdx", None)
    ok, reason = eltdx_bridge.availability()
    assert ok is False
    assert "未安装" in reason


def test_availability_ok(fake_eltdx):
    fake_eltdx(FakeTdxClient())
    ok, reason = eltdx_bridge.availability()
    assert ok is True
    assert reason == "ok"


# ---- symbol 转换 ----


def test_symbol_conversion():
    assert _to_tdx("000001.SZ") == "sz000001"
    assert _to_tdx("600000.SH") == "sh600000"
    assert _to_symbol("sz000001") == "000001.SZ"
    assert _to_symbol("sh600000") == "600000.SH"


# ---- daily ----


def test_get_daily(fake_eltdx):
    series = SimpleNamespace(
        bars=[
            _bar(datetime(2026, 1, 2, 15, 0), close=10.0, volume=1200, amount=1.2e6),
            _bar(datetime(2026, 1, 3, 15, 0), close=10.5, volume=1300, amount=1.4e6),
        ]
    )
    fake_eltdx(FakeTdxClient(series=series))
    provider = EltdxProvider()
    df = provider.get_daily(["000001.SZ"], datetime(2026, 1, 1), datetime(2026, 1, 2, 23, 59))
    assert df.columns == ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
    assert df["symbol"].to_list() == ["000001.SZ"]
    assert df["date"].to_list() == [datetime(2026, 1, 2).date()]
    assert df["close"].to_list() == [10.0]
    assert df["volume"].to_list() == [1200.0]


def test_get_daily_empty():
    provider = EltdxProvider()
    assert provider.get_daily([], None, None).is_empty()


# ---- adj_factor ----


def test_get_adj_factors(fake_eltdx):
    factors = SimpleNamespace(
        items=[
            SimpleNamespace(time=datetime(2026, 1, 2, 15, 0), qfq_factor=1.2345),
            SimpleNamespace(time=datetime(2026, 2, 3, 15, 0), qfq_factor=1.3456),
        ]
    )
    fake_eltdx(FakeTdxClient(factors=factors))
    provider = EltdxProvider()
    df = provider.get_adj_factors(["000001.SZ"], None, None)
    assert df.columns == ["symbol", "trade_date", "ex_factor"]
    assert df["symbol"].to_list() == ["000001.SZ", "000001.SZ"]
    assert df["trade_date"].to_list() == [datetime(2026, 1, 2).date(), datetime(2026, 2, 3).date()]
    assert df["ex_factor"].to_list() == [1.2345, 1.3456]


# ---- minute ----


@pytest.mark.parametrize(
    "freq,expected",
    [("1m", "1m"), ("5m", "5m"), ("60m", "60m"), ("unknown_freq", "1m")],
)
def test_get_minute_period_mapping(fake_eltdx, freq, expected):
    series = SimpleNamespace(bars=[_bar(datetime(2026, 1, 2, 9, 31), close=10.0)])
    period_log: list[str] = []
    fake_eltdx(FakeTdxClient(series=series, period_log=period_log))
    provider = EltdxProvider()
    df = provider.get_minute(["000001.SZ"], None, None, freq=freq)
    assert period_log == [expected]
    assert df.columns == ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    assert df["datetime"].to_list() == [datetime(2026, 1, 2, 9, 31)]


def test_get_minute_aware_datetime_normalized(fake_eltdx):
    """eltdx 返回 UTC 带时区时间戳: 去时区为 naive 墙钟, 与 naive start/end_time 过滤不冲突。"""
    series = SimpleNamespace(
        bars=[
            _bar(datetime(2026, 1, 2, 9, 31, tzinfo=timezone.utc), close=10.0),
            _bar(datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc), close=10.5),
        ]
    )
    fake_eltdx(FakeTdxClient(series=series))
    provider = EltdxProvider()
    df = provider.get_minute(
        ["000001.SZ"],
        datetime(2026, 1, 2, 9, 31),
        datetime(2026, 1, 2, 10, 0),
        freq="1m",
    )
    assert df["datetime"].dtype == pl.Datetime("us")  # naive, 无时区
    assert df["datetime"].to_list() == [datetime(2026, 1, 2, 9, 31)]


# ---- realtime ----


def test_get_realtime(fake_eltdx):
    codes = [
        SimpleNamespace(full_code="sz000001"),
        SimpleNamespace(full_code="sh600000"),
    ]
    snapshots = [
        SimpleNamespace(
            full_code="sz000001",
            last_price=12.0,
            pre_close_price=10.0,
            open_price=10.2,
            high_price=12.3,
            low_price=10.1,
            total_hand=123456,
            amount=1.5e8,
            change_pct=20.0,
        ),
        SimpleNamespace(
            full_code="sh600000",
            last_price=8.0,
            pre_close_price=8.0,
            open_price=8.0,
            high_price=8.2,
            low_price=7.9,
            total_hand=5000,
            amount=4e6,
            change_pct=-0.5,
        ),
    ]
    fake_eltdx(FakeTdxClient(codes=codes, snapshots=snapshots))
    provider = EltdxProvider()
    rows = provider.get_realtime()
    assert len(rows) == 2
    assert rows[0]["symbol"] == "000001.SZ"
    assert rows[0]["last_price"] == 12.0
    assert rows[0]["change_pct"] == 0.20  # 百分数(20%) → 小数制(0.20)
    assert rows[1]["symbol"] == "600000.SH"
    assert rows[1]["change_pct"] == -0.005


# ---- instruments ----


def test_get_instruments(fake_eltdx):
    codes = [
        SimpleNamespace(full_code="sz000001", name="平安银行", code="000001", exchange="sz"),
        SimpleNamespace(full_code="sh600000", name="浦发银行", code="600000", exchange="sh"),
    ]
    fake_eltdx(FakeTdxClient(codes=codes))
    provider = EltdxProvider()
    rows = provider.get_instruments("stock")
    assert rows[0]["symbol"] == "000001.SZ"
    assert rows[0]["name"] == "平安银行"
    assert rows[0]["code"] == "000001"
    assert rows[0]["exchange"] == "SZ"
    assert rows[0]["region"] == "CN"
    assert rows[0]["type"] == "stock"
    assert rows[1]["exchange"] == "SH"
