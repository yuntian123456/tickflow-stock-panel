"""eltdx 内置数据源 provider 契约测试。

不依赖真实网络/通达信服务器: 通过注入假的 eltdx 模块模拟 TdxClient 响应,
覆盖插件声明/注册、symbol 转换、各数据集归一化、周期映射与单位换算。
"""

from __future__ import annotations

import sys
import types
from datetime import datetime, timedelta
from types import SimpleNamespace

import polars as pl
import pytest

from app.data_providers.custom import loader as cs_loader
from app.plugins.eltdx import bridge as eltdx_bridge
from app.plugins.eltdx.provider import _MINUTE_MAX_PAGES, EltdxProvider, _to_symbol, _to_tdx


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


def _gen_minute_bars(n_days: int, start_date, bars_per_day: int = 240) -> list:
    """生成 n_days 个交易日的 1m 分钟K(北京墙钟 naive), 每个交易日 bars_per_day 根。

    仅用于分页契约测试: 数据最旧在前、严格递增跨多个交易日, 与 provider 的
    「start 递增向更早翻页」及「bars[0] 为本页最早一根」约定对齐。
    """
    out: list = []
    day = datetime(start_date.year, start_date.month, start_date.day, 9, 30)
    made = 0
    while made < n_days:
        if day.weekday() < 5:
            for i in range(bars_per_day):
                out.append(_bar(day + timedelta(minutes=i), close=10.0 + made * 0.01 + i * 0.0001))
            made += 1
        day = (day + timedelta(days=1)).replace(hour=9, minute=30)
    return out


def _paged_server(bars: list):
    """把最旧在前的 bar 列表变成「按分页语义响应」的 bars.get 回调。

    模拟服务端: bars.get(code, start=s, count=c) 返回「从最旧端跳过 s 根后的 c 根」,
    即 start 递增 → 更早(更旧)的一页; 越过最旧端 → 空页。这正对应
    provider 的分页终止逻辑(空页 / 本页最早一根早于窗口起点 / 不足一页)。
    """
    n = len(bars)
    calls: list[int] = []

    def fn(code, **kwargs):
        s = kwargs.get("start", 0)
        c = kwargs.get("count", 800)
        calls.append(s)
        hi = n - s
        lo = max(0, hi - c)
        return SimpleNamespace(bars=list(bars[lo:hi]))

    fn.calls = calls
    return fn


class FakeTdxClient:
    """模拟 eltdx 客户端: 返回预设的 K线/因子/代码表/快照。

    对齐 eltdx v3.x 模块化 API:
    - codes.all_a_shares() / all_etfs() / all_indices() 返回 list[str] (full_code)
    - codes.all(market)  返回 list[SecurityCode] (含 name/code/exchange/category)
    - helpers.factors(code) 返回 FactorResponse (含 .items)
    - helpers.xdxr(code)     返回 list[XdxrRecord]
    """

    def __init__(
        self,
        *,
        series=None,
        factors=None,
        codes=None,
        snapshots=None,
        period_log=None,
        codes_all=None,
        kind_log=None,
        xdxr=None,
        bars_get_fn=None,
    ):
        self._series = series
        self._factors = factors
        self._codes = codes
        self._snapshots = snapshots
        self._period_log = period_log
        self._codes_all = codes_all or []
        self._kind_log = kind_log
        self._xdxr = xdxr or []
        self._bars_get_fn = bars_get_fn
        self.bars = SimpleNamespace(all=self._bars_all, get=self._bars_get)
        self.quotes = SimpleNamespace(get_snapshots=self._quotes_get_snapshots)
        self.codes = SimpleNamespace(
            all_a_shares=self.codes_all_a_shares,
            all_etfs=self.codes_all_etfs,
            all_indices=self.codes_all_indices,
            all=self.codes_all_market,
        )
        self.helpers = SimpleNamespace(
            factors=self._helpers_factors,
            xdxr=self._helpers_xdxr,
        )

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def _record_kind(self, kwargs):
        if self._kind_log is not None:
            self._kind_log.append(kwargs.get("kind"))

    def _bars_all(self, code, **kwargs):
        self._record_kind(kwargs)
        return self._series

    def _bars_get(self, code, period="day", **kwargs):
        self._record_kind(kwargs)
        if self._period_log is not None:
            self._period_log.append(period)
        if self._bars_get_fn is not None:
            return self._bars_get_fn(code, **kwargs)
        return self._series

    # ---- codes 模块(all_xxx 返回 full_code 字符串, 按前缀分类为互斥三类) ----
    def codes_all_a_shares(self):
        return [
            it
            for it in (self._codes or [])
            if not it.startswith(("sh000", "sz399", "sh880", "sh881", "bj899", "sh5", "sz15", "sz16", "sh56", "sh58"))
        ]

    def codes_all_etfs(self):
        return [it for it in (self._codes or []) if it.startswith(("sh5", "sz15", "sz16", "sh56", "sh58"))]

    def codes_all_indices(self):
        return [it for it in (self._codes or []) if it.startswith(("sh000", "sz399", "sh880", "sh881", "bj899"))]

    def codes_all_market(self, market):
        return [it for it in self._codes_all if getattr(it, "exchange", "") == market]

    # ---- helpers 模块 ----
    def _helpers_factors(self, code):
        return self._factors

    def _helpers_xdxr(self, code):
        return self._xdxr

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
    """eltdx qfq_factor 是每日累积前复权系数, 应转换为除权事件因子。

    事件因子 ex(D) = qfq(D)/qfq(D-1), 记在跳变日, 仅保留 |ex-1|>1e-9 的除权日;
    序列首日无前值跳过, 无跳变的日常数(1.0)不产出因子行。
    """
    factors = SimpleNamespace(
        items=[
            SimpleNamespace(time=datetime(2026, 1, 2, 15, 0), qfq_factor=1.0),  # 首日: 跳过
            SimpleNamespace(time=datetime(2026, 2, 3, 15, 0), qfq_factor=2.0),  # ex=2.0
            SimpleNamespace(time=datetime(2026, 3, 4, 15, 0), qfq_factor=2.0),  # 无跳变: 滤除
            SimpleNamespace(time=datetime(2026, 4, 5, 15, 0), qfq_factor=1.5),  # ex=0.75
        ]
    )
    fake_eltdx(FakeTdxClient(factors=factors))
    provider = EltdxProvider()
    df = provider.get_adj_factors(["000001.SZ"], None, None)
    assert df.columns == ["symbol", "trade_date", "ex_factor"]
    assert df["symbol"].to_list() == ["000001.SZ", "000001.SZ"]
    assert df["trade_date"].to_list() == [datetime(2026, 2, 3).date(), datetime(2026, 4, 5).date()]
    assert df["ex_factor"].to_list() == [2.0, 0.75]


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
    assert df["datetime"].to_list() == [datetime(2026, 1, 2, 9, 31)]  # 9:31 北京墙钟 naive(契约口径)


def test_get_minute_beijing_wallclock_naive(fake_eltdx):
    """eltdx 返回带 +08:00 时区的北京墙钟时间戳: 去 tz 后直接得到北京墙钟 naive。

    分钟K契约 (CONTRIBUTING §3.3): datetime 必须为北京时间墙钟, 前端不做时区换算。
    """
    from zoneinfo import ZoneInfo
    sh = ZoneInfo("Asia/Shanghai")
    series = SimpleNamespace(
        bars=[
            _bar(datetime(2026, 1, 2, 9, 31, tzinfo=sh), close=10.0),
            _bar(datetime(2026, 1, 2, 14, 30, tzinfo=sh), close=10.5),
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
    assert df["datetime"].to_list() == [datetime(2026, 1, 2, 9, 31)]  # 9:31 北京墙钟


# ---- realtime ----


def test_get_realtime(fake_eltdx):
    # 对齐真实 API: codes.all_a_shares() 返回 full_code 字符串列表
    codes = ["sz000001", "sh600000"]
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
    # 对齐真实 API: codes.all(market) 返回 SecurityCode 对象 (含 category)
    codes_all = [
        SimpleNamespace(
            full_code="sz000001", name="平安银行", code="000001", exchange="sz", category="a_share"
        ),
        SimpleNamespace(
            full_code="sh600000", name="浦发银行", code="600000", exchange="sh", category="a_share"
        ),
        SimpleNamespace(
            full_code="sh510300", name="沪深300ETF", code="510300", exchange="sh", category="etf"
        ),
    ]
    fake_eltdx(FakeTdxClient(codes_all=codes_all))
    provider = EltdxProvider()
    rows = provider.get_instruments("stock")
    assert len(rows) == 2  # etf 被过滤
    assert {r["symbol"] for r in rows} == {"000001.SZ", "600000.SH"}
    by_symbol = {r["symbol"]: r for r in rows}
    row = by_symbol["000001.SZ"]
    assert row["name"] == "平安银行"
    assert row["code"] == "000001"
    assert row["exchange"] == "SZ"
    assert row["region"] == "CN"
    assert row["type"] == "stock"
    assert by_symbol["600000.SH"]["exchange"] == "SH"
    rows_etf = provider.get_instruments("etf")
    assert [r["symbol"] for r in rows_etf] == ["510300.SH"]


# ---- 指数/ETF 修复回归: kind 透传与实时包含 ETF/指数 ----


def test_get_daily_index_kind(fake_eltdx):
    """指数日K 需给 bars 传 kind="index", 否则宽幅字段解析错位导致缺失。"""
    kind_log: list[str | None] = []
    series = SimpleNamespace(
        bars=[
            _bar(datetime(2026, 1, 2, 15, 0), close=3500.0, volume=1000, amount=1.0e7),
        ]
    )
    fake_eltdx(FakeTdxClient(series=series, kind_log=kind_log))
    provider = EltdxProvider()
    df = provider.get_daily(["000001.SH"], None, None)
    assert not df.is_empty()
    assert kind_log == ["index"]


def test_get_minute_index_kind(fake_eltdx):
    """指数分钟K 需给 bars 传 kind="index"。"""
    kind_log: list[str | None] = []
    series = SimpleNamespace(bars=[_bar(datetime(2026, 1, 2, 9, 31), close=3500.0)])
    fake_eltdx(FakeTdxClient(series=series, kind_log=kind_log))
    provider = EltdxProvider()
    df = provider.get_minute(["000001.SH"], None, None, freq="1m")
    assert df.columns == ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    assert kind_log == ["index"]


def test_get_daily_etf_kind_is_stock(fake_eltdx):
    """ETF 走股票协议, kind 应为 "stock"(而非 index)。"""
    kind_log: list[str | None] = []
    series = SimpleNamespace(bars=[_bar(datetime(2026, 1, 2, 15, 0), close=4.6, volume=1000, amount=1.0e6)])
    fake_eltdx(FakeTdxClient(series=series, kind_log=kind_log))
    provider = EltdxProvider()
    df = provider.get_daily(["510300.SH"], None, None)
    assert not df.is_empty()
    assert kind_log == ["stock"]


def test_get_realtime_includes_etf_index(fake_eltdx):
    """实时快照应包含 ETF 与指数代码(旧实现只拉 A股, 导致 etf/index 不更新)。"""
    codes = ["sz000001", "sh600000", "sh510300", "sh000001"]
    snapshots = [
        SimpleNamespace(full_code="sz000001", last_price=11.0, pre_close_price=10.0, open_price=10.2, high_price=11.3, low_price=10.1, total_hand=1000, amount=1.0e6, change_pct=10.0),
        SimpleNamespace(full_code="sh600000", last_price=8.0, pre_close_price=8.0, open_price=8.0, high_price=8.2, low_price=7.9, total_hand=500, amount=4.0e5, change_pct=-0.5),
        SimpleNamespace(full_code="sh510300", last_price=4.6, pre_close_price=4.5, open_price=4.55, high_price=4.62, low_price=4.53, total_hand=20000, amount=9.0e7, change_pct=2.2),
        SimpleNamespace(full_code="sh000001", last_price=3500.0, pre_close_price=3490.0, open_price=3495.0, high_price=3510.0, low_price=3480.0, total_hand=500000, amount=1.0e11, change_pct=0.29),
    ]
    fake_eltdx(FakeTdxClient(codes=codes, snapshots=snapshots))
    provider = EltdxProvider()
    rows = provider.get_realtime()
    symbols = {r["symbol"] for r in rows}
    assert {"510300.SH", "000001.SH"} <= symbols  # ETF 与指数都在实时快照里
    by_symbol = {r["symbol"]: r for r in rows}
    assert by_symbol["000001.SH"]["last_price"] == 3500.0
    assert by_symbol["510300.SH"]["last_price"] == 4.6


# ---- 分钟K分页契约: 多页合并、空页终止、页数上限 ----
#
# 契约(docs/plugin-development.md「测试要求」第 3 点): 分页需覆盖
# 「多页合并、空页终止、页数上限」。eltdx bars.get 单次 count 上限 800 根,
# 窗口跨多交易日时 provider 按 start 递增向更早翻页合并, 行为由 _paged_server 模拟。


def test_get_minute_pagination_merges_pages(fake_eltdx):
    """多页合并: 窗口覆盖超过单页(800根)时, 按 start 分页向更早翻页并合并全部数据。

    7 个交易日 = 1680 根 > 单页 800, 且窗口起点=最早一根(触发不了「已覆盖」提前中断),
    → 必须翻满 3 页(start=0/800/1600)才能取全窗口。
    """
    all_bars = _gen_minute_bars(n_days=7, start_date=datetime(2026, 1, 5))
    n = len(all_bars)
    assert n > 800  # 确保跨页
    server = _paged_server(all_bars)
    fake_eltdx(FakeTdxClient(bars_get_fn=server))
    provider = EltdxProvider()
    df = provider.get_minute(
        ["000001.SZ"],
        start_time=all_bars[0].time,
        end_time=all_bars[-1].time,
        freq="1m",
    )
    assert not df.is_empty()
    assert [s for s in server.calls] == [0, 800, 1600]  # 确实跨多页合并
    assert df.height == n  # 合并了全部 7 个交易日, 而非只最近 800 根
    assert set(df["datetime"].to_list()) == {b.time for b in all_bars}


def test_get_minute_pagination_empty_page_terminates(fake_eltdx):
    """空页终止: start_time 早于全量数据, 翻到最旧端服务端无更早数据 → 空页终止, 不死循环。

    用恰好 2 个满页(1600 根)的数据: 第 3 次请求(start=1600)返回空页触发终止。
    """
    all_bars = _gen_minute_bars(n_days=7, start_date=datetime(2026, 1, 5))[:1600]  # 恰好 2 满页
    server = _paged_server(all_bars)
    fake_eltdx(FakeTdxClient(bars_get_fn=server))
    provider = EltdxProvider()
    df = provider.get_minute(
        ["000001.SZ"],
        start_time=datetime(2026, 1, 1),  # 早于最早一根 → 不因「已覆盖窗口」中断
        end_time=datetime(2026, 1, 31),
        freq="1m",
    )
    assert df.height == 1600  # 两个满页全部合并
    assert server.calls[-1] == 1600  # 最后一次翻到越过最旧端 → 空页终止


def test_get_minute_pagination_cap(fake_eltdx):
    """页数上限: 服务端异常(永远返回满页、最早一根永不早于窗口起点、永不空页)时,
    由 _MINUTE_MAX_PAGES 页数上限终止, 避免 while 死循环。
    """
    page = _gen_minute_bars(n_days=4, start_date=datetime(2026, 1, 5))  # 960 根, 取前 count 根作满页
    calls: list[int] = []

    def bad_server(code, **kwargs):
        c = kwargs.get("count", 800)
        calls.append(kwargs.get("start", 0))
        # 始终返回满页 count 根; 最早一根(2026)不早于 start_time(2020) → 永不「已覆盖」,
        # 且不空页、不足一页 → 只能靠页数上限终止。
        return SimpleNamespace(bars=list(page[:c]))

    fake_eltdx(FakeTdxClient(bars_get_fn=bad_server))
    provider = EltdxProvider()
    df = provider.get_minute(
        ["000001.SZ"],
        start_time=datetime(2020, 1, 1),
        end_time=datetime(2030, 1, 1),
        freq="1m",
    )
    assert len(calls) == _MINUTE_MAX_PAGES  # 恰好翻满上限页, 未死循环
    assert not df.is_empty()
