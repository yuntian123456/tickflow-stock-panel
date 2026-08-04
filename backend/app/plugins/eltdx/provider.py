"""通达信行情(eltdx)内置数据源 provider。

基于 <https://github.com/electkismet/eltdx> (纯 Python 通达信在线行情客户端)。

方法签名对齐 custom.GenericHTTPProvider(service 分流点按这套签名调用),
因此注入 custom loader 注册表后, 各 service 无需改动即可路由到本 provider。

数据集: daily / adj_factor / minute / realtime + instruments(标的维表)。
financial 未声明: financial_sync 直连 TickFlow SDK, 不走 provider 抽象。

口径注意(与 TickFlow 数据核对):
- realtime 的 change_pct 上游为百分数(如 3.66), 本 provider 转小数制(0.0366)。
- volume 单位为「手」(eltdx volume_lots / total_hand), 与 TickFlow 口径可能不同。
- ex_factor 取 eltdx 前复权因子 qfq_factor, 定义与 TickFlow 可能不同, 需抽样比对。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

import polars as pl

from app.data_providers.normalizer import normalize_adj_factors, normalize_daily
from app.tickflow.rate_limits import chunked

logger = logging.getLogger(__name__)

# eltdx 支持的数据集(financial 不支持 → 不声明, 自动回退 tickflow)
_DATASETS = ("daily", "adj_factor", "minute", "realtime")

_EXCHANGE_TO_TDX = {"SH": "sh", "SZ": "sz", "BJ": "bj"}
_TDX_TO_EXCHANGE = {v: k for k, v in _EXCHANGE_TO_TDX.items()}
# 分钟K周期: 项目 freq → eltdx period
_PERIOD_MAP = {"1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m", "60m": "60m"}
# 每批符号数(仅用于进度反馈; TDX 快照单批上限)
_BATCH = 40
_SNAPSHOT_BATCH = 80
_MINUTE_CANONICAL = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]


@dataclass
class _EltdxConfig:
    """轻量 config shim, 让 custom loader 的 list_sources/provider_has_dataset 能识别本 provider。"""

    name: str = "eltdx"
    display_name: str = "通达信行情(eltdx)"
    datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS))
    path: None = None
    builtin: bool = True


def _to_tdx(symbol: str) -> str:
    """000001.SZ → sz000001。非股票(指数/ETF)按市场前缀处理。"""
    code, _, ex = symbol.partition(".")
    return f"{_EXCHANGE_TO_TDX.get(ex.upper(), 'sz')}{code}"


def _to_symbol(full_code: str) -> str:
    """sz000001 → 000001.SZ。"""
    ex, code = full_code[:2].lower(), full_code[2:]
    return f"{code}.{_TDX_TO_EXCHANGE.get(ex, 'SZ')}"


def _naive(t) -> datetime:
    """去掉时区保留墙钟时间: eltdx 返回 UTC 带时区 datetime, 而项目内
    start_time/end_time 均为无时区(naive)北京时间, 两者直接比较会因
    dtype 不一致报错。通达信协议的时间本就是交易所本地墙钟, 去 tz 后口径对齐。"""
    return t.replace(tzinfo=None) if t.tzinfo is not None else t


def _bar_to_daily_row(bar, symbol: str) -> dict:
    """eltdx KlineBar → 内部日K行(不含 datetime, 避免与 date 冲突)。"""
    return {
        "symbol": symbol,
        "date": _naive(bar.time).date(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume_lots,  # 单位: 手
        "amount": bar.amount,
    }


def _bar_to_minute_row(bar, symbol: str) -> dict:
    """eltdx KlineBar → 内部分钟K行。"""
    return {
        "symbol": symbol,
        "datetime": _naive(bar.time),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume_lots,  # 单位: 手
        "amount": bar.amount,
    }


class EltdxProvider:
    """内置通达信行情(eltdx)数据源。"""

    name = "eltdx"
    builtin = True

    def __init__(self) -> None:
        self.config = _EltdxConfig()

    def close(self) -> None:  # loader.load_all 会对每个 provider 调 close
        pass

    def _client(self):
        """每次调用独立建连: 避免多线程共享 socket。eltdx 进入 with 即连, 退出自动断开。"""
        from eltdx import TdxClient

        return TdxClient(timeout=10)

    # ---- daily ----
    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        on_chunk_done=None,
    ) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        frames: list[pl.DataFrame] = []
        chunks = chunked(symbols, _BATCH)
        for i, chunk in enumerate(chunks):
            for sym in chunk:
                try:
                    with self._client() as c:
                        series = c.bars.all(_to_tdx(sym), period="day", adjust="none")
                    rows = [_bar_to_daily_row(b, sym) for b in (series.bars or [])]
                    df = normalize_daily(rows, source=self.name)
                    if df.is_empty():
                        continue
                    if start_time:
                        df = df.filter(pl.col("date") >= start_time.date())
                    if end_time:
                        df = df.filter(pl.col("date") <= end_time.date())
                    if not df.is_empty():
                        frames.append(df)
                except Exception as e:
                    logger.warning("eltdx daily %s 拉取失败: %s", sym, e)
            if on_chunk_done:
                on_chunk_done(i + 1, len(chunks))
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    # ---- adj_factor ----
    def get_adj_factors(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        on_chunk_done=None,
    ) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        frames: list[pl.DataFrame] = []
        chunks = chunked(symbols, _BATCH)
        for i, chunk in enumerate(chunks):
            for sym in chunk:
                try:
                    with self._client() as c:
                        factors = c.get_factors(_to_tdx(sym))
                    rows = [
                        {
                            "symbol": sym,
                            "trade_date": item.time.date(),
                            "ex_factor": item.qfq_factor,
                        }
                        for item in (factors.items or [])
                    ]
                    df = normalize_adj_factors(rows, source=self.name)
                    if df.is_empty():
                        continue
                    if start_time:
                        df = df.filter(pl.col("trade_date") >= start_time.date())
                    if end_time:
                        df = df.filter(pl.col("trade_date") <= end_time.date())
                    if not df.is_empty():
                        frames.append(df)
                except Exception as e:
                    logger.warning("eltdx adj %s 拉取失败: %s", sym, e)
            if on_chunk_done:
                on_chunk_done(i + 1, len(chunks))
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    # ---- minute ----
    def get_minute(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        freq: str = "1m",
        on_chunk_done=None,
    ) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        period = _PERIOD_MAP.get(freq, "1m")
        frames: list[pl.DataFrame] = []
        for sym in symbols:
            try:
                with self._client() as c:
                    series = c.bars.get(_to_tdx(sym), period=period, adjust="none", count=800)
                rows = [_bar_to_minute_row(b, sym) for b in (series.bars or [])]
                df = pl.DataFrame(rows) if rows else pl.DataFrame()
                if df.is_empty():
                    continue
                df = df.select(_MINUTE_CANONICAL)
                if start_time:
                    df = df.filter(pl.col("datetime") >= start_time)
                if end_time:
                    df = df.filter(pl.col("datetime") <= end_time)
                frames.append(df)
            except Exception as e:
                logger.warning("eltdx minute %s 拉取失败: %s", sym, e)
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    # ---- realtime ----
    def get_realtime(self) -> list[dict]:
        """全市场实时快照: 先取代码表, 再按 TDX 批量上限分批取快照拼成全市场。"""
        try:
            with self._client() as c:
                # get_a_share_codes_all() 返回 list[str] (full_code, 如 "sh600000"),
                # 直接作为快照批量查询入参, 不再取 .full_code 属性。
                codes = c.get_a_share_codes_all()
                out: list[dict] = []
                for batch in chunked(codes or [], _SNAPSHOT_BATCH):
                    quotes = c.quotes.get_snapshots(batch)
                    for q in quotes or []:
                        pct = getattr(q, "change_pct", None) or 0.0
                        out.append(
                            {
                                "symbol": _to_symbol(q.full_code),
                                "last_price": q.last_price,
                                "prev_close": q.pre_close_price,
                                "open": q.open_price,
                                "high": q.high_price,
                                "low": q.low_price,
                                "volume": q.total_hand,  # 单位: 手
                                "amount": getattr(q, "amount", None),
                                "change_pct": float(pct) / 100.0,  # 百分数 → 小数制
                            }
                        )
                return out
        except Exception as e:
            logger.warning("eltdx realtime 拉取失败: %s", e)
            return []

    # ---- instruments ----
    def get_instruments(self, asset_type: str = "stock") -> list[dict]:
        """标的维表, 供 instrument_sync 复用 flatten 路径(列结构与 tickflow 一致)。

        注意: eltdx 的 get_xxx_codes_all() 只返回 full_code 字符串, 不含
        name/code/exchange; 这里改用 get_codes_all(market) 取 SecurityCode
        对象并按 category 过滤 (a_share / etf / index)。
        """
        try:
            with self._client() as c:
                target = {"etf": "etf", "index": "index"}.get(asset_type, "a_share")
                items: list[dict] = []
                for market in ("sh", "sz", "bj"):
                    for it in c.get_codes_all(market):
                        if getattr(it, "category", "") != target:
                            continue
                        items.append(
                            {
                                "symbol": _to_symbol(it.full_code),
                                "name": it.name,
                                "code": it.code,
                                "exchange": (it.exchange or "").upper(),
                                "region": "CN",
                                "type": "stock",
                                "ext": {},
                            }
                        )
                return items
        except Exception as e:
            logger.warning("eltdx instruments 拉取失败: %s", e)
            return []
