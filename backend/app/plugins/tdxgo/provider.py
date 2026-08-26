"""tdxgo 内置数据源 provider: 通过 Go 桥接二进制接入 injoyai/tdx。

方法签名对齐 base.MarketDataProvider 契约(service 分流点按这套签名调用),
因此注入 custom loader 注册表后各 service 无需改动即可路由到本 provider。

数据集: daily / adj_factor / minute / realtime + instruments(标的维表)。
financial 未声明: financial_sync 直连 TickFlow SDK, 不走 provider 抽象。

口径注意(与 eltdx/TickFlow 数据核对):
- Symbol: 000001.SZ <-> sz000001。
- realtime 的 change_pct 桥接层已转小数制(0.0366, 天然是 (现价-昨收)/昨收)。
- volume 单位为「手」(沿用 tdx 库归一化, 与 eltdx volume_lots 同口径)。
- ex_factor 为每次除权事件的 pre/post 比值(个股级, 非累积), 桥接层已按
  ex(D)=qfq(D)/qfq(D-1) 转换, 与 pipeline 期望一致。
"""

from __future__ import annotations

import concurrent.futures as _futures
import logging
import threading as _threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable

import polars as pl

from app.data_providers.normalizer import normalize_adj_factors, normalize_daily
from app.plugins.tdxgo.bridge import TdxGoBridgeError, run_job

logger = logging.getLogger(__name__)

# 支持的数据集
_DATASETS = ("daily", "adj_factor", "minute", "realtime")

_EXCHANGE_TO_TDX = {"SH": "sh", "SZ": "sz", "BJ": "bj"}
_TDX_TO_EXCHANGE = {v: k for k, v in _EXCHANGE_TO_TDX.items()}
_PERIOD_MAP = {"1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m", "60m": "60m"}

# 每批符号数: 一个子进程调用内顺序拉取 N 只, 摊薄 Go 进程/建连开销。
_BATCH = 20
# 并发 worker 数: 静态分片后每 worker 一个子进程并复用连接。
_WORKERS = 8
# 分钟K单日根数上限(1 分钟 240 根/交易日)。桥接层一次拉全量; 单位用做进度粒度。
_MINUTE_PAGE = 800
_MINUTE_CANONICAL = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]


@dataclass
class _TdxGoConfig:
    """轻量 config shim, 让 custom loader 的 list_sources/provider_has_dataset 能识别本 provider。"""

    name: str = "tdxgo"
    display_name: str = "通达信行情(tdxgo)"
    datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS))
    path: None = None
    builtin: bool = True


def _to_tdx(symbol: str) -> str:
    """000001.SZ -> sz000001。"""
    code, _, ex = symbol.partition(".")
    return f"{_EXCHANGE_TO_TDX.get(ex.upper(), 'sz')}{code}"


def _to_symbol(full_code: str) -> str:
    """sz000001 -> 000001.SZ。"""
    ex, code = full_code[:2].lower(), full_code[2:]
    return f"{code}.{_TDX_TO_EXCHANGE.get(ex, 'SZ')}"


def _to_utc_naive(t: datetime | str) -> datetime:
    """北京墙钟(桥接返回的 UTC naive datetime 字符串) 保持原样的 naive datetime。

    桥接层已把分钟时间转成真实 UTC naive(RFC3339), 调用方 start_time/end_time
    是北京墙钟 naive, 过滤边界须同步 -8h 才能正确比较(= 真实 UTC naive)。
    """
    if isinstance(t, str):
        return datetime.fromisoformat(t)
    return t.replace(tzinfo=None) if t.tzinfo is not None else t


def _filter_daily(df: pl.DataFrame, start_time: datetime | None, end_time: datetime | None) -> pl.DataFrame:
    if df.is_empty():
        return df
    if start_time:
        df = df.filter(pl.col("date") >= start_time.date())
    if end_time:
        df = df.filter(pl.col("date") <= end_time.date())
    return df


def _filter_minute(df: pl.DataFrame, start_time: datetime | None, end_time: datetime | None) -> pl.DataFrame:
    if df.is_empty():
        return df
    # 桥接分钟时间已是真实 UTC naive; 调用方传北京墙钟, 需 -8h 后比较。
    if start_time:
        df = df.filter(pl.col("datetime") >= (start_time - timedelta(hours=8)))
    if end_time:
        df = df.filter(pl.col("datetime") <= (end_time - timedelta(hours=8)))
    return df


class TdxGoProvider:
    """内置通达信行情(tdxgo)数据源。"""

    name = "tdxgo"
    builtin = True

    def __init__(self) -> None:
        self.config = _TdxGoConfig()

    def close(self) -> None:
        pass

    def _call(self, job: dict) -> object:
        """调桥接层, 失败记日志并返回空, 不向上抛(单批失败不阻断整任务)。"""
        try:
            return run_job(job)
        except TdxGoBridgeError as e:
            logger.warning("tdxgo bridge 调用失败(%s): %s", job.get("op"), e)
            return []

    def _run_concurrent(
        self,
        symbols: list[str],
        op: str,
        build_job: Callable[[list[str]], dict],
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> list[dict]:
        """静态分片并发: 每 worker 一个子进程, 每个子进程顺序拉 _BATCH 只。

        返回扁平后的行列表(dict); on_chunk_done(cur, tot) 按批粒度回调进度。
        """
        if not symbols:
            return []
        total = len(symbols)
        size = max(1, (total + _WORKERS - 1) // _WORKERS)
        chunks = [symbols[i : i + size] for i in range(0, total, size)]
        tot_batches = (total + _BATCH - 1) // _BATCH
        lock = _threading.Lock()
        done = 0
        reported = 0

        def tick() -> None:
            nonlocal done, reported
            if not on_chunk_done:
                return
            with lock:
                done += 1
                target = tot_batches if done >= total else done // _BATCH
                while reported < target:
                    reported += 1
                    on_chunk_done(reported, tot_batches)

        def worker(chunk: list[str]) -> list[dict]:
            out: list[dict] = []
            # 一个大子进程内再按 _BATCH 切, 避免单次子进程符号过多、响应过大。
            for i in range(0, len(chunk), _BATCH):
                piece = chunk[i : i + _BATCH]
                data = self._call(build_job(piece))
                if isinstance(data, list):
                    out.extend(data)
            return out

        results: list[list[dict]] = []
        with _futures.ThreadPoolExecutor(max_workers=len(chunks)) as pool:
            futures = [pool.submit(worker, ch) for ch in chunks]
            for fut in _futures.as_completed(futures):
                results.append(fut.result())
        merged: list[dict] = []
        for batch in results:
            merged.extend(batch)
        return merged

    # ---- daily ----
    def get_daily(self, symbols, start_time=None, end_time=None, asset_type="stock", on_chunk_done=None) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        tdx_symbols = [_to_tdx(s) for s in symbols]

        def build_job(piece: list[str]) -> dict:
            return {"op": "daily", "args": {"symbols": piece}}

        rows = self._run_concurrent(tdx_symbols, "daily", build_job, on_chunk_done)
        # 桥接返回的 symbol 是 sz000001, 归一回项目格式。
        for r in rows:
            if r.get("symbol"):
                r["symbol"] = _to_symbol(r["symbol"])
        df = normalize_daily(rows, source=self.name)
        return _filter_daily(df, start_time, end_time)

    # ---- adj_factor ----
    def get_adj_factors(self, symbols, start_time=None, end_time=None, asset_type="stock", on_chunk_done=None) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        tdx_symbols = [_to_tdx(s) for s in symbols]

        def build_job(piece: list[str]) -> dict:
            return {"op": "adj_factor", "args": {"symbols": piece}}

        rows = self._run_concurrent(tdx_symbols, "adj_factor", build_job, on_chunk_done)
        for r in rows:
            if r.get("symbol"):
                r["symbol"] = _to_symbol(r["symbol"])
        df = normalize_adj_factors(rows, source=self.name)
        if df.is_empty():
            return df
        if start_time:
            df = df.filter(pl.col("trade_date") >= start_time.date())
        if end_time:
            df = df.filter(pl.col("trade_date") <= end_time.date())
        return df

    # ---- minute ----
    def get_minute(self, symbols, start_time=None, end_time=None, asset_type="stock", freq="1m", on_chunk_done=None) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        period = _PERIOD_MAP.get(freq, "1m")
        tdx_symbols = [_to_tdx(s) for s in symbols]

        def build_job(piece: list[str]) -> dict:
            return {"op": "minute", "args": {"symbols": piece, "freq": period}}

        rows = self._run_concurrent(tdx_symbols, "minute", build_job, on_chunk_done)
        for r in rows:
            if r.get("symbol"):
                r["symbol"] = _to_symbol(r["symbol"])
            # 桥接层返回 RFC3339 字符串(真实 UTC naive), 解析为 naive datetime 以便
            # polars 建 Datetime 列并参与窗口过滤比较。
            if isinstance(r.get("datetime"), str):
                r["datetime"] = datetime.fromisoformat(r["datetime"].replace("Z", "+00:00")).replace(tzinfo=None)
        df = pl.DataFrame(rows) if rows else pl.DataFrame()
        if df.is_empty():
            return df
        df = df.select([c for c in _MINUTE_CANONICAL if c in df.columns])
        return _filter_minute(df, start_time, end_time)

    # ---- realtime ----
    def get_realtime(self, universes=None, symbols=None) -> list[dict]:
        try:
            codes = symbols or self._all_codes()
            if not codes:
                return []
            tdx_codes = [_to_tdx(s) for s in codes]
            rows: list[dict] = []
            for i in range(0, len(tdx_codes), _BATCH):
                piece = tdx_codes[i : i + _BATCH]
                data = self._call({"op": "realtime", "args": {"codes": piece}})
                if isinstance(data, list):
                    rows.extend(data)
            for r in rows:
                if r.get("symbol"):
                    r["symbol"] = _to_symbol(r["symbol"])
            return rows
        except Exception as e:  # noqa: BLE001
            logger.warning("tdxgo realtime 拉取失败: %s", e)
            return []

    def _all_codes(self) -> list[str]:
        """全市场代码: 取股票/指数/ETF 三类标的并聚合(维度表优先, 失败则返回空)。"""
        out: list[str] = []
        for asset_type in ("stock", "index", "etf"):
            for it in self.get_instruments(asset_type):
                sym = it.get("symbol")
                if sym:
                    out.append(sym)
        return out

    # ---- instruments ----
    def get_instruments(self, asset_type="stock") -> list[dict]:
        data = self._call({"op": "instruments", "args": {"asset_type": asset_type}})
        rows: list[dict] = []
        for it in data or []:
            full = it.get("symbol") or ""
            if not full:
                continue
            rows.append(
                {
                    "symbol": _to_symbol(full),
                    "name": it.get("name") or _to_symbol(full),
                    "code": it.get("code") or full[2:],
                    "exchange": (it.get("exchange") or "").upper(),
                }
            )
        return rows

    # ---- 设置页试拉 ----
    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        symbols = symbols or ["600519.SH"]
        if dataset == "daily":
            df = self.get_daily(symbols, None, None)
            return self._preview("daily", df)
        if dataset == "adj_factor":
            df = self.get_adj_factors(symbols, None, None)
            return self._preview("adj_factor", df)
        if dataset == "minute":
            df = self.get_minute(symbols, None, None)
            return self._preview("minute", df)
        if dataset == "realtime":
            rows = self.get_realtime(symbols=symbols)
            head = rows[:5]
            return {
                "provider": self.name,
                "dataset": "realtime",
                "rows": len(rows),
                "columns": list(head[0].keys()) if head else [],
                "preview": head,
            }
        raise ValueError(f"tdxgo 不支持数据集: {dataset}")

    @staticmethod
    def _preview(dataset: str, df: pl.DataFrame) -> dict:
        return {
            "provider": "tdxgo",
            "dataset": dataset,
            "rows": df.height,
            "columns": df.columns,
            "preview": df.head(5).to_dicts() if not df.is_empty() else [],
        }
