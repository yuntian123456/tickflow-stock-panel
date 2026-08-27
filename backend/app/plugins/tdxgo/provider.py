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
from pathlib import Path
from typing import Callable

import polars as pl

from app.config import settings
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
# 实时快照专用并发: 全市场每轮轮询(秒级)用 2 个 worker 足够摊薄网络延迟,
# 8 个 worker = 每轮 8 个 tdxgo.exe 子进程建连, CPU 峰值高(与 eltdx 相比的差距来源)。
_REALTIME_WORKERS = 2
# 实时快照专用批大小: 每帧报价可容纳更多代码, 80 只/批减少子进程内批次数,
# 全市场 ~9600 只从 484 批降到 ~121 批, 显著降低轮询总耗时。
_REALTIME_BATCH = 80
# 全市场代码表缓存 TTL(秒): 代码表是静态维表, 每轮实时快照都重拉 3 次子进程属浪费。
_CODES_CACHE_TTL = 600.0
# 股本(财务)拉取专用并发/批大小: instruments_sync 盘前一次性全量拉, 用 8 worker×100 摊薄
# 5560 只的 GetFinanceInfo 调用(每个子进程内顺序拉 100 只, 降低子进程数量)。
_FINANCE_WORKERS = 8
_FINANCE_BATCH = 100
# 分钟K单日根数上限(1 分钟 240 根/交易日)。桥接层一次拉全量; 单位用做进度粒度。
_MINUTE_PAGE = 800
_MINUTE_CANONICAL = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
# 每个 period 单交易日分钟K根数 (A股 9:30-11:30 + 13:00-15:00 = 240 分钟)。
_BARS_PER_DAY = {"1m": 240, "5m": 48, "15m": 16, "30m": 8, "60m": 4}
# 桥接层(库)单次分钟K上限: 超此需分页, 且库本身最多 24000 根。
_KLINE_MAX = 24000
# 日K单标的根数上限(约 24 年 A股历史), 桥接层全年份拉全量会打爆桥接层内存。
_DAILY_MAX = 6000
# 日K单页(每次 TDX 请求)条数上限。
_DAILY_PAGE = 800


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


# 通达信板块指数(880xxx 概念/风格/地区, 881xxx 行业)归属上海交易所, 但并非可交易 A 股。
# 主项目实时拆分的 index 集合来自 TickFlow 的 instruments_index(不含板块指数),
# 若实时快照把这类代码返回, 会被当作 stock 写入 kline_daily, 污染涨幅/成交额榜。
# 因此实时快照(_all_codes)与指数标的里显式剔除。
def _is_block_index(symbol: str) -> bool:
    code, _, _ = symbol.partition(".")
    return code[:3] in ("880", "881")


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


def _filter_adj(df: pl.DataFrame, start_time: datetime | None, end_time: datetime | None) -> pl.DataFrame:
    if df.is_empty():
        return df
    if start_time:
        df = df.filter(pl.col("trade_date") >= start_time.date())
    if end_time:
        df = df.filter(pl.col("trade_date") <= end_time.date())
    return df


def _filter_minute(df: pl.DataFrame, start_time: datetime | None, end_time: datetime | None) -> pl.DataFrame:
    if df.is_empty():
        return df
    # 桥接分钟时间已是真实 UTC naive; 调用方传北京墙钟, 需 -8h 后比较。
    # 注意: 调用方(fetch_minute_single / sync_and_persist_minute)传的是带时区
    # (CN_TZ) 的 aware datetime, 直接与 naive 列比较会抛 SchemaError(导致:
    # 自定义源异常 → 回退 TickFlow → 分时为空)。须先去 tzinfo 取其北京墙钟再 -8h。
    def _naive(t: datetime | None) -> datetime | None:
        if t is None:
            return None
        return t.replace(tzinfo=None) if t.tzinfo is not None else t

    st = _naive(start_time)
    et = _naive(end_time)
    if st:
        df = df.filter(pl.col("datetime") >= (st - timedelta(hours=8)))
    if et:
        df = df.filter(pl.col("datetime") <= (et - timedelta(hours=8)))
    return df


def _minute_count(start_time: datetime | None, end_time: datetime | None, period: str) -> int:
    """按窗口估算该拉取的分钟K根数, 供桥接层限界拉取, 避免小窗口拉满库上限。

    - 给了 start/end: 按自然日×5/7 估交易日, 加 1 天裕度覆盖节假日/边界;
    - 未给(预览): 取最近 4 个交易日; 最终封顶 _KLINE_MAX。
    """
    bars_per_day = _BARS_PER_DAY.get(period, 240)
    if start_time and end_time:
        cal_days = max(1, (end_time - start_time).days)
        trading_days = max(1, int(cal_days * 5 / 7) + 1)
    else:
        trading_days = 4
    return max(bars_per_day, min(trading_days * bars_per_day + bars_per_day, _KLINE_MAX))


def _daily_count(start_time: datetime | None, end_time: datetime | None) -> int:
    """按窗口估算该拉的日K根数, 供桥接层限界拉取, 避免全市场拉全量历史打爆桥接层。"""
    if start_time and end_time:
        cal_days = max(1, (end_time - start_time).days)
        trading_days = max(1, int(cal_days * 5 / 7) + 1)
    else:
        trading_days = 250  # 默认最近约 1 年
    return max(1, min(trading_days + 5, _DAILY_MAX))


def _adj_count(start_time: datetime | None, end_time: datetime | None) -> int:
    """按窗口估算要回看的日K根数, 供桥接层求事件日的"前收盘价"。

    除权事件落在 [start, end] 内时, 其前收盘价通常会落在窗口起点稍前, 故多加裕度回看;
    无窗口(全量)则回看全历史, 确保能覆盖全部历史除权事件。
    """
    if start_time and end_time:
        cal_days = max(1, (end_time - start_time).days)
        trading_days = max(1, int(cal_days * 5 / 7) + 1)
    else:
        return _DAILY_MAX  # 全历史(约 24 年)
    return max(1, min(trading_days + 30, _DAILY_MAX))


class TdxGoProvider:
    """内置通达信行情(tdxgo)数据源。"""

    name = "tdxgo"
    builtin = True

    def __init__(self) -> None:
        self.config = _TdxGoConfig()
        # 全市场代码表缓存 (进程内, 轮询每轮不再重拉 3 次全市场代码表子进程)。
        self._codes_cache: list[str] | None = None
        self._codes_cache_at: float = 0.0

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
        workers: int | None = None,
        batch: int | None = None,
    ) -> list[dict]:
        """静态分片并发: 每 worker 一个子进程, 每个子进程顺序拉 batch 只。

        workers 可覆盖默认 _WORKERS: 实时快照(秒级轮询)传小值以降低每轮子进程数,
        daily/minute(低频批量)保持默认 8 worker。
        batch 可覆盖默认 _BATCH: 实时快照用更大批(80)减少子进程内批次数, 降低总耗时。
        返回扁平后的行列表(dict); on_chunk_done(cur, tot) 按批粒度回调进度。
        """
        if not symbols:
            return []
        total = len(symbols)
        n_workers = min(workers or _WORKERS, total)
        bsize = batch or _BATCH
        size = max(1, (total + n_workers - 1) // n_workers)
        chunks = [symbols[i : i + size] for i in range(0, total, size)]
        tot_batches = (total + bsize - 1) // bsize
        lock = _threading.Lock()
        done = 0
        reported = 0

        def tick(n: int = 1) -> None:
            nonlocal done, reported
            if not on_chunk_done:
                return
            with lock:
                done += n
                target = tot_batches if done >= total else done // bsize
                while reported < target:
                    reported += 1
                    on_chunk_done(reported, tot_batches)

        def worker(chunk: list[str]) -> list[dict]:
            out: list[dict] = []
            # 一个大子进程内再按 batch 切, 避免单次子进程符号过多、响应过大。
            for i in range(0, len(chunk), bsize):
                piece = chunk[i : i + bsize]
                data = self._call(build_job(piece))
                if isinstance(data, list):
                    out.extend(data)
                tick(len(piece))  # 每批上报进度, 否则进度永不更新(前端卡死)
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
        # 按窗口限界拉取: 桥接层不再逐年份拉全量历史(约6000根/只), 全市场会打爆桥接层。
        count = _daily_count(start_time, end_time)

        def build_job(piece: list[str]) -> dict:
            return {"op": "daily", "args": {"symbols": piece, "count": count}}

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
        # 按窗口限界回看日K(求前收盘), 避免每只 GetKlineDayAll 全量历史导致全市场除权拉取过慢。
        count = _adj_count(start_time, end_time)
        start_date = start_time.date().isoformat() if start_time else None
        end_date = end_time.date().isoformat() if end_time else None

        def build_job(piece: list[str]) -> dict:
            args: dict = {"symbols": piece, "count": count}
            if start_date:
                args["start_date"] = start_date
            if end_date:
                args["end_date"] = end_date
            return {"op": "adj_factor", "args": args}

        rows = self._run_concurrent(tdx_symbols, "adj_factor", build_job, on_chunk_done)
        for r in rows:
            if r.get("symbol"):
                r["symbol"] = _to_symbol(r["symbol"])
        df = normalize_adj_factors(rows, source=self.name)
        return _filter_adj(df, start_time, end_time)

    # ---- minute ----
    def get_minute(self, symbols, start_time=None, end_time=None, asset_type="stock", freq="1m", on_chunk_done=None) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        period = _PERIOD_MAP.get(freq, "1m")
        tdx_symbols = [_to_tdx(s) for s in symbols]
        # 按窗口限界拉取: 桥接层不再拉满库上限 24000 根(约 91 天), 小窗口只拉所需根数。
        count = _minute_count(start_time, end_time, period)

        def build_job(piece: list[str]) -> dict:
            return {"op": "minute", "args": {"symbols": piece, "freq": period, "count": count}}

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
        # 统一数值列类型: 桥接层 volume 为 int, 而分钟 parquet 落盘按 Float64 存储
        # (eltdx/tickflow 口径)。不转会在 _write_minute_partition 的 concat 处抛
        # "Type Int64 is incompatible with expected type Float64 (vstack 'volume')"。
        for col in ("open", "high", "low", "close", "volume", "amount"):
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))
        return _filter_minute(df, start_time, end_time)

    # ---- realtime ----
    def get_realtime(self, universes=None, symbols=None) -> list[dict]:
        try:
            codes = symbols or self._all_codes()
            if not codes:
                return []
            tdx_codes = [_to_tdx(s) for s in codes]

            def build_job(piece: list[str]) -> dict:
                return {"op": "realtime", "args": {"codes": piece}}

            # 并发分片(与 daily/minute 一致): 顺序逐批会触发数百次子进程拉取,
            # 全市场实时严重滞后。实时快照是秒级轮询, 用 _REALTIME_WORKERS 个
            # 子进程即可摊薄网络延迟, 避免每轮起满 8 个 tdxgo.exe 子进程(CPU 峰值高)。
            rows = self._run_concurrent(
                tdx_codes, "realtime", build_job,
                workers=_REALTIME_WORKERS, batch=_REALTIME_BATCH,
            )
            for r in rows:
                if r.get("symbol"):
                    r["symbol"] = _to_symbol(r["symbol"])
            return rows
        except Exception as e:  # noqa: BLE001
            logger.warning("tdxgo realtime 拉取失败: %s", e)
            return []

    def _all_codes(self) -> list[str]:
        """全市场代码: 取股票/指数/ETF 三类标的并聚合(进程内缓存, TTL 内不重拉)。

        代码表是静态维表, 实时快照每轮(秒级)都重拉 3 次全市场代码表子进程属浪费;
        缓存 10 分钟显著降低轮询开销。维度表失败则返回空。

        只返回主项目已同步的指数/ETF(instruments_index/_etf): 实时快照里不在受管集合
        的指数(通达信板块指数 880/881xxx、999xxx、399379/399380 等深证统计指数)会被
        quote_service 当作 stock 写进 kline_daily, 污染涨幅/成交额榜。
        """
        import time as _time
        now = _time.monotonic()
        if self._codes_cache is not None and (now - self._codes_cache_at) < _CODES_CACHE_TTL:
            return self._codes_cache
        idx_set = self._read_managed_symbols("instruments_index")
        etf_set = self._read_managed_symbols("instruments_etf")
        out: list[str] = []
        for asset_type in ("stock", "index", "etf"):
            for it in self._code_rows(asset_type):
                sym = it.get("symbol")
                if not sym:
                    continue
                # 板块指数硬过滤兜底(受管集合缺失时仍不混入)
                if _is_block_index(sym):
                    continue
                # 指数/ETF 仅在受管集合内返回; 集合为空(未同步)则不过滤, 回退旧行为
                if asset_type == "index" and idx_set and sym not in idx_set:
                    continue
                if asset_type == "etf" and etf_set and sym not in etf_set:
                    continue
                out.append(sym)
        self._codes_cache = out
        self._codes_cache_at = now
        return out

    @staticmethod
    def _read_managed_symbols(dirname: str) -> set[str]:
        """读取主项目已同步的指数/ETF 维表代码集。

        目录形态: data/instruments_index/instruments_index.parquet 等。
        文件缺失/为空/读取失败时返回空集(调用方据此跳过过滤, 回退旧行为)。
        """
        try:
            p = Path(settings.data_dir) / dirname / f"{dirname}.parquet"
            if not p.exists():
                return set()
            df = pl.read_parquet(p, columns=["symbol"])
            if df.is_empty():
                return set()
            return set(df["symbol"].cast(pl.Utf8).to_list())
        except Exception as e:  # noqa: BLE001
            logger.warning("tdxgo 读取 %s 受管集合失败: %s", dirname, e)
            return set()

    # ---- instruments ----
    def _code_rows(self, asset_type: str) -> list[dict]:
        """仅代码表行(symbol/name/code/exchange), 不含股本。供实时快照代码表复用。"""
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

    def _fetch_shares(self, symbols: list[str]) -> dict[str, dict]:
        """按项目格式 symbol 批量拉流通股本/总股本, 返回 {symbol: {"float_shares":..., "total_shares":...}}。

        仅 instrument_sync(盘前全量)调用, 走 GetFinanceInfo(财务协议)。单只失败跳过。
        """
        if not symbols:
            return {}
        tdx_symbols = [_to_tdx(s) for s in symbols]

        def build_job(piece: list[str]) -> dict:
            return {"op": "finance", "args": {"codes": piece}}

        rows = self._run_concurrent(
            tdx_symbols, "finance", build_job,
            workers=_FINANCE_WORKERS, batch=_FINANCE_BATCH,
        )
        out: dict[str, dict] = {}
        for r in rows:
            sym = _to_symbol(r.get("symbol") or "")
            if not sym:
                continue
            out[sym] = {
                "float_shares": r.get("float_shares"),
                "total_shares": r.get("total_shares"),
            }
        return out

    def get_instruments(self, asset_type="stock") -> list[dict]:
        """标的维表。股票额外附带流通/总股本(供换手率计算), 写进 ext 由 flatten 读取。"""
        rows = self._code_rows(asset_type)
        if asset_type == "stock" and rows:
            shares = self._fetch_shares([r["symbol"] for r in rows])
            for r in rows:
                s = shares.get(r["symbol"])
                r["ext"] = {
                    "float_shares": (s or {}).get("float_shares"),
                    "total_shares": (s or {}).get("total_shares"),
                }
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
