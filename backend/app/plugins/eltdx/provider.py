"""通达信行情(eltdx)内置数据源 provider。

基于 <https://github.com/electkismet/eltdx> (纯 Python 通达信在线行情客户端)。

方法签名对齐 custom.GenericHTTPProvider(service 分流点按这套签名调用),
因此注入 custom loader 注册表后, 各 service 无需改动即可路由到本 provider。

数据集: daily / adj_factor / minute / realtime + instruments(标的维表)。
financial 未声明: financial_sync 直连 TickFlow SDK, 不走 provider 抽象。

口径注意(与 TickFlow 数据核对):
- realtime 的 change_pct 上游为百分数(如 3.66), 本 provider 转小数制(0.0366)。
- volume 单位为「手」(eltdx volume_lots / total_hand), 与 TickFlow 口径可能不同。
- eltdx 的 qfq_factor 是**每日累积前复权系数**(最新日=1.0), 而 pipeline 期望
  ex_factor 为**每次除权事件的 pre/post 比值**(个股级, 非累积)。故 get_adj_factors
  先把每日系数序列转成事件因子: ex(D) = qfq(D)/qfq(D-1), 仅保留跳变日。
"""

from __future__ import annotations

import concurrent.futures as _futures
import logging
import threading as _threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta

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
_BATCH = 80
_SNAPSHOT_BATCH = 80
# 并发拉取连接数: 静态分片后每 worker 一个 TDX 连接并复用。
# 实测(20只日K): 逐只建连 2.49s → 8 worker 分片复用 0.70s, 提升 ~3.5 倍;
# 4/8/16 worker 差距极小(0.71/0.70/0.67s), 取 8 平衡速度与服务器压力。
_WORKERS = 8
# 日K窗口缓存余量(交易日): 按 start/end 窗口估算条数时多加的裕度, 覆盖
# 边界非交易日/服务器截断等场景; 单次 bars.get(count) 服务端上限 800。
_DAILY_SLACK = 60
# 分钟K单页条数: eltdx bars.get 单次 count 上限 800(超限抛 ProtocolError 截断)。
# 1 分钟 240 根/交易日 → 单页约覆盖 3.3 个交易日, 需按 start 分页才能补全历史窗口。
_MINUTE_PAGE = 800
_MINUTE_CANONICAL = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
# 每个 period 单交易日分钟K根数 (A股 9:30-11:30 + 13:00-15:00 = 240 分钟)。
_BARS_PER_DAY = {"1m": 240, "5m": 48, "15m": 16, "30m": 8, "60m": 4}


def _minute_count(start_time, end_time, period: str) -> int:
    """按窗口估算该拉的分钟K根数, 作为首屏 count(小窗口避免一次拉满 800)。"""
    bars_per_day = _BARS_PER_DAY.get(period, 240)
    if start_time and end_time:
        cal_days = max(1, (end_time - start_time).days)
        trading_days = max(1, int(cal_days * 5 / 7) + 1)
    else:
        trading_days = 4
    return max(bars_per_day, trading_days * bars_per_day + bars_per_day)


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


def _is_index_symbol(symbol: str) -> bool:
    """判断项目符号(如 000001.SH)是否为指数。

    eltdx 把指数与股票/ETF 的 K 线协议区分开: bars 需传 kind="index", 否则解析器
    不读宽幅(涨跌家数)字段会导致分钟/日K 错位而缺失。这里按代码模式判定。
    """
    full = _to_tdx(symbol)  # sh000001 / sz399001 / sh510300 ...
    m, num = full[:2], full[2:]
    if m == "sh":
        return num.startswith(("000", "880", "881"))  # 上证系列/板块指数
    if m == "sz":
        return num.startswith("399")  # 深证系列指数
    if m == "bj":
        return num.startswith("899")  # 北证指数
    return False


def _kind_for(symbol: str) -> str:
    """返回 bars 所需的 kind: 指数用 "index", 其余(股票/ETF)用 "stock"。"""
    return "index" if _is_index_symbol(symbol) else "stock"


def _naive(t) -> datetime:
    """去掉时区保留墙钟时间: eltdx 返回 UTC 带时区 datetime, 而项目内
    start_time/end_time 均为无时区(naive)北京时间, 两者直接比较会因
    dtype 不一致报错。通达信协议的时间本就是交易所本地墙钟, 去 tz 后口径对齐。"""
    return t.replace(tzinfo=None) if t.tzinfo is not None else t


def _to_utc_naive(t) -> datetime:
    """eltdx 分钟时间(北京时间墙钟) → 真实 UTC naive (北京墙钟 - 8h)。

    前端分时组件把 datetime 字符串按真实 UTC 解析并 +8 还原为北京墙钟
    (见 EChartsIntraday.tsx fmtTime), 与 tickflow 的 from_epoch(ms) 口径一致。
    eltdx 返回的 bar.time 是带 UTC tzinfo 的北京时间墙钟, 直接 _naive 会得到
    北京时间(如 09:35), 前端 +8 后变成 17:35 无法映射到 242 个全天刻度 → 曲线全空。
    故分钟时间须先转真实 UTC naive(01:35) 再给前端。
    """
    return _naive(t) - timedelta(hours=8)


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
    """eltdx KlineBar → 内部分钟K行。

    datetime 转真实 UTC naive (北京时间墙钟 - 8h, 见 _to_utc_naive), 与
    tickflow from_epoch(ms) 及前端 +8 还原的口径保持一致。
    """
    return {
        "symbol": symbol,
        "datetime": _to_utc_naive(bar.time),
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

    def _run_concurrent(self, symbols: list[str], fetch_one, on_chunk_done=None) -> list:
        """静态分片并发拉取: symbols 均分给 _WORKERS 个 worker, 每个 worker 建连一次并复用。

        fetch_one(c, sym) -> 返回该 symbol 的结果(DataFrame / dict / None), 异常由框架捕获记日志。
        返回所有成功结果的列表(顺序不保证); on_chunk_done(cur, tot) 按 _BATCH 粒度回调进度。

        进度实时性: worker 每拉完一只即上报(非等整块完成), 否则慢服务器下
        首个 worker 完成前进度长时间不动, 前端误判为卡死。
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

        def worker(chunk):
            out: list = []
            with self._client() as c:
                for sym in chunk:
                    try:
                        row = fetch_one(c, sym)
                        if row is not None:
                            out.append(row)
                    except Exception as e:  # noqa: BLE001
                        logger.warning("eltdx %s 拉取失败: %s", sym, e)
                    tick()
            return out

        results: list = []
        with _futures.ThreadPoolExecutor(max_workers=len(chunks)) as pool:
            futures = [pool.submit(worker, ch) for ch in chunks]
            for fut in _futures.as_completed(futures):
                results.extend(fut.result())
        return results

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

        # 窗口内最多 (end-start).days 根日线: 窗口较小时用单次 bars.get(count)
        # 替代 bars.all 全量翻页(每代码 3~4 次请求 → 1 次), 日K同步提速明显;
        # 窗口超过单请求上限(约 2.7 年)或未传窗口时回退全量翻页保证完整性。
        window_days = None
        if start_time and end_time:
            window_days = max(0, (end_time - start_time).days)
        use_single = window_days is not None and window_days + _DAILY_SLACK <= 800

        def fetch_one(c, sym) -> pl.DataFrame | None:
            kind = _kind_for(sym)
            if use_single:
                series = c.bars.get(
                    _to_tdx(sym), period="day", adjust="none", count=window_days + _DAILY_SLACK,
                    kind=kind,
                )
            else:
                series = c.bars.all(_to_tdx(sym), period="day", adjust="none", kind=kind)
            rows = [_bar_to_daily_row(b, sym) for b in (series.bars or [])]
            df = normalize_daily(rows, source=self.name)
            if df.is_empty():
                return None
            if start_time:
                df = df.filter(pl.col("date") >= start_time.date())
            if end_time:
                df = df.filter(pl.col("date") <= end_time.date())
            return df if not df.is_empty() else None

        frames = self._run_concurrent(symbols, fetch_one, on_chunk_done)
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

        def fetch_one(c, sym) -> pl.DataFrame | None:
            # 指数自身不发生分红/送转除权, 不产生 ex_factor; 且 helpers.factors
            # 内部 bars.all 不带 kind(指数会解析错位), 故指数直接跳过。
            if _is_index_symbol(sym):
                return None
            # 快速路径: helpers.factors 内部是 bars.all(day) 全量翻页 + capital_changes, 实测 ~0.64s/只;
            # 改单次 bars.get(day) + helpers.xdxr + build_factor_response, 实测 ~0.07s/只(约 9x 提速),
            # 最近 30 交易日 qfq_factor 逐日对比零差异(基准验证)。窗口超出单页覆盖时回退全量保完整性。
            window_days = None
            if start_time and end_time:
                window_days = max(0, (end_time - start_time).days)
            if window_days is not None and window_days + _DAILY_SLACK <= _MINUTE_PAGE:
                from eltdx.equity import build_factor_response

                series = c.bars.get(
                    _to_tdx(sym), period="day", adjust="none",
                    count=window_days + _DAILY_SLACK,
                    kind=_kind_for(sym),
                )
                factors = build_factor_response(series, c.helpers.xdxr(_to_tdx(sym)))
            else:
                factors = c.helpers.factors(_to_tdx(sym))
            # eltdx 的 qfq_factor 是每日累积前复权系数(最新日=1.0), 并非除权事件因子。
            # pipeline 期望 ex_factor = 每次除权事件的 pre/post 比值(个股级,非累积),
            # 直接存每日系数会让 cum_prod 连乘爆表(f64→i64 溢出报错)。
            # 转换: ex(D) = qfq(D)/qfq(D-1), 记在跳变日(D), 仅保留 |ex-1|>1e-9 的除权日。
            rows: list[dict] = []
            prev_qfq: float | None = None
            for item in sorted(factors.items or [], key=lambda it: it.time):
                qfq = float(item.qfq_factor)
                if prev_qfq is not None and prev_qfq != 0.0:
                    ex = qfq / prev_qfq
                    if abs(ex - 1.0) > 1e-9:
                        rows.append(
                            {
                                "symbol": sym,
                                "trade_date": item.time.date(),
                                "ex_factor": ex,
                            }
                        )
                prev_qfq = qfq
            df = normalize_adj_factors(rows, source=self.name)
            if df.is_empty():
                return None
            if start_time:
                df = df.filter(pl.col("trade_date") >= start_time.date())
            if end_time:
                df = df.filter(pl.col("trade_date") <= end_time.date())
            return df if not df.is_empty() else None

        frames = self._run_concurrent(symbols, fetch_one, on_chunk_done)
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
        # 首次页按窗口估算根数(小窗口不一次拉满 800), 更大窗口仍按 _MINUTE_PAGE 分页。
        first_count = max(1, min(_minute_count(start_time, end_time, period), _MINUTE_PAGE))

        def fetch_one(c, sym) -> pl.DataFrame | None:
            # 单次 count 上限 800 根(~4 个交易日), 只取一页会让历史窗口外的分钟数据
            # 全部缺失 → 前端只能看到最近 4 天。按 start 分页向前翻到覆盖窗口起点;
            # start_time 未传(设置页预览)时保持单页 800 根, 避免预览拉全量。
            # 指数需传 kind="index", 否则宽幅字段导致协议解析错位(分钟K缺失)。
            kind = _kind_for(sym)
            rows: list[dict] = []
            start = 0
            while True:
                count = first_count if start == 0 else _MINUTE_PAGE
                series = c.bars.get(
                    _to_tdx(sym), period=period, adjust="none",
                    start=start, count=count,
                    kind=kind,
                )
                bars = series.bars or []
                if not bars:
                    break
                rows.extend(_bar_to_minute_row(b, sym) for b in bars)
                if start_time is None or _naive(bars[0].time) < _naive(start_time):
                    break  # 预览模式 / 本页最早一根已早于窗口起点 → 覆盖完成
                if len(bars) < count:
                    break  # 服务器无更早数据
                start += count
            df = pl.DataFrame(rows) if rows else pl.DataFrame()
            if df.is_empty():
                return None
            df = df.select(_MINUTE_CANONICAL)
            # 调用方 start_time/end_time 为北京墙钟 naive(见 fetch_minute_single /
            # get_minute_batch 的 9:25/15:05 构造), 而本行 datetime 已是真实 UTC
            # naive, 过滤边界须同步 -8h 才能正确比较。
            if start_time:
                df = df.filter(pl.col("datetime") >= _to_utc_naive(start_time))
            if end_time:
                df = df.filter(pl.col("datetime") <= _to_utc_naive(end_time))
            return df if not df.is_empty() else None

        frames = self._run_concurrent(symbols, fetch_one, on_chunk_done)
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    # ---- realtime ----
    def get_realtime(self) -> list[dict]:
        """全市场实时快照: 先取代码表, 再按 TDX 批量上限分批取快照拼成全市场。"""
        try:
            with self._client() as c:
                # 全市场实时快照需包含 ETF 与指数。旧实现只拉 A股代码,
                # 导致 quote_service 切分后的 etf/index 无实时记录(数据不更新)。
                # 三个 codes 方法底层共享 codes.all_markets() 缓存, 不会重复拉代码表。
                codes = (
                    c.codes.all_a_shares()
                    + c.codes.all_etfs()
                    + c.codes.all_indices()
                )

            # 先按 _SNAPSHOT_BATCH 分组, 再把组交给 _run_concurrent 并发分片:
            # _run_concurrent 是「每 worker 每 symbol 调一次 fetch_one」, 若把原始
            # codes 传进去, fetch_one 收到的就是单个字符串, 再 chunked 会得到单字符。
            groups = list(chunked(codes or [], _SNAPSHOT_BATCH))

            def fetch_one(cc, group) -> list[dict]:
                out: list[dict] = []
                quotes = cc.quotes.get_snapshots(list(group))
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

            # 静态分片并发: 每个 worker 一个连接, 每次快照调用不超过 80 个代码
            out_all: list[list[dict]] = self._run_concurrent(groups, fetch_one)
            merged: list[dict] = []
            for batch in out_all:
                merged.extend(batch)
            return merged
        except Exception as e:
            logger.warning("eltdx realtime 拉取失败: %s", e)
            return []

    # ---- 测试(设置页试拉) ----
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
            rows = self.get_realtime()
            head = rows[:5]
            return {
                "provider": self.name,
                "dataset": "realtime",
                "rows": len(rows),
                "columns": list(head[0].keys()) if head else [],
                "preview": head,
            }
        raise ValueError(f"eltdx 不支持数据集: {dataset}")

    @staticmethod
    def _preview(dataset: str, df: pl.DataFrame) -> dict:
        return {
            "provider": "eltdx",
            "dataset": dataset,
            "rows": df.height,
            "columns": df.columns,
            "preview": df.head(5).to_dicts() if not df.is_empty() else [],
        }

    # ---- instruments ----
    def get_instruments(self, asset_type: str = "stock") -> list[dict]:
        """标的维表, 供 instrument_sync 复用 flatten 路径(列结构与 tickflow 一致)。

        注意: codes.all_xxx() 只返回 full_code 字符串, 不含 name/code/exchange;
        这里改用 codes.all(market) 取 SecurityCode 对象并按 category 过滤
        (a_share / etf / index)。
        """
        try:
            with self._client() as c:
                target = {"etf": "etf", "index": "index"}.get(asset_type, "a_share")
                items: list[dict] = []
                for market in ("sh", "sz", "bj"):
                    for it in c.codes.all(market):
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
