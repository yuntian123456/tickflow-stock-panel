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
from datetime import datetime
from pathlib import Path

import polars as pl

from app.config import settings
from app.data_providers.normalizer import normalize_adj_factors, normalize_daily
from app.market_time import cn_now
from app.tickflow.rate_limits import chunked

logger = logging.getLogger(__name__)

# eltdx 支持的数据集(financial 不支持 → 不声明, 自动回退 tickflow)。
# full_minute: 全量分钟修复轮(当日全市场批量分钟), 见 get_intraday_batch。
_DATASETS = ("daily", "adj_factor", "minute", "realtime", "full_minute")

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
# 实时快照专用并发: 全市场每轮轮询(秒级), 8 个连接并发拉快照 CPU 偏高;
# 4 个连接即可覆盖网络延迟, 降低 CPU 与服务器压力。
_REALTIME_WORKERS = 4
# 股本(财务)批量拉取分块大小: finance_batch 每次请求的代码数上限(通达信协议限制),
# 超出需分块; instruments_sync 盘前一次性全量拉。
_FINANCE_BATCH = 100
# 日K窗口缓存余量(交易日): 按 start/end 窗口估算条数时多加的裕度, 覆盖
# 边界非交易日/服务器截断等场景; 单次 bars.get(count) 服务端上限 800。
_DAILY_SLACK = 60
# 分钟K单页条数: eltdx bars.get 单次 count 上限 800(超限抛 ProtocolError 截断)。
# 1 分钟 240 根/交易日 → 单页约覆盖 3.3 个交易日, 需按 start 分页才能补全历史窗口。
_MINUTE_PAGE = 800
# 分钟K分页最大页数: 契约要求「分页必须有页数上限(防 count 异常导致死循环)」。
# 单页 800 根, 50 页共 40000 根, 约 166 交易日, 远超分钟同步默认窗口(数天);
# 达到上限说明服务端异常(重复返回满页 / 永不覆盖窗口起点), 记警告而非无限循环。
_MINUTE_MAX_PAGES = 50
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


def _ex_events_from_server_qfq(c, symbol: str, window_days: int | None) -> list[dict]:
    """主路径: 用服务端前复权K线(qfq)与不复权K线的比值推导事件因子。

    (eltdx >=3.1.0 移除 helpers.factors/xdxr 与 equity.build_factor_response,
    服务端 qfq 是通达信权威前复权, 含现金分红, 能覆盖 xdxr 未登记的拆分。)
      cum(D) = qfq_close(D) / raw_close(D)   (累积前复权比值, 拆分为 1/3 → 1.0)
      ex(D)  = cum(D) / cum(D-1)             (拆分日 ex≈3.0)
    阈值 0.01 过滤 qfq/raw 的日内舍入漂移(~0.2%), 保留真实拆分/分红(通常>1%)。
    """
    count = min(window_days + _DAILY_SLACK, 800) if window_days is not None else 800
    kind = _kind_for(symbol)

    def _closes(adjust: str) -> dict:
        s = c.bars.get(
            _to_tdx(symbol), period="day", adjust=adjust, count=count, kind=kind,
        )
        return {b.time.strftime("%Y-%m-%d"): float(b.close) for b in (s.bars or [])}

    try:
        raw = _closes("none")
        qfq = _closes("qfq")
    except Exception as e:  # noqa: BLE001
        logger.warning("eltdx %s 服务端复权推导失败: %s", symbol, e)
        return []
    rows: list[dict] = []
    prev_cum = None
    for d in sorted(set(raw) & set(qfq)):
        rc, qc = raw[d], qfq[d]
        if rc == 0:
            continue
        cum = qc / rc
        if prev_cum is not None and prev_cum != 0.0:
            ex = cum / prev_cum
            if abs(ex - 1.0) > 0.01:
                rows.append({"symbol": symbol, "trade_date": d, "ex_factor": ex})
        prev_cum = cum
    return rows


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


def _is_block_code(full_code: str) -> bool:
    """判断 TDX full_code(sh880076/sz000001) 或项目符号是否为通达信板块指数。

    880xxx(概念/风格/地区)、881xxx(行业)是通达信板块指数, 归属上海交易所但非可交易
    A 股。主项目实时拆分的 index 集合来自 TickFlow 的 instruments_index(不含板块指数),
    若不剔除会被当作 stock 写入 kline_daily, 污染涨幅/成交额榜。
    """
    num = full_code[2:] if len(full_code) >= 8 else full_code
    return num[:3] in ("880", "881")


def _kind_for(symbol: str) -> str:
    """返回 bars 所需的 kind: 指数用 "index", 其余(股票/ETF)用 "stock"。"""
    return "index" if _is_index_symbol(symbol) else "stock"


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
    """eltdx KlineBar → 内部分钟K行。

    datetime 直接取北京时间墙钟 naive (去 tz), 与分钟K契约一致
    (CONTRIBUTING §3.3): kline_minute.datetime 必须是北京墙钟, 如 09:35:00。
    前端分时组件按交易时段时轴映射, 不再做时区换算。
    """
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
        # 受管指数/ETF 集合缓存(读维表 parquet)。get_realtime 秒级轮询, 每轮读文件浪费;
        # TTL 内复用, 维表变更在 TTL 刷新后自动生效。
        self._managed_cache: dict[str, set[str]] = {}
        self._managed_cache_at: float = 0.0
        self._managed_cache_ttl = 300.0

    def close(self) -> None:  # loader.load_all 会对每个 provider 调 close
        pass

    def _client(self):
        """每次调用独立建连: 避免多线程共享 socket。eltdx 进入 with 即连, 退出自动断开。"""
        from eltdx import TdxClient

        return TdxClient(timeout=10)

    def _run_concurrent(self, symbols: list[str], fetch_one, on_chunk_done=None, workers: int | None = None) -> list:
        """静态分片并发拉取: symbols 均分给 worker 个 worker, 每个 worker 建连一次并复用。

        fetch_one(c, sym) -> 返回该 symbol 的结果(DataFrame / dict / None), 异常由框架捕获记日志。
        返回所有成功结果的列表(顺序不保证); on_chunk_done(cur, tot) 按 _BATCH 粒度回调进度。
        workers 可覆盖默认 _WORKERS: 实时快照(秒级轮询)传小值以降低每轮连接数。

        进度实时性: worker 每拉完一只即上报(非等整块完成), 否则慢服务器下
        首个 worker 完成前进度长时间不动, 前端误判为卡死。
        """
        if not symbols:
            return []
        total = len(symbols)
        n_workers = min(workers or _WORKERS, total)
        size = max(1, (total + n_workers - 1) // n_workers)
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
                # eltdx >=3.1.0: bars.all 并入 bars.get(..., all_pages=True)
                series = c.bars.get(
                    _to_tdx(sym), period="day", adjust="none", kind=kind, all_pages=True,
                )
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
            # 指数自身不发生分红/送转除权, 不产生 ex_factor; 且服务端复权推导
            # 不带 kind(指数会解析错位), 故指数直接跳过。
            if _is_index_symbol(sym):
                return None
            # eltdx >=3.1.0 移除 helpers.factors/xdxr 与 equity.build_factor_response,
            # 统一改用服务端前复权(qfq)与不复权K线比值推导事件因子(含现金分红, 权威):
            # ex(D) = cum(D)/cum(D-1), cum = qfq_close/raw_close。小窗口用单页 bars.get。
            window_days = None
            if start_time and end_time:
                window_days = max(0, (end_time - start_time).days)
            rows = _ex_events_from_server_qfq(c, sym, window_days)
            if not rows:
                return None
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
            # 分页契约: 空页终止 + 页数上限(防服务端异常死循环)。
            kind = _kind_for(sym)
            rows: list[dict] = []
            start = 0
            for _page in range(_MINUTE_MAX_PAGES):
                count = first_count if start == 0 else _MINUTE_PAGE
                series = c.bars.get(
                    _to_tdx(sym), period=period, adjust="none",
                    start=start, count=count,
                    kind=kind,
                )
                bars = series.bars or []
                if not bars:
                    break  # 空页终止: 服务端无更早数据
                rows.extend(_bar_to_minute_row(b, sym) for b in bars)
                if start_time is None or _naive(bars[0].time) < _naive(start_time):
                    break  # 预览模式 / 本页最早一根已早于窗口起点 → 覆盖完成
                if len(bars) < count:
                    break  # 服务端无更早数据(不足一页)
                start += count
            else:
                logger.warning(
                    "eltdx %s 分钟K分页达到上限 %d 页, 可能未覆盖完整窗口",
                    sym, _MINUTE_MAX_PAGES,
                )
            df = pl.DataFrame(rows) if rows else pl.DataFrame()
            if df.is_empty():
                return None
            df = df.select(_MINUTE_CANONICAL)
            # 调用方 start_time/end_time 为北京墙钟 (fetch_minute_single 的 9:25/15:05 带
            # +08:00 时区, sync 为 naive), 本行 datetime 已是北京墙钟 naive, 过滤边界
            # 统一用 _naive 取北京墙钟即可直接比较。
            if start_time:
                df = df.filter(pl.col("datetime") >= _naive(start_time))
            if end_time:
                df = df.filter(pl.col("datetime") <= _naive(end_time))
            return df if not df.is_empty() else None

        frames = self._run_concurrent(symbols, fetch_one, on_chunk_done)
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    # ---- full_minute (全量分钟修复轮) ----
    def get_intraday_batch(
        self,
        symbols: list[str],
        count: int = 300,  # noqa: ARG002 - 与插件契约对齐; 当日窗口单页(<=800)即覆盖
        asset_type: str = "stock",
    ) -> pl.DataFrame:
        """全量分钟修复轮: 当日窗口全市场批量分钟K (canonical 8 列同 get_minute)。

        声明 full_minute 数据集后, 被 minute_refresh 在冷启动/覆盖断档/连续空轮时调用
        (docs/plugin-development.md)。复用 get_minute 的当日窗口单页拉取(每只 1 次
        bars.get)与 _run_concurrent 并发; 返回北京墙钟 naive, 时区守卫由调用方
        fetch_intraday_custom_batch 统一执行。未实现 get_intraday_latest → 服务
        自动降级为仅修复轮(节奏下限 60s), 避免全市场分钟增量请求压垮 TDX 服务器。
        """
        if not symbols:
            return pl.DataFrame()
        now = cn_now().replace(tzinfo=None)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return self.get_minute(
            symbols, start_time=start, end_time=now,
            asset_type=asset_type, freq="1m",
        )

    # ---- realtime ----
    def _read_managed_symbols(self, dirname: str) -> set[str]:
        """读取主项目已同步的指数/ETF 维表代码集(目录形态 data/<dirname>/<dirname>.parquet)。

        实时快照据此只返回主项目受管的指数/ETF, 避免其余指数/ETF 被当作股票写入 kline_daily。
        结果按实例级 TTL 缓存(秒级轮询不重复读文件), 维表变更在 TTL 刷新后自动生效。
        文件缺失/为空/读取失败时返回空集(调用方据此跳过过滤, 回退旧行为)。
        """
        import time as _t
        now = _t.monotonic()
        if now - self._managed_cache_at < self._managed_cache_ttl and dirname in self._managed_cache:
            return self._managed_cache[dirname]
        try:
            p = Path(settings.data_dir) / dirname / f"{dirname}.parquet"
            out = set()
            if p.exists():
                df = pl.read_parquet(p, columns=["symbol"])
                if not df.is_empty():
                    out = set(df["symbol"].cast(pl.Utf8).to_list())
        except Exception as e:  # noqa: BLE001
            logger.warning("eltdx 读取 %s 受管集合失败: %s", dirname, e)
            out = set()
        self._managed_cache[dirname] = out
        self._managed_cache_at = now
        return out

    def get_realtime(self) -> list[dict]:
        """全市场实时快照: 先取代码表, 再按 TDX 批量上限分批取快照拼成全市场。"""
        try:
            with self._client() as c:
                # 全市场实时快照需包含 ETF 与指数。旧实现只拉 A股代码,
                # 导致 quote_service 切分后的 etf/index 无实时记录(数据不更新)。
                # 三个 codes 方法底层共享 codes.all_markets() 缓存, 不会重复拉代码表。
                # 只返回主项目已同步的指数/ETF(受管集合): 其余指数(通达信板块指数 880/881xxx、
                # 999xxx、399379/399380 等统计指数)会被 quote_service 当作 stock 写入
                # kline_daily, 污染涨幅/成交额榜。受管集合每次轮询重建时重新读取, 维表变更
                # 会在代码表缓存(TTL)刷新后自动生效; 集合为空(未同步)则不过滤, 回退旧行为。
                idx_set = self._read_managed_symbols("instruments_index")
                etf_set = self._read_managed_symbols("instruments_etf")
                codes = []
                codes += c.codes.all_a_shares()
                codes += [
                    x for x in c.codes.all_etfs()
                    if not etf_set or _to_symbol(x) in etf_set
                ]
                codes += [
                    x for x in c.codes.all_indices()
                    if not _is_block_code(x) and (not idx_set or _to_symbol(x) in idx_set)
                ]

            # 先按 _SNAPSHOT_BATCH 分组, 再把组交给 _run_concurrent 并发分片:
            # _run_concurrent 是「每 worker 每 symbol 调一次 fetch_one」, 若把原始
            # codes 传进去, fetch_one 收到的就是单个字符串, 再 chunked 会得到单字符。
            groups = list(chunked(codes or [], _SNAPSHOT_BATCH))

            def fetch_one(cc, group) -> list[dict]:
                out: list[dict] = []
                quotes = cc.quotes.get_snapshots(list(group))
                for q in quotes or []:
                    pct = getattr(q, "change_pct", None) or 0.0
                    last = q.last_price
                    prev = q.pre_close_price
                    # 集合竞价/停牌时段现价可能为 0(未出价/无成交): 用昨收兜底现价(交易所惯例),
                    # 避免 K 线/现价显示为 0; 涨跌幅为 0(视为未成交, 不计涨跌)而非 -100%。
                    if last is None or float(last) == 0.0:
                        last = prev
                        pct = 0.0
                    out.append(
                        {
                            "symbol": _to_symbol(q.full_code),
                            "last_price": last,
                            "prev_close": prev,
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
            # 实时快照是秒级轮询, 4 个连接即可摊薄网络延迟, 避免每轮 8 连接并发拉全市场。
            out_all: list[list[dict]] = self._run_concurrent(groups, fetch_one, workers=_REALTIME_WORKERS)
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
        if dataset == "full_minute":
            df = self.get_intraday_batch(symbols)
            return self._preview("full_minute", df)
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
                full_codes: list[str] = []
                for market in ("sh", "sz", "bj"):
                    for it in c.codes.all(market):
                        if getattr(it, "category", "") != target:
                            continue
                        if target == "index" and _is_block_code(it.full_code):
                            # 通达信板块指数(880/881xxx)非可交易 A 股, 不入维表
                            continue
                        full_codes.append(it.full_code)
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
                # 股票补流通/总股本(供换手率计算), 写进 ext 由 flatten 读取
                if asset_type == "stock" and full_codes:
                    share_map = self._fetch_shares(c, full_codes)
                    for item in items:
                        s = share_map.get(item["symbol"])
                        if s:
                            item["ext"].update(s)
                return items
        except Exception as e:
            logger.warning("eltdx instruments 拉取失败: %s", e)
            return []

    @staticmethod
    def _fetch_shares(c, full_codes: list[str]) -> dict[str, dict]:
        """用 corporate.finance_batch 拉流通/总股本(单位: 股), 按 project symbol 缓存返回。

        单块失败跳过, 不阻断整批; finance_batch 内部按协议上限分块, 这里再按
        _FINANCE_BATCH 分块以控制单次请求规模。
        """
        share_map: dict[str, dict] = {}
        for chunk in chunked(full_codes, _FINANCE_BATCH):
            try:
                batch = c.corporate.finance_batch(list(chunk), fields=["流通股本", "总股本"])
            except Exception as e:  # noqa: BLE001
                logger.warning("eltdx finance_batch 失败(跳过该批): %s", e)
                continue
            for rec in batch or []:
                fc = (rec or {}).get("full_code")
                if not fc:
                    continue
                share_map[_to_symbol(fc)] = {
                    "float_shares": (rec or {}).get("流通股本"),
                    "total_shares": (rec or {}).get("总股本"),
                }
        return share_map
