# self-plugin 回退的非插件优化清单（上游同步核对用）

> ⚠️ 本文件为 self-plugin 专属，**不随上游同步**：合并上游时若涉及本文件，
> 一律保留本分支版本（ours）；禁止用 origin/main 覆盖或删除。

> 本分支约定：除 `backend/app/plugins/eltdx/` 与 Dockerfile 的 eltdx 打包外，
> 代码与上游保持一致。以下优化曾在本分支实现并验证，后按约定回退。
> **每次同步上游后逐项核对：上游若已修复则划掉；若未修复且症状复发，
> 与分支维护者确认后再决定是否重新应用。**

最后核对：2026-09-11（上游 ea4d8a8，均未修复）

## 1. 分钟分区写放大优化（高优先级 — 已确认复发）

- 文件：`backend/app/services/kline_sync.py` → `_write_minute_partition`
- 内容：分区合并只处理本次写入触及的 symbol 子集，其余行原样保留；
  去重语义与旧版一致（symbol+datetime 取后到者）
- 效果（实测）：单股 8 分区补齐 21.8s→9.3s；锁内单分区 ~2s→~0.3s
- 复发事故（2026-09-11 23:06）：个股 20 日分时自动补齐在 `repo._write_lock`
  内对每个日期分区做全市场级 concat+unique+sort（~1.2M 行/分区 × N 分区），
  写锁被连续持有几十秒 → watchdog 探针（1s 锁试探，硬编码）连续 2 次失败
  → 后端被判定僵死主动退出（"repository write lock unavailable"）
- 上游修复判据：`_write_minute_partition` 是否只合并触及 symbol 子集；
  或 `app/watchdog.py` `default_probe` 的 1s 锁试探是否放宽

## 2. ETF 分钟独立落盘 kline_etf_minute

- 文件：`kline_sync.py`（`_persist` 按 etf_syms 分流到 kline_etf_minute + 视图刷新）、
  `jobs/daily_pipeline.py`（`_resolve_minute_symbols` 标的池加 ETF）、
  `tickflow/repository.py`（kline_etf_minute 视图）
- 症状：ETF 无法查看历史分时（分钟数据混入股票表或缺失）
- 上游判据：是否存在 `kline_etf_minute` 目录/视图与 ETF 分钟同步标的

## 3. 分钟同步窗口用北京墙钟

- 文件：`kline_sync.py` `sync_and_persist_minute`
- 内容：`now = cn_now().replace(tzinfo=None)` 替代 `datetime.now()`
- 症状：仅 UTC 容器（Docker）触发 —— 增量窗口 end_time 落前一天导致当日分钟
  不同步；DB 读出的 naive last_dt 与 aware now 混比抛异常。Windows 本机
  北京时区下无症状
- 上游判据：该函数 now 的取值

## 4. 指数日K写盘放宽

- 文件：`services/quote_service.py`（写盘条件 `has_asset_rules("index")` →
  `if index_records and self._repo`）
- 症状：未配置指数监控规则的用户，盘中指数日K/分时停在昨天
  （指数日K接口读落盘分区，不写就永远返回昨天）
- 上游判据：`_fetch_full_market_records` 尾部指数写盘条件

## 5. 指数分时默认日期（配套 4）

- 文件：`api/indices.py`（`date.today()` → `cn_today()`）、
  `pages/Indices.tsx`（默认选中"今天"，今天无日K才回退最近交易日）
- 症状：UTC 容器下默认日期错一天；本机表现为盘中默认显示昨天
- 上游判据：indices.py 是否用 cn_today；Indices.tsx selectedDate 默认逻辑

## 6. 多日分时补齐前端修复

- 文件：`components/StockMultiDayIntradayChart.tsx`
- 内容：① 补齐判断基于本地落库交易日（`history.sessions`，即 localSessions）
  而非合并当天实时后的 sessions；② autoSyncRef 状态机（symbol 为空/切换标的
  时清空防重、补齐失败时清空允许重试）
- 症状：本地只有 4~5 天却无补齐横条/按钮（当天实时数据把 sessions.length
  撑到 days 误判"已够"）；补过一次后切回该股不再自动补；补齐失败后永久不再重试
- 上游判据：该组件 autoSync effect 是否仍以 `sessions.length >= days` 为门槛
  （注意上游 f3b916c 已做 prefetch 重构，核对时以最新代码为准）

## 附：其他同步时注意项

- 上游已知测试失败：`tests/test_mining_manager.py::
  test_cancel_running_job_wins_over_worker_success`（上游 ea4d8a8 即失败，
  与本分支无关，勿误判为回归）
- eltdx 插件侧自有能力（不依赖上游、不随同步回退）：iter_daily 流式日K、
  depth5 五档、建连退避重试与 worker 连接复用、get_realtime 代码表 TTL 缓存、
  adj_factor 采用服务端 qfq/raw 比值推导（选型理由见
  `app/plugins/eltdx/provider.py` 模块 docstring「接口选型实测审计」）
- Dockerfile 差异仅 `INCLUDE_ELTDX=1` 与安装其 requirements（合并冲突时保留）
