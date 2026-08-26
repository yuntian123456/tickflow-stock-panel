// tdxgo — injoyai/tdx 通达信行情桥接二进制。
//
// 通过 stdin 读入单个 JSON 请求, 经 stdout 输出单个 JSON 响应(与 stocksdk/bridge.mjs 协议一致)。
// 请求: {"op": "...", "args": {...}}
// 响应: {"ok": true, "data": ...} 或 {"ok": false, "error": "..."}
//
// 支持 op: ping / daily / adj_factor / minute / realtime / instruments
//
// 数据口径(对齐 eltdx provider / 项目内部 schema):
//   - 价格: protocol.Price 单位为「厘」(元×1000), 用 .Float64() 转元。
//   - volume: 沿用 tdx 库归一化结果; 项目约定为「手」。
//   - 分钟 datetime: TDX 返回北京墙钟, 需转真实 UTC naive (墙钟 - 8h),
//     与前端分时 +8 还原口径一致(参考 eltdx._to_utc_naive)。
//   - 除权因子: 用 gb.QFQKlineDay 直接取前复权日线, 逐日 ex(D)=qfq(D)/qfq(D-1),
//     仅保留跳变日(与 eltdx 的 ex_factor 事件因子语义一致)。
package main

import (
	"encoding/json"
	"os"
	"time"

	"github.com/injoyai/logs"
	"github.com/injoyai/tdx"
	"github.com/injoyai/tdx/protocol"
)

// ---- JSON 协议 ----

type request struct {
	Op   string          `json:"op"`
	Args json.RawMessage `json:"args"`
}

type response struct {
	Ok    bool        `json:"ok"`
	Data  interface{} `json:"data,omitempty"`
	Error string      `json:"error,omitempty"`
}

func main() {
	// 把库内日志(信息/调试等)重定向到 stderr, 保证 stdout 只输出纯净 JSON,
	// 否则 tdx 库的连接日志会污染 stdout 导致 Python 桥接解析失败。
	logs.SetWriter(os.Stderr)

	var rq request
	if err := json.NewDecoder(os.Stdin).Decode(&rq); err != nil {
		respond(response{Ok: false, Error: "invalid request: " + err.Error()})
		return
	}

	// ping 不建连, 用于 availability 快速探活。
	if rq.Op == "ping" {
		respond(response{Ok: true, Data: map[string]string{"status": "ok", "version": "tdxgo"}})
		return
	}

	c, err := tdx.DialDefault()
	if err != nil {
		respond(response{Ok: false, Error: "connect: " + err.Error()})
		return
	}

	switch rq.Op {
	case "daily":
		opDaily(c, rq.Args)
	case "minute":
		opMinute(c, rq.Args)
	case "adj_factor":
		opAdjFactor(c, rq.Args)
	case "realtime":
		opRealtime(c, rq.Args)
	case "instruments":
		opInstruments(c, rq.Args)
	default:
		respond(response{Ok: false, Error: "unknown op: " + rq.Op})
	}
}

func respond(r response) {
	b, _ := json.Marshal(r)
	os.Stdout.Write(b)
}

// ---- daily ----

func opDaily(c *tdx.Client, args json.RawMessage) {
	var a struct {
		Symbols []string `json:"symbols"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		respond(response{Ok: false, Error: "daily args: " + err.Error()})
		return
	}
	rows := make([]map[string]any, 0, len(a.Symbols))
	for _, code := range a.Symbols {
		resp, err := fetchDayAll(c, code)
		if err != nil {
			continue // 单只失败不中断整批
		}
		for _, k := range resp.List {
			rows = append(rows, map[string]any{
				"symbol": code,
				"date":   k.Time.Format("2006-01-02"),
				"open":   k.Open.Float64(),
				"high":   k.High.Float64(),
				"low":    k.Low.Float64(),
				"close":  k.Close.Float64(),
				"volume": k.Volume,             // 单位: 手(沿用库归一化)
				"amount": k.Amount.Float64(),   // 单位: 元
			})
		}
	}
	respond(response{Ok: true, Data: rows})
}

// ---- minute ----

var freqType = map[string]uint8{
	"1m":  protocol.TypeKlineMinute,
	"5m":  protocol.TypeKline5Minute,
	"15m": protocol.TypeKline15Minute,
	"30m": protocol.TypeKline30Minute,
	"60m": protocol.TypeKline60Minute,
}

func opMinute(c *tdx.Client, args json.RawMessage) {
	var a struct {
		Symbols []string `json:"symbols"`
		Freq    string   `json:"freq"`
		Count   int      `json:"count"` // 最多拉取的分钟K根数(限界, 避免拉满库上限24000根)
	}
	if err := json.Unmarshal(args, &a); err != nil {
		respond(response{Ok: false, Error: "minute args: " + err.Error()})
		return
	}
	typ, ok := freqType[a.Freq]
	if !ok {
		typ = protocol.TypeKlineMinute // 默认 1m
	}
	limit := a.Count
	if limit <= 0 {
		limit = 800 // 默认最近约 3.3 个交易日, 兼顾预览
	}
	rows := make([]map[string]any, 0)
	for _, code := range a.Symbols {
		resp, err := fetchKlineBounded(c, typ, code, limit)
		if err != nil {
			continue // 单只失败不中断整批
		}
		for _, k := range resp.List {
			rows = append(rows, map[string]any{
				"symbol":   code,
				"datetime": beijingToUTC(k.Time),
				"open":     k.Open.Float64(),
				"high":     k.High.Float64(),
				"low":      k.Low.Float64(),
				"close":    k.Close.Float64(),
				"volume":   k.Volume, // 单位: 手(库已对分钟÷100)
				"amount":   k.Amount.Float64(),
			})
		}
	}
	respond(response{Ok: true, Data: rows})
}

// fetchKlineBounded 只取最近 limit 根分钟K(指数走 GetIndex 口径), 分页按 800 前进。
// 相比 GetKlineAll(库上限 24000 根, 约 91 天), 小窗口(如单日分时)只拉几十~几百根,
// 避免全市场分钟同步把上百GB数据打回桥接层。
func fetchKlineBounded(c *tdx.Client, typ uint8, code string, limit int) (*protocol.KlineResp, error) {
	const page = 800
	if limit <= 0 {
		limit = page
	}
	if limit <= page {
		if protocol.IsIndex(code) {
			return c.GetIndex(typ, code, 0, uint16(limit))
		}
		return c.GetKline(typ, code, 0, uint16(limit))
	}
	resp := &protocol.KlineResp{}
	for start := uint16(0); len(resp.List) < limit && start < 24000; start += page {
		var r *protocol.KlineResp
		var err error
		if protocol.IsIndex(code) {
			r, err = c.GetIndex(typ, code, start, page)
		} else {
			r, err = c.GetKline(typ, code, start, page)
		}
		if err != nil {
			return nil, err
		}
		if len(r.List) == 0 {
			break
		}
		resp.List = append(r.List, resp.List...) // 旧页在前, 整体升序
		if len(r.List) < page {
			break // 已无更早数据
		}
	}
	if len(resp.List) > limit {
		resp.List = resp.List[len(resp.List)-limit:] // 仅保留最新 limit 根
	}
	resp.Count = uint16(len(resp.List))
	return resp, nil
}

// ---- adj_factor ----

func opAdjFactor(c *tdx.Client, args json.RawMessage) {
	var a struct {
		Symbols []string `json:"symbols"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		respond(response{Ok: false, Error: "adj_factor args: " + err.Error()})
		return
	}

	rows := make([]map[string]any, 0)
	for _, code := range a.Symbols {
		resp, err := c.GetKlineDayAll(code)
		if err != nil {
			continue // 单只失败不中断整批
		}
		ks := resp.List // 不复权日线, 时间升序

		// 用单标的接口直取除权除息事件, 避免 NewGbbq 首次触发全市场 Update(极慢)。
		gbbqResp, err := c.GetGbbq(code)
		if err != nil {
			continue
		}
		var xrxds protocol.XRXDs
		for _, g := range gbbqResp.List {
			if g.IsXRXD() {
				xrxds = append(xrxds, g.XRXD())
			}
		}

		// Factors() 返回每日仿射复权系数; QFQMul 为纯比例乘法因子(锚定最新日=1.0,
		// 非除权日恒为常量)。按 QFQMul 逐日比值得到纯净的除权事件因子(送转/配股),
		// 不含日常涨跌噪声; 若用 qfq(D)/qfq(D-1) 会混入价格波动导致因子天天非1。
		factors := xrxds.Pre(ks).Factors()

		// ex(D) = QFQMul(D)/QFQMul(D-1), 仅保留 |ex-1|>1e-9 的除权跳变日(事件因子)。
		var prev float64
		for _, f := range factors {
			cur := f.QFQMul
			if prev != 0.0 {
				ex := cur / prev
				if abs(ex-1.0) > 1e-9 {
					rows = append(rows, map[string]any{
						"symbol":     code,
						"trade_date": f.Time.Format("2006-01-02"),
						"ex_factor":  ex,
					})
				}
			}
			prev = cur
		}
	}
	respond(response{Ok: true, Data: rows})
}

// ---- realtime ----

func opRealtime(c *tdx.Client, args json.RawMessage) {
	var a struct {
		Codes []string `json:"codes"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		respond(response{Ok: false, Error: "realtime args: " + err.Error()})
		return
	}
	if len(a.Codes) == 0 {
		respond(response{Ok: true, Data: []any{}})
		return
	}

	resp, err := c.GetQuote(a.Codes...)
	// 指数/ETF 等非股票代码需要 DefaultCodes 修正价格, 未初始化时重试一次。
	if err != nil {
		if initErr := ensureCodes(c); initErr != nil {
			respond(response{Ok: false, Error: "realtime: " + err.Error()})
			return
		}
		resp, err = c.GetQuote(a.Codes...)
		if err != nil {
			respond(response{Ok: false, Error: "realtime: " + err.Error()})
			return
		}
	}

	rows := make([]map[string]any, 0, len(resp))
	for _, q := range resp {
		if q.Kline == nil {
			continue
		}
		last := q.Kline.Last.Float64()   // 昨收
		cur := q.Kline.Close.Float64()   // 现价
		pct := 0.0
		if last != 0.0 {
			pct = (cur - last) / last // 小数制(百分数/100)
		}
	// Quote.Exchange + Quote.Code 才是完整代码(如 sh600519); 单用 Code 会丢市场前缀。
		exCode := q.Exchange.String() + q.Code
		rows = append(rows, map[string]any{
			"symbol":     exCode,
			"last_price": cur,
			"prev_close": last,
			"open":       q.Kline.Open.Float64(),
			"high":       q.Kline.High.Float64(),
			"low":        q.Kline.Low.Float64(),
			"volume":     q.Kline.Volume, // 单位: 手
			"amount":     q.Kline.Amount.Float64(),
			"change_pct": pct,
		})
	}
	respond(response{Ok: true, Data: rows})
}

// ---- instruments ----

func opInstruments(c *tdx.Client, args json.RawMessage) {
	var a struct {
		AssetType string `json:"asset_type"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		respond(response{Ok: false, Error: "instruments args: " + err.Error()})
		return
	}
	target := a.AssetType
	exchanges := []protocol.Exchange{protocol.ExchangeSH, protocol.ExchangeSZ, protocol.ExchangeBJ}
	rows := make([]map[string]any, 0, 8192)
	for _, ex := range exchanges {
		resp, err := c.GetCodeAll(ex)
		if err != nil {
			continue // 单个市场失败不影响其它
		}
		for _, v := range resp.List {
			full := ex.String() + v.Code
			switch target {
			case "stock":
				if !protocol.IsStock(full) {
					continue
				}
			case "etf":
				if !protocol.IsETF(full) {
					continue
				}
			case "index":
				if !protocol.IsIndex(full) {
					continue
				}
			default:
				// 仅支持 stock/etf/index
			}
			rows = append(rows, map[string]any{
				"symbol":  full,
				"name":    v.Name,
				"code":    v.Code,
				"exchange": ex.String(),
				"asset_type": target,
			})
		}
	}
	respond(response{Ok: true, Data: rows})
}

// ---- helpers ----

// fetchDayAll/GetKlineDayAll, 指数走 GetIndexDayAll(指数解读含量×100/涨跌家数)。
func fetchDayAll(c *tdx.Client, code string) (*protocol.KlineResp, error) {
	if protocol.IsIndex(code) {
		return c.GetIndexDayAll(code)
	}
	return c.GetKlineDayAll(code)
}

// fetchKlineAll: 按周期取全量; 指数用 GetIndexAll(量/涨跌家数口径), 股票/ETF 用 GetKlineAll。
func fetchKlineAll(c *tdx.Client, typ uint8, code string) (*protocol.KlineResp, error) {
	if protocol.IsIndex(code) {
		return c.GetIndexAll(typ, code)
	}
	return c.GetKlineAll(typ, code)
}

// ensureCodes 初始化 tdx.DefaultCodes(指数/ETF 等非股票行情 GetQuote 需以此修正价格)。
// 仅首次调用有效; 可能较慢(需拉代码表), 故仅当 GetQuote 报错时才触发。
func ensureCodes(c *tdx.Client) error {
	if tdx.DefaultCodes != nil {
		return nil
	}
	codes, err := tdx.NewCodesSqlite(tdx.WithCodesClient(c))
	if err != nil {
		return err
	}
	tdx.DefaultCodes = codes
	return nil
}

// beijingToUTC 把 TDX 分钟时间(北京墙钟)转真实 UTC naive RFC3339: 墙钟 - 8h。
// TDX 库用 time.Local 构造时间, 这里显式按 +08:00 解释墙钟再取 UTC, 避免依赖机器时区。
func beijingToUTC(t time.Time) string {
	bj := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.FixedZone("CST", 8*3600))
	return bj.UTC().Format(time.RFC3339)
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}
