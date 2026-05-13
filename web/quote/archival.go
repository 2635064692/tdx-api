package quote

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"xorm.io/xorm"
)

type ArchivalTask struct {
	db           *xorm.Engine
	now          func() time.Time
	progressFunc func(current, total int, message string)
}

type archivalPayload struct {
	Code       string            `json:"code"`
	Exchange   string            `json:"exchange"`
	TotalHand  int64             `json:"totalHand"`
	Amount     float64           `json:"amount"`
	InsideDish int               `json:"insideDish"`
	OuterDisc  int               `json:"outerDisc"`
	Ts         int64             `json:"ts"`
	Minutes    []minutePayload   `json:"minutes"`
}

type minutePayload struct {
	Ts        int64             `json:"ts"`
	BuyLevel  []compressedLevel `json:"buyLevel"`
	SellLevel []compressedLevel `json:"sellLevel"`
}

type compressedLevel struct {
	Ts     int64 `json:"ts"`
	Price  int   `json:"price"`
	Number []int `json:"number"`
}

const (
	sameNumTag = -120
	gapNumTag  = 0
)

type priceLevel struct {
	Price  int
	Number int
}

type levelAgg struct {
	FirstTs      int64
	LastSecondTs int64
	Numbers      []int
}

func NewArchivalTask(db *xorm.Engine) *ArchivalTask {
	return &ArchivalTask{db: db, now: time.Now}
}

func (a *ArchivalTask) SetProgressFunc(fn func(current, total int, message string)) {
	a.progressFunc = fn
}

func (a *ArchivalTask) reportProgress(current, total int, message string) {
	if a.progressFunc != nil {
		a.progressFunc(current, total, message)
	}
}

func (a *ArchivalTask) Run(ctx context.Context, tradeDate time.Time) error {
	if tradeDate.IsZero() {
		tradeDate = a.now()
	}
	return a.archiveDate(ctx, normalizeTradeDate(tradeDate))
}

func (a *ArchivalTask) archiveDate(ctx context.Context, tradeDate time.Time) error {
	codes, err := a.loadCodesByDate(tradeDate)
	if err != nil {
		return err
	}
	if len(codes) == 0 {
		return nil
	}
	totalCodes := len(codes)
	a.reportProgress(0, totalCodes, "开始归档")

	var histories []QuoteTickHistory
	for i, code := range codes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		a.reportProgress(i, totalCodes, "处理股票: "+code)

		rows, err := a.loadRealtimeRowsByCode(tradeDate, code)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			continue
		}
		history, err := aggregateQuoteTicks(rows)
		if err != nil {
			return err
		}
		histories = append(histories, history)
		if len(histories) >= 100 {
			if err := a.writeHistory(histories); err != nil {
				return err
			}
			histories = histories[:0]
		}
	}
	if len(histories) > 0 {
		if err := a.writeHistory(histories); err != nil {
			return err
		}
	}
	a.reportProgress(totalCodes, totalCodes, "验证归档结果")

	if err := a.verifyArchival(tradeDate, len(codes)); err != nil {
		return err
	}

	a.reportProgress(totalCodes, totalCodes, "清理实时数据")
	return a.cleanupRealtime(tradeDate)
}

func (a *ArchivalTask) loadCodesByDate(tradeDate time.Time) ([]string, error) {
	if a.db == nil {
		return nil, nil
	}
	var codes []string
	err := a.db.Table(QuoteTickRealtime{}.TableName()).
		Where("DATE(trade_date) = ?", tradeDateKey(tradeDate)).
		Distinct("code").
		Find(&codes)
	return codes, err
}

func (a *ArchivalTask) loadRealtimeRowsByCode(tradeDate time.Time, code string) ([]QuoteTickRealtime, error) {
	if a.db == nil {
		return nil, nil
	}
	rows := make([]QuoteTickRealtime, 0)
	err := a.db.Table(QuoteTickRealtime{}.TableName()).
		Where("DATE(trade_date) = ? AND code = ?", tradeDateKey(tradeDate), code).
		Asc("event_ts", "seq").
		Find(&rows)
	return rows, err
}

func aggregateQuoteTicks(rows []QuoteTickRealtime) (QuoteTickHistory, error) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].EventTs == rows[j].EventTs {
			return rows[i].Seq < rows[j].Seq
		}
		return rows[i].EventTs < rows[j].EventTs
	})

	minutes := make([]minutePayload, 0)
	var currentMinuteStart int64 = -1
	var minuteBuyAgg map[int]*levelAgg
	var minuteSellAgg map[int]*levelAgg

	flushMinute := func() {
		if currentMinuteStart < 0 {
			return
		}
		minutes = append(minutes, minutePayload{
			Ts:        currentMinuteStart,
			BuyLevel:  toCompressedLevels(minuteBuyAgg, true),
			SellLevel: toCompressedLevels(minuteSellAgg, false),
		})
	}

	for i := range rows {
		minuteStart := (rows[i].EventTs / 60_000) * 60_000
		if minuteStart != currentMinuteStart {
			flushMinute()
			currentMinuteStart = minuteStart
			minuteBuyAgg = make(map[int]*levelAgg)
			minuteSellAgg = make(map[int]*levelAgg)
		}
		mergeLevels(minuteBuyAgg, rowLevels(rows[i], true), rows[i].EventTs)
		mergeLevels(minuteSellAgg, rowLevels(rows[i], false), rows[i].EventTs)
	}
	flushMinute()

	payload := archivalPayload{
		Code:      rows[0].Code,
		Exchange:  rows[0].Exchange,
		Ts:        rows[0].EventTs,
		Minutes:   minutes,
	}
	last := rows[len(rows)-1]
	payload.TotalHand = last.Volume
	payload.Amount = last.Amount
	payload.InsideDish = last.InsideVol
	payload.OuterDisc = last.OutsideVol
	body, err := json.Marshal(payload)
	if err != nil {
		return QuoteTickHistory{}, err
	}
	return QuoteTickHistory{
		Code:       rows[0].Code,
		Exchange:   rows[0].Exchange,
		TradeDate:  rows[0].TradeDate,
		Volume:     last.Volume,
		Amount:     last.Amount,
		InsideVol:  last.InsideVol,
		OutsideVol: last.OutsideVol,
		QuoteTicks: string(body),
	}, nil
}

// compressLevels aggregates tick data into second-level compressed format.
// Assumes rows are sorted by EventTs (and Seq) in ascending order.
func compressLevels(rows []QuoteTickRealtime, buy bool) []compressedLevel {
	aggByPrice := make(map[int]*levelAgg)

	for i := range rows {
		mergeLevels(aggByPrice, rowLevels(rows[i], buy), rows[i].EventTs)
	}

	return toCompressedLevels(aggByPrice, buy)
}

func rowLevels(row QuoteTickRealtime, buy bool) []priceLevel {
	levels := make([]priceLevel, 0, 5)
	for idx := 0; idx < 5; idx++ {
		price, vol := levelValues(row, idx, buy)
		if price == 0 {
			continue
		}
		levels = append(levels, priceLevel{Price: price, Number: vol})
	}
	return levels
}

func mergeLevels(out map[int]*levelAgg, levels []priceLevel, tickTs int64) {
	if out == nil {
		return
	}
	secondTs := (tickTs / 1000) * 1000
	levelMap := make(map[int]int, len(levels))
	for _, level := range levels {
		levelMap[level.Price] = level.Number
	}
	for price, agg := range out {
		if _, ok := levelMap[price]; ok || secondTs <= agg.LastSecondTs {
			continue
		}
		baseTs := agg.LastSecondTs
		gapSeconds := int((secondTs-baseTs)/1000 - 1)
		for g := 0; g < gapSeconds; g++ {
			agg.appendGapNum(baseTs + int64(g+1)*1000)
		}
	}
	for _, level := range levels {
		agg := out[level.Price]
		if agg == nil {
			out[level.Price] = newLevelAgg(secondTs, level.Number)
			continue
		}
		agg.appendNum(level.Number, secondTs)
	}
}

func toCompressedLevels(aggByPrice map[int]*levelAgg, buy bool) []compressedLevel {
	if len(aggByPrice) == 0 {
		return nil
	}
	prices := make([]int, 0, len(aggByPrice))
	for price := range aggByPrice {
		prices = append(prices, price)
	}
	sort.Ints(prices)
	if buy {
		sort.Sort(sort.Reverse(sort.IntSlice(prices)))
	}
	levels := make([]compressedLevel, 0, len(prices))
	for _, price := range prices {
		agg := aggByPrice[price]
		if agg == nil {
			continue
		}
		levels = append(levels, compressedLevel{
			Ts:     agg.FirstTs,
			Price:  price,
			Number: append([]int(nil), agg.Numbers...),
		})
	}
	return levels
}

func newLevelAgg(secondTs int64, num int) *levelAgg {
	return &levelAgg{
		FirstTs:      secondTs,
		LastSecondTs: secondTs,
		Numbers:      []int{num},
	}
}

func (l *levelAgg) findLastValidNum() (int, bool) {
	for i := len(l.Numbers) - 1; i >= 0; i-- {
		if l.Numbers[i] >= 0 {
			return l.Numbers[i], true
		}
	}
	return 0, false
}

func (l *levelAgg) appendNum(num int, secondTs int64) {
	lastNum := l.Numbers[len(l.Numbers)-1]
	lastValidNum, ok := l.findLastValidNum()
	switch {
	case lastNum > 0:
		if lastNum == num {
			l.Numbers = append(l.Numbers, sameNumTag+1)
		} else {
			l.Numbers = append(l.Numbers, num)
		}
	case ok && num == lastValidNum:
		if lastNum < -60 {
			l.Numbers[len(l.Numbers)-1] = lastNum + 1
		} else {
			l.Numbers = append(l.Numbers, sameNumTag+1)
		}
	default:
		l.Numbers = append(l.Numbers, num)
	}
	if secondTs > l.LastSecondTs {
		l.LastSecondTs = secondTs
	}
}

func (l *levelAgg) appendGapNum(secondTs int64) {
	lastNum := l.Numbers[len(l.Numbers)-1]
	if lastNum > 0 {
		l.Numbers = append(l.Numbers, gapNumTag-1)
	} else if lastNum > -60 {
		l.Numbers[len(l.Numbers)-1] = lastNum - 1
	} else {
		l.Numbers = append(l.Numbers, gapNumTag-1)
	}
	l.LastSecondTs = secondTs
}

func levelValues(row QuoteTickRealtime, idx int, buy bool) (int, int) {
	if buy {
		switch idx {
		case 0:
			return row.Bid1Price, row.Bid1Vol
		case 1:
			return row.Bid2Price, row.Bid2Vol
		case 2:
			return row.Bid3Price, row.Bid3Vol
		case 3:
			return row.Bid4Price, row.Bid4Vol
		default:
			return row.Bid5Price, row.Bid5Vol
		}
	}
	switch idx {
	case 0:
		return row.Ask1Price, row.Ask1Vol
	case 1:
		return row.Ask2Price, row.Ask2Vol
	case 2:
		return row.Ask3Price, row.Ask3Vol
	case 3:
		return row.Ask4Price, row.Ask4Vol
	default:
		return row.Ask5Price, row.Ask5Vol
	}
}

func (a *ArchivalTask) writeHistory(histories []QuoteTickHistory) error {
	return NewSession(a.db, func(session *xorm.Session) error {
		for _, history := range histories {
			_, err := session.Table(history.TableName()).Insert(&history)
			if err != nil && !isDuplicateErr(err) {
				return err
			}
		}
		return nil
	})
}

func (a *ArchivalTask) verifyArchival(tradeDate time.Time, expected int) error {
	count, err := a.db.Table(QuoteTickHistory{}.TableName()).Where("DATE(trade_date) = ?", tradeDateKey(tradeDate)).Count()
	if err != nil {
		return err
	}
	if int(count) < expected {
		return xorm.ErrNotExist
	}
	return nil
}

func (a *ArchivalTask) cleanupRealtime(tradeDate time.Time) error {
	if a.db != nil && a.db.DriverName() == "sqlite" {
		_, err := a.db.Exec(
			"DELETE FROM "+QuoteTickRealtime{}.TableName()+" WHERE DATE(trade_date) = ?",
			tradeDateKey(tradeDate),
		)
		return err
	}

	const batchSize = 10000
	for {
		result, err := a.db.Exec(
			"DELETE FROM "+QuoteTickRealtime{}.TableName()+" WHERE DATE(trade_date) = ? LIMIT ?",
			tradeDateKey(tradeDate), batchSize,
		)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			break
		}
	}
	return nil
}

func normalizeTradeDate(tradeDate time.Time) time.Time {
	return time.Date(tradeDate.Year(), tradeDate.Month(), tradeDate.Day(), 0, 0, 0, 0, time.UTC)
}

func tradeDateKey(tradeDate time.Time) string {
	return normalizeTradeDate(tradeDate).Format("2006-01-02")
}
