package quote

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"xorm.io/xorm"
)

type ArchivalTask struct {
	db  *xorm.Engine
	now func() time.Time
}

type archivalPayload struct {
	Code       string            `json:"code"`
	Exchange   string            `json:"exchange"`
	TotalHand  int64             `json:"totalHand"`
	Amount     float64           `json:"amount"`
	InsideDish int               `json:"insideDish"`
	OuterDisc  int               `json:"outerDisc"`
	Ts         int64             `json:"ts"`
	BuyLevel   []compressedLevel `json:"buyLevel"`
	SellLevel  []compressedLevel `json:"sellLevel"`
}

type compressedLevel struct {
	Ts     int64 `json:"ts"`
	Price  int   `json:"price"`
	Number []int `json:"number"`
}

func NewArchivalTask(db *xorm.Engine) *ArchivalTask {
	return &ArchivalTask{db: db, now: time.Now}
}

func (a *ArchivalTask) Run(ctx context.Context, tradeDate time.Time) error {
	if tradeDate.IsZero() {
		tradeDate = a.now()
	}
	return a.archiveDate(ctx, normalizeTradeDate(tradeDate))
}

func (a *ArchivalTask) archiveDate(ctx context.Context, tradeDate time.Time) error {
	rows, err := a.loadRealtimeRows(tradeDate)
	if err != nil || len(rows) == 0 {
		return err
	}
	histories, err := a.aggregateRows(rows)
	if err != nil {
		return err
	}
	if err := a.writeHistory(histories); err != nil {
		return err
	}
	if err := a.verifyArchival(tradeDate, len(histories)); err != nil {
		return err
	}
	return a.cleanupRealtime(tradeDate)
}

func (a *ArchivalTask) loadRealtimeRows(tradeDate time.Time) ([]QuoteTickRealtime, error) {
	if a.db == nil {
		return nil, nil
	}
	rows := make([]QuoteTickRealtime, 0)
	err := a.db.Table(QuoteTickRealtime{}.TableName()).Where("DATE(trade_date) = ?", tradeDateKey(tradeDate)).Asc("code", "event_ts", "seq").Find(&rows)
	return rows, err
}

func (a *ArchivalTask) aggregateRows(rows []QuoteTickRealtime) ([]QuoteTickHistory, error) {
	groups := make(map[string][]QuoteTickRealtime)
	order := make([]string, 0)
	for _, row := range rows {
		if _, ok := groups[row.Code]; !ok {
			order = append(order, row.Code)
		}
		groups[row.Code] = append(groups[row.Code], row)
	}
	histories := make([]QuoteTickHistory, 0, len(groups))
	for _, code := range order {
		history, err := aggregateQuoteTicks(groups[code])
		if err != nil {
			return nil, err
		}
		histories = append(histories, history)
	}
	return histories, nil
}

func aggregateQuoteTicks(rows []QuoteTickRealtime) (QuoteTickHistory, error) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].EventTs == rows[j].EventTs {
			return rows[i].Seq < rows[j].Seq
		}
		return rows[i].EventTs < rows[j].EventTs
	})
	payload := archivalPayload{
		Code:      rows[0].Code,
		Exchange:  rows[0].Exchange,
		Ts:        rows[0].EventTs,
		BuyLevel:  compressLevels(rows, true),
		SellLevel: compressLevels(rows, false),
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

func compressLevels(rows []QuoteTickRealtime, buy bool) []compressedLevel {
	levels := make([]compressedLevel, 0)
	for idx := 0; idx < 5; idx++ {
		levels = append(levels, compressLevel(rows, idx, buy)...)
	}
	return levels
}

func compressLevel(rows []QuoteTickRealtime, idx int, buy bool) []compressedLevel {
	series := make([]compressedLevel, 0)
	lastPrice := 0
	lastVol := 0
	initialized := false
	for _, row := range rows {
		price, vol := levelValues(row, idx, buy)
		if !initialized || price != lastPrice {
			series = append(series, compressedLevel{Ts: row.EventTs, Price: price, Number: []int{vol}})
			lastPrice = price
			lastVol = vol
			initialized = true
			continue
		}
		delta := vol - lastVol
		series[len(series)-1].Number = append(series[len(series)-1].Number, delta)
		lastVol = vol
	}
	return series
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
	_, err := a.db.Table(QuoteTickRealtime{}.TableName()).Where("DATE(trade_date) = ?", tradeDateKey(tradeDate)).Delete(&QuoteTickRealtime{})
	return err
}

func normalizeTradeDate(tradeDate time.Time) time.Time {
	return time.Date(tradeDate.Year(), tradeDate.Month(), tradeDate.Day(), 0, 0, 0, 0, time.UTC)
}

func tradeDateKey(tradeDate time.Time) string {
	return normalizeTradeDate(tradeDate).Format("2006-01-02")
}
