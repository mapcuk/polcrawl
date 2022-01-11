package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"log"
	"strconv"
	"strings"
	"time"
)

type Conf struct {
	Poloniex []string `json:"poloniex"`
}

var input = []byte(`{
  "poloniex": [
    "BTC_USDT",
    "TRX_USDT",
    "ETH_USDT"
  ]
}`)

func main() {
	conf := Conf{}
	err := json.Unmarshal(input, &conf)
	if err != nil {
		log.Fatalf("error during unmarshal %v", err)
	}

	err = fetchAndOutput(conf)
	if err != nil {
		log.Print(err.Error())
	}
}

func fetchAndOutput(cfg Conf) error {
	c, _, err := websocket.DefaultDialer.Dial("wss://api2.poloniex.com", nil)
	if err != nil {
		log.Println(err)
		return err
	}
	defer c.Close()

	for _, pair := range cfg.Poloniex {
		fetchOneCurPair(c, pair)
	}
	return nil
}

func fetchOneCurPair(conn *websocket.Conn, pair string) error {
	pairs := strings.Split(pair, "_")
	if len(pairs) != 2 {
		return errors.New(fmt.Sprintf("incorrect currency pair %s", pair))
	}
	pairs[0], pairs[1] = pairs[1], pairs[0]
	channel := strings.Join(pairs, "_")
	subscribeCmd := []byte(fmt.Sprintf(`{ "command": "subscribe", "channel": "%s" }`, channel))

	err := conn.WriteMessage(websocket.TextMessage, subscribeCmd)

	if err != nil {
		return errors.New(fmt.Sprintf("error during sending message %s", subscribeCmd))
	}

	// NOTICE: task doesn't contain clear condition to stop monitoring
	// so made up simple timeout
	var timeOut = 3 * time.Second
	ctx, _ := context.WithTimeout(context.Background(), timeOut)
	return fetcher(conn, ctx)
}

/*
Response

Content
{
	"currencyPair": "<currency pair name>",
	"orderBook": [
		{ "<lowest ask price>": "<lowest ask size>", "<next ask price>": "<next ask size>", ... },
		{ "<highest bid price>": "<highest bid size>", "<next bid price>": "<next bid size>", ... }
	]
}

[ <channel id>, <sequence number>,
	[
		[ "i", Content, "<epoch_ms>" ]
	]
]

[ <channel id>, <sequence number>,
	[
		["o", <1 for bid 0 for ask>, "<price>", "<size>", "<epoch_ms>"],
		["o", <1 for bid 0 for ask>, "<price>", "<size>", "<epoch_ms>"],
		["t", "<trade id>", <1 for buy 0 for sell>, "<price>", "<size>", <timestamp>, "<epoch_ms>"]
	]
]
*/

type Order map[string]string

type Content struct {
	CurrencyPair string  `json:"currencyPair"`
	OrderBook    []Order `json:"orderBook"`
}

type RecentTrade struct {
	Id        string    // ID транзакции
	Pair      string    // Торговая пара (из списка выше)
	Price     float64   // Цена транзакции
	Amount    float64   // Объём транзакции
	Side      string    // Как биржа засчитала эту сделку (как buy или как sell)
	Timestamp time.Time // Время транзакции
}

func parseMsg(rawMessage []byte) error {
	var channelID int
	var seq int64
	var specificContent [][]json.RawMessage
	var arr = []interface{}{&channelID, &seq, &specificContent}
	err := json.Unmarshal(rawMessage, &arr)
	if err != nil {
		return err
	}

	for _, v := range specificContent {
		var msgType string
		err = json.Unmarshal(v[0], &msgType)
		if err != nil {
			log.Printf("can not unmarshal %s\n", v[0])
		}

		// TODO: no idea how to map to RecentTrade
		switch msgType {
		case "i":
			content := Content{}
			err = json.Unmarshal(v[1], &content)
			if err != nil {
				log.Println(err)
				continue
			}
			log.Println(content)
		case "o":
			tradeType := string(v[1])
			price := jsByteToStr(v[2])
			size := jsByteToStr(v[3])
			epochMs := jsByteToStr(v[4])
			log.Println(tradeType, price, size, epochMs)
		case "t":
			tradeId := byteToInt(v[1])
			tradeType := string(v[2])
			price := jsByteToStr(v[3])
			size := jsByteToStr(v[4])
			timestamp := string(v[5])
			epochMs := jsByteToStr(v[6])
			log.Println(tradeId, tradeType, price, size, timestamp, epochMs)
		default:
			log.Printf("unknown type %s\n", v[0])
		}
	}
	return nil
}

func jsByteToStr(val []byte) string {
	if len(val) == 0 {
		return ""
	}
	first := 0
	last := len(val) - 1
	if val[0] == '"' {
		first = 1
	}
	if val[last] == '"' {
		last = last - 1
	}
	return string(val[first:last])
}

func byteToInt(val []byte) int {
	res, _ := strconv.Atoi(jsByteToStr(val))
	return res
}

func fetcher(conn *websocket.Conn, ctx context.Context) error {
	for {
		_, rawMessage, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		err = parseMsg(rawMessage)
		if err != nil {
			log.Printf("parse error: %s", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
