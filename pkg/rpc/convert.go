package rpc

import (
	"github.com/golang/protobuf/ptypes"
	"github.com/yarkeeb/l2orderbook/api"
	"github.com/yarkeeb/l2orderbook/pkg/types"
)

func ConvertToProtocolL2(symbol string, l2 types.L2OrderBook) *api.L2OrderBook {
	convertItem := func(item *types.L2OrderBookItem) *api.L2OrderBookItem {
		return &api.L2OrderBookItem{
			Price:  item.Price.String(),
			Volume: item.Volume,
		}
	}

	ret := &api.L2OrderBook{
		Symbol: symbol,
		Time:   ptypes.TimestampNow(),
		Bid:    make([]*api.L2OrderBookItem, 0, len(l2.Bid)),
		Ack:    make([]*api.L2OrderBookItem, 0, len(l2.Ask)),
	}

	for _, item := range l2.Bid {
		ret.Bid = append(ret.Bid, convertItem(item))
	}
	for _, item := range l2.Ask {
		ret.Ack = append(ret.Ack, convertItem(item))
	}
	return ret
}
