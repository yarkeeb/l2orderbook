package onederx

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/yarkeeb/l2orderbook/pkg/types"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	onederxWsUrl = "wss://api.onederx.com/v1/ws"

	defaultRetryInterval = time.Second
	defaultReadTimeout   = time.Second * 5
	defaultWriteTimeout  = time.Second * 5
)

type Source struct {
	sync.RWMutex
	l2BySymbol map[string]*L2OrderBook
}

func NewSource() *Source {
	return &Source{
		l2BySymbol: make(map[string]*L2OrderBook),
	}
}

func (s *Source) GetL2OrderBook(symbol string, size uint32) (*types.L2OrderBook, error) {
	s.RLock()
	defer s.RUnlock()
	l2, ok := s.l2BySymbol[symbol]
	if !ok {
		return nil, fmt.Errorf("no data for %s", symbol)
	}
	return &types.L2OrderBook{
		Bid: l2.GetBid(int(size)),
		Ask: l2.GetAsk(int(size)),
	}, nil
}

func (s *Source) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("source stopped: context cancelled")
			return
		default:
			if err := s.receiveData(ctx); err != nil {
				log.Printf("receiving failed: %v", err)
			}
			log.Printf("sleep for %v ", defaultRetryInterval)
			time.Sleep(defaultRetryInterval)

		}
	}
}

func (s *Source) receiveData(ctx context.Context) error {
	dialer := websocket.Dialer{}
	conn, _, err := dialer.DialContext(ctx, onederxWsUrl, nil)
	if err != nil {
		return err
	}
	if err := conn.SetWriteDeadline(time.Now().Add(defaultWriteTimeout)); err != nil {
		return err
	}
	if err := conn.WriteJSON(GetWsL2SubscribeRequest()); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msgType, data, err := conn.ReadMessage()
			if err != nil {
				return err
			}
			if msgType != websocket.TextMessage {
				return fmt.Errorf("unexpected meessage type")
			}
			header := struct {
				Type string
			}{}
			if err := json.Unmarshal(data, &header); err != nil {
				return err
			}
			switch header.Type {
			case "snapshot":
				err = s.onSnapshot(data)
			case "update":
				err = s.onUpdate(data)
			default:
			}
			if err != nil {
				return err
			}
		}

	}
}

func (s *Source) onSnapshot(data []byte) error {
	s.Lock()
	defer s.Unlock()
	var wsSnapshot WsL2Snapshot
	if err := json.Unmarshal(data, &wsSnapshot); err != nil {
		return err
	}

	l2 := NewL2OrderBook()
	s.l2BySymbol[wsSnapshot.Params.Symbol] = l2

	for _, updates := range [][]*WsL2Item{
		wsSnapshot.Payload.Snapshot,
		wsSnapshot.Payload.Updates} {
		for _, item := range updates {
			side := types.SideFromString(item.Side)
			tm := time.Unix(0, item.Timestamp)
			l2.Apply(item.Price, side, item.Volume, tm)
		}
	}
	log.Printf("snapshot applied: bid=%d, ask=%d", l2.bid.Len(), l2.ask.Len())
	return nil
}

func (s *Source) onUpdate(data []byte) error {
	s.Lock()
	defer s.Unlock()
	var update WsL2Update
	if err := json.Unmarshal(data, &update); err != nil {
		return err
	}
	l2, ok := s.l2BySymbol[update.Params.Symbol]
	if !ok {
		log.Printf("inconsistent update for %s", update.Params.Symbol)
		return nil
	}

	side := types.SideFromString(update.Payload.Side)
	tm := time.Unix(0, update.Payload.Timestamp)
	l2.Apply(update.Payload.Price, side, update.Payload.Volume, tm)
	log.Printf("update applied: bid=%d, ask=%d", l2.bid.Len(), l2.ask.Len())
	return nil
}
