package rpc

import (
	"github.com/yarkeeb/l2orderbook/api"
	"github.com/yarkeeb/l2orderbook/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"time"
)

type Service struct {
	sources []types.Source
}

func NewService() *Service {
	return &Service{
		sources: make([]types.Source, 0),
	}
}

func (s *Service) AddSource(source types.Source) {
	s.sources = append(s.sources, source)
}

func (s *Service) GetL2OrderBook(req *api.L2OrderBookRequest, stream api.OrderBook_GetL2OrderBookServer) error {
	log.Printf("client connected")
	if req.Size <= 0 {
		return status.Error(codes.InvalidArgument, "invalid size")
	}
	if req.Interval <= 0 {
		return status.Error(codes.InvalidArgument, "invalid interval")
	}
	var (
		stop bool
		l2   *types.L2OrderBook
		err  error
	)
	for !stop {
		select {
		case <-stream.Context().Done():
			stop = true
			break
		case <-time.After(time.Duration(req.Interval) * time.Millisecond):
			for _, source := range s.sources {
				l2, err = source.GetL2OrderBook(req.Symbol, req.Size)
				if err != nil {
					stop = true
					break
				}
				if err = stream.Send(ConvertToProtocolL2(req.Symbol, *l2)); err != nil {
					stop = true
					break
				}

			}

		}
	}

	log.Printf("client disconnected")
	return err
}
