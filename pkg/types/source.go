package types

type Source interface {
	GetL2OrderBook(symbol string, size uint32) (*L2OrderBook, error)
}
