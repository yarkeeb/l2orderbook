package main

import (
	"context"
	"github.com/yarkeeb/l2orderbook/api"
	"github.com/yarkeeb/l2orderbook/pkg/onederx"
	"github.com/yarkeeb/l2orderbook/pkg/rpc"
	"google.golang.org/grpc"
	"log"
	"net"
)

func main() {
	source := onederx.NewSource()
	go source.Start(context.Background())

	service := rpc.NewService()
	service.AddSource(source)
	server := grpc.NewServer()
	api.RegisterOrderBookServer(server, service)

	lsn, err := net.Listen("tcp", "localhost:50051")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("start listening")
	if err := server.Serve(lsn); err != nil {
		log.Fatal(err)
	}
}
