package main

import (
	"context"
	"log"
	"net"
	"sync"
	"learn-go/local/keyvaluestore"
	"google.golang.org/grpc"
)

type server struct {
	keyvaluestore.UnimplementedKeyValueStoreServer
	data sync.Map
}

func (s *server) Set(ctx context.Context, req *keyvaluestore.SetRequest) (*keyvaluestore.SetResponse, error) {
	s.data.Store(req.Key, req.Value)
	return &keyvaluestore.SetResponse{Status: "Success"}, nil
}

func (s *server) Get(ctx context.Context, req *keyvaluestore.GetRequest) (*keyvaluestore.GetResponse, error) {
	if value, ok := s.data.Load(req.Key); ok {
		return &keyvaluestore.GetResponse{Value: value.(string)}, nil
	}
	return &keyvaluestore.GetResponse{Value: ""}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	keyvaluestore.RegisterKeyValueStoreServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
