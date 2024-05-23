package internal

import (
	"context"
	keyvaluestore "if3230-tubes-williamjeffersonclinton/pkg/proto/auto-generated"
	"sync"

	"google.golang.org/grpc"
)

type server struct {
	keyvaluestore.UnimplementedKeyValueStoreServer
	data sync.Map
}

type ServerInterface interface {
	Set(ctx context.Context, req *keyvaluestore.SetRequest) (*keyvaluestore.SetResponse, error)
	Get(ctx context.Context, req *keyvaluestore.GetRequest) (*keyvaluestore.GetResponse, error)
}

func New(serv *grpc.Server) {
	keyvaluestore.RegisterKeyValueStoreServer(serv, &server{})
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
