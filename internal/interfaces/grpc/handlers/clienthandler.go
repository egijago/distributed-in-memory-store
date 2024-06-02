package handlers

import (
	"context"

	dtos "github.com/melvinkj/raft-consensus/internal/domain/dtos"
	pb "github.com/melvinkj/raft-consensus/internal/pb/proto"
)

func (s *GrpcHandler) Ping(ctx context.Context, req *pb.ClientPingRequest) (*pb.ClientPingResponse, error) {
	return &pb.ClientPingResponse{
		Message: "PONG",
	}, nil
}

func (s *GrpcHandler) Get(ctx context.Context, req *pb.ClientGetRequest) (*pb.ClientGetResponse, error) {
	request := dtos.ClientGetRequest{
		Key: req.GetKey(),
	}

	response := s.clientService.Get(request)
	return &pb.ClientGetResponse{Value: response.Value}, nil
}

func (s *GrpcHandler) Set(ctx context.Context, req *pb.ClientSetRequest) (*pb.ClientSetResponse, error) {
	request := dtos.ClientSetRequest{
		Key:   req.GetKey(),
		Value: req.GetValue(),
	}

	response := s.clientService.Set(request)

	var message string
	if response.Success {
		message = "Success"
	} else {
		message = "Failed"
	}
	return &pb.ClientSetResponse{
		Message: message,
	}, nil
}

func (s *GrpcHandler) Strln(ctx context.Context, req *pb.ClientStrlnRequest) (*pb.ClientStrlnResponse, error) {
	request := dtos.ClientStrlnRequest{
		Key: req.GetKey(),
	}

	response := s.clientService.Strln(request)
	return &pb.ClientStrlnResponse{Length: int32(response.Length)}, nil
}

func (s *GrpcHandler) Del(ctx context.Context, req *pb.ClientDelRequest) (*pb.ClientDelResponse, error) {
	request := dtos.ClientDeleteRequest{
		Key: req.GetKey(),
	}

	response := s.clientService.Delete(request)
	return &pb.ClientDelResponse{Value: response.Value}, nil
}

func (s *GrpcHandler) Append(ctx context.Context, req *pb.ClientAppendRequest) (*pb.ClientAppendResponse, error) {
	request := dtos.ClientAppendRequest{
		Key:   req.GetKey(),
		Value: req.GetValue(),
	}

	response := s.clientService.Append(request)
	return &pb.ClientAppendResponse{Value: response.Value}, nil
}
