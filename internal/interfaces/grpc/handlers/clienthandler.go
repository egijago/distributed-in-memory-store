package handlers

import (
	dtos "if3230-tubes-williamjeffersonclinton/internal/domain/dtos"
	pb "if3230-tubes-williamjeffersonclinton/internal/pb/proto"

	"context"
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
	return &pb.ClientGetResponse{Value: response.Value, Success: response.Success, LeaderPort: response.LeaderPort}, nil
}

func (s *GrpcHandler) Set(ctx context.Context, req *pb.ClientSetRequest) (*pb.ClientSetResponse, error) {
	request := dtos.ClientSetRequest{
		Key:   req.GetKey(),
		Value: req.GetValue(),
	}

	response := s.clientService.Set(request)
	return &pb.ClientSetResponse{
		Success:    response.Success,
		LeaderPort: response.LeaderPort,
	}, nil
}

func (s *GrpcHandler) Strln(ctx context.Context, req *pb.ClientStrlnRequest) (*pb.ClientStrlnResponse, error) {
	request := dtos.ClientStrlnRequest{
		Key: req.GetKey(),
	}

	response := s.clientService.Strln(request)
	return &pb.ClientStrlnResponse{Length: int32(response.Length), Success: response.Success, LeaderPort: response.LeaderPort}, nil
}

func (s *GrpcHandler) Del(ctx context.Context, req *pb.ClientDelRequest) (*pb.ClientDelResponse, error) {
	request := dtos.ClientDeleteRequest{
		Key: req.GetKey(),
	}

	response := s.clientService.Delete(request)
	return &pb.ClientDelResponse{Value: response.Value, Success: response.Success, LeaderPort: response.LeaderPort}, nil
}

func (s *GrpcHandler) Append(ctx context.Context, req *pb.ClientAppendRequest) (*pb.ClientAppendResponse, error) {
	request := dtos.ClientAppendRequest{
		Key:   req.GetKey(),
		Value: req.GetValue(),
	}

	response := s.clientService.Append(request)
	return &pb.ClientAppendResponse{Value: response.Value, Success: response.Success, LeaderPort: response.LeaderPort}, nil
}

func (s *GrpcHandler) ReqLog(ctx context.Context, req *pb.ClientRequestLogRequest) (*pb.ClientRequestLogResponse, error) {
	response := s.clientService.ReqLog()
	return &pb.ClientRequestLogResponse{Value: response.Value, Success: response.Success, LeaderPort: response.LeaderPort}, nil
}