package handlers

import (
	"if3230-tubes-williamjeffersonclinton/internal/domain/services"
	pb "if3230-tubes-williamjeffersonclinton/internal/pb/proto"
)

type GrpcHandler struct {
	pb.UnimplementedRaftServiceServer
	pb.UnimplementedClientServiceServer

	raftService   *services.RaftService
	clientService *services.ClientService
}

func NewGrpcHandler(raftService *services.RaftService, clientService *services.ClientService) *GrpcHandler {
	return &GrpcHandler{
		raftService:   raftService,
		clientService: clientService,
	}
}
