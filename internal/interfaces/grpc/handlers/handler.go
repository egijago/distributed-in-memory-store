package handlers

import (
	pb "if3230-tubes-williamjeffersonclinton/internal/pb/proto"
	"if3230-tubes-williamjeffersonclinton/internal/domain/services"
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

