package handlers

import (
	pb "github.com/melvinkj/raft-consensus/internal/pb/proto"
	"github.com/melvinkj/raft-consensus/internal/domain/services"
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

