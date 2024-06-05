package entities

import (
	pb "if3230-tubes-williamjeffersonclinton/internal/pb/proto"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftClient struct {
	Address     Address
	NextIndex   int
	MatchIndex  int
	ProtoClient pb.RaftServiceClient
}

func NewRaftClient(address Address) *RaftClient {
	conn, err := grpc.NewClient(address.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return &RaftClient{
		Address:     address,
		ProtoClient: pb.NewRaftServiceClient(conn),
		NextIndex:   0,
		MatchIndex:  0,
	}
}
