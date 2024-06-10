package entities

import (
	"log"

	pb "if3230-tubes-williamjeffersonclinton/internal/pb/proto"

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

// func (r *RaftClient) SetMatchIndex(index int) {
// 	r.MatchIndex = index
// }

// func (r *RaftClient) SetNextIndex(index int) {
// 	r.NextIndex = index
// }

// func (r *RaftClient) GetMatchIndex() int {
// 	return r.MatchIndex
// }

// func (r *RaftClient) GetNextIndex() int {
// 	return r.NextIndex
// }

// func (r *RaftClient) GetAddress() Address {
// 	return r.Address
// }

// func (r *RaftClient) SetAddress(address Address) {
// 	r.Address = address
// }

// func (r *RaftClient) GetProtoClient() pb.RaftServiceClient {
// 	return r.ProtoClient
// }

// func (r *RaftClient) SetProtoClient(client pb.RaftServiceClient) {
// 	r.ProtoClient = client
// }
