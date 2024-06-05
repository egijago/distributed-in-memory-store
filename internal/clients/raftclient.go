package clients

import (
	"if3230-tubes-williamjeffersonclinton/internal/domain/dtos"
	"if3230-tubes-williamjeffersonclinton/internal/domain/entities"
	pb "if3230-tubes-williamjeffersonclinton/internal/pb/proto"

	"context"
	// "log"
	// "google.golang.org/grpc"
	// "google.golang.org/grpc/credentials/insecure"
)

// func NewRaftClientCaller(address entities.Address) *raftClientCaller {
// 	conn, err := grpc.NewClient(address.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
// 	if err != nil {
// 		log.Fatalf("did not connect: %v", err)
// 	}
// 	return &raftClientCaller{
// 		raftClient: entities.RaftClient{
// 			Address:     address,
// 			ProtoClient: pb.NewRaftServiceClient(conn),
// 		},
// 	}
// }


type RaftClientCallerImpl struct{}

type RaftClientCaller interface {
	AppendEntries(client entities.RaftClient, request dtos.RaftAppendEntriesRequest) (*dtos.RaftAppendEntriesResponse, error)
	RequestVote(client entities.RaftClient, request dtos.RaftRequestVoteRequest) (*dtos.RaftRequestVoteResponse, error)
	ApplyMembership(client entities.RaftClient, request dtos.RaftApplyMembershipRequest) (*dtos.RaftApplyMembershipResponse, error)
}

func (*RaftClientCallerImpl) AppendEntries(client entities.RaftClient, request dtos.RaftAppendEntriesRequest) (*dtos.RaftAppendEntriesResponse, error) {
	pbEntries := []*pb.Log{}
	for _, entry := range request.Entries {
		pbEntries = append(pbEntries, &pb.Log{
			Index:   int32(entry.Index),
			Command: entry.Command,
			Term:    int32(entry.Term),
		})
	}

	response, err := client.ProtoClient.AppendEntries(context.Background(), &pb.RaftAppendEntriesRequest{
		Term:         int32(request.Term),
		LeaderId:     int32(request.LeaderId),
		PrevLogIndex: int32(request.PrevLogIndex),
		PrevLogTerm:  int32(request.PrevLogTerm),
		Entries:      pbEntries,
		LeaderCommit: int32(request.LeaderCommit),
	})

	if err != nil {
		return nil, err
	}

	return &dtos.RaftAppendEntriesResponse{
		Term:    int(response.Term),
		Success: response.Success,
	}, nil
}

func (*RaftClientCallerImpl) RequestVote(client entities.RaftClient, request dtos.RaftRequestVoteRequest) (*dtos.RaftRequestVoteResponse, error) {

	response, err := client.ProtoClient.RequestVote(context.Background(), &pb.RaftRequestVoteRequest{
		Term:         int32(request.Term),
		CandidateId:  int32(request.CandidateId),
		LastLogIndex: int32(request.LastLogIndex),
		LastLogTerm:  int32(request.LastLogTerm),
	})
	if err != nil {
		return nil, err
	}
	return &dtos.RaftRequestVoteResponse{
		Term:        int(response.Term),
		VoteGranted: response.VoteGranted,
	}, nil
}

func (*RaftClientCallerImpl) ApplyMembership(client entities.RaftClient, request dtos.RaftApplyMembershipRequest) (*dtos.RaftApplyMembershipResponse, error) {
	response, err := client.ProtoClient.ApplyMembership(context.Background(), &pb.RaftApplyMembershipRequest{
		Address: &pb.Address{
			Host: request.Address.Host,
			Port: request.Address.Port,
		},
	})

	if err != nil {
		return nil, err
	}

	addresses := []entities.Address{}
	for _, member := range response.Members {
		addresses = append(addresses, *entities.NewAddress(member.Host, member.Port))
	}

	return &dtos.RaftApplyMembershipResponse{
		Id:      int(response.Id),
		Members: addresses,
		Success: response.Success,
		Term:    int(response.Term),
	}, nil
}

// func (r *raftClientCaller) GetAddress() entities.Address {
// 	return r.raftClient.Address
// }

// func (r *raftClientCaller) SetAddress(address entities.Address) {
// 	r.raftClient.Address = address
// }

// func (r *raftClientCaller) GetNextIndex() int {
// 	return r.raftClient.NextIndex
// }

// func (r *raftClientCaller) SetNextIndex(nextIndex int) {
// 	r.raftClient.NextIndex = nextIndex
// }

// func (r *raftClientCaller) GetMatchIndex() int {
// 	return r.raftClient.MatchIndex
// }

// func (r *raftClientCaller) SetMatchIndex(matchIndex int) {
// 	r.raftClient.MatchIndex = matchIndex
// }
