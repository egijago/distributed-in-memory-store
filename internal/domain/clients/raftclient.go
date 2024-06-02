package clients

import (
	"context"
	"log"

	"github.com/melvinkj/raft-consensus/internal/domain/dtos"
	entity "github.com/melvinkj/raft-consensus/internal/domain/entities"
	pb "github.com/melvinkj/raft-consensus/internal/pb/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftClient struct {
	Address    entity.Address
	NextIndex  int
	MatchIndex int
	protoClient   pb.RaftServiceClient
}

func NewRaftClient(address entity.Address) *RaftClient {
	conn, err := grpc.NewClient(address.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return &RaftClient{
		Address:  address,
		protoClient: pb.NewRaftServiceClient(conn),
	}
}

func (r *RaftClient) AppendEntries(ctx context.Context, request dtos.RaftAppendEntriesRequest) (*dtos.RaftAppendEntriesResponse, error) {
	pbEntries := []*pb.Log{}
	for _, entry := range request.Entries {
		pbEntries = append(pbEntries, &pb.Log{
			Index: int32(entry.Index),
			Command: entry.Command,
			Term: int32(entry.Term),
		})
	}


	response, err := r.protoClient.AppendEntries(ctx, &pb.RaftAppendEntriesRequest{
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
	

func (r *RaftClient) RequestVote(ctx context.Context, request dtos.RaftRequestVoteRequest) (*dtos.RaftRequestVoteResponse, error) {
	
	response, err := r.protoClient.RequestVote(ctx, &pb.RaftRequestVoteRequest{
		Term: int32(request.Term),
		CandidateId: int32(request.CandidateId),
		LastLogIndex: int32(request.LastLogIndex),
		LastLogTerm: int32(request.LastLogTerm),
	})
	if err != nil {
		return nil, err
	}
	return &dtos.RaftRequestVoteResponse{
		Term:        int(response.Term),
		VoteGranted: response.VoteGranted,
	}, nil
}

func (r *RaftClient) ApplyMembership(ctx context.Context, request dtos.RaftApplyMembershipRequest) (*dtos.RaftApplyMembershipResponse, error) {
	response, err := r.protoClient.ApplyMembership(ctx, &pb.RaftApplyMembershipRequest{
		Address: &pb.Address{
			Host: request.Address.Host,
			Port: request.Address.Port,
		},
	})

	if err != nil {
		return nil, err
	}

	addreses := []entity.Address{}
	for _, member := range response.Members {
		addreses = append(addreses, *entity.NewAddress(member.Host, member.Port))
	}


	return &dtos.RaftApplyMembershipResponse{
		Id:      int(response.Id),
		Members: addreses,
		Success: response.Success,
		Term:    int(response.Term),
	}, nil
}
