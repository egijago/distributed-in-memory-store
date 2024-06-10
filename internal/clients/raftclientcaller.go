package clients

import (
	"if3230-tubes-williamjeffersonclinton/internal/domain/dtos"
	"if3230-tubes-williamjeffersonclinton/internal/domain/entities"
	pb "if3230-tubes-williamjeffersonclinton/internal/pb/proto"

	"context"
)

type RaftClientCallerImpl struct {
}

func NewRaftClientCaller(context context.Context) *RaftClientCallerImpl {
	return &RaftClientCallerImpl{}
}

type RaftClientCaller interface {
	AppendEntries(ctx context.Context, client entities.RaftClient, request dtos.RaftAppendEntriesRequest) (*dtos.RaftAppendEntriesResponse, error)
	RequestVote(ctx context.Context, client entities.RaftClient, request dtos.RaftRequestVoteRequest) (*dtos.RaftRequestVoteResponse, error)
	ApplyMembership(ctx context.Context, client entities.RaftClient, request dtos.RaftApplyMembershipRequest) (*dtos.RaftApplyMembershipResponse, error)
	PrepareAddMember(ctx context.Context, client entities.RaftClient, request dtos.RaftPrepareAddMemberRequest) (*dtos.RaftPrepareAddMemberResponse, error)
	CommitAddMember(ctx context.Context, client entities.RaftClient, request dtos.RaftCommitAddMemberRequest) (*dtos.RaftCommitAddMemberResponse, error)
}

func (l *RaftClientCallerImpl) AppendEntries(ctx context.Context, client entities.RaftClient, request dtos.RaftAppendEntriesRequest) (*dtos.RaftAppendEntriesResponse, error) {
	pbEntries := []*pb.Log{}
	for _, entry := range request.Entries {
		pbEntries = append(pbEntries, &pb.Log{
			Index:   int32(entry.Index),
			Command: entry.Command,
			Term:    int32(entry.Term),
		})
	}

	response, err := client.ProtoClient.AppendEntries(ctx, &pb.RaftAppendEntriesRequest{
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

func (l *RaftClientCallerImpl) RequestVote(ctx context.Context, client entities.RaftClient, request dtos.RaftRequestVoteRequest) (*dtos.RaftRequestVoteResponse, error) {

	response, err := client.ProtoClient.RequestVote(ctx, &pb.RaftRequestVoteRequest{
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

func (l *RaftClientCallerImpl) ApplyMembership(ctx context.Context, client entities.RaftClient, request dtos.RaftApplyMembershipRequest) (*dtos.RaftApplyMembershipResponse, error) {
	response, err := client.ProtoClient.ApplyMembership(ctx, &pb.RaftApplyMembershipRequest{
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

func (l *RaftClientCallerImpl) PrepareAddMember(ctx context.Context, client entities.RaftClient, request dtos.RaftPrepareAddMemberRequest) (*dtos.RaftPrepareAddMemberResponse, error) {
	response, err := client.ProtoClient.PrepareAddMember(ctx, &pb.RaftPrepareAddMemberRequest{
		Term:     int32(request.Term),
		LeaderId: int32(request.LeaderId),
	})

	if err != nil {
		return nil, err
	}

	return &dtos.RaftPrepareAddMemberResponse{
		Term:    int(response.Term),
		Success: response.Success,
	}, nil
}

func (l *RaftClientCallerImpl) CommitAddMember(ctx context.Context, client entities.RaftClient, request dtos.RaftCommitAddMemberRequest) (*dtos.RaftCommitAddMemberResponse, error) {
	response, err := client.ProtoClient.CommitAddMember(ctx, &pb.RaftCommitAddMemberRequest{
		Term:     int32(request.Term),
		LeaderId: int32(request.LeaderId),
		Commit:   request.Commit,
		Address: &pb.Address{
			Host: request.Address.Host,
			Port: request.Address.Port,
		},
	})

	if err != nil {
		return nil, err
	}

	return &dtos.RaftCommitAddMemberResponse{
		Term:    int(response.Term),
		Success: response.Success,
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
