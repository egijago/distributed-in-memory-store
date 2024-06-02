package handlers

import (
	"if3230-tubes-williamjeffersonclinton/internal/domain/dtos"
	entity "if3230-tubes-williamjeffersonclinton/internal/domain/entities"
	pb "if3230-tubes-williamjeffersonclinton/internal/pb/proto"
	
	"context"
)

func (s *GrpcHandler) AppendEntries(ctx context.Context, req *pb.RaftAppendEntriesRequest) (*pb.RaftAppendEntriesResponse, error) {
	var entries []entity.Log
	for _, entry := range req.GetEntries() {
		entries = append(entries, entity.Log{
			Index:   int(entry.GetIndex()),
			Command: entry.GetCommand(),
			Term:    int(entry.GetTerm()),
		})
	}

	request := dtos.RaftAppendEntriesRequest{
		Term:         int(req.GetTerm()),
		LeaderId:     int(req.GetLeaderId()),
		PrevLogIndex: int(req.GetPrevLogIndex()),
		PrevLogTerm:  int(req.GetPrevLogTerm()),
		Entries:      entries,
		LeaderCommit: int(req.GetLeaderCommit()),
	}

	response := s.raftService.AppendEntries(request)
	return &pb.RaftAppendEntriesResponse{Term: int32(response.Term), Success: response.Success}, nil
}

func (s *GrpcHandler) RequestVote(ctx context.Context, req *pb.RaftRequestVoteRequest) (*pb.RaftRequestVoteResponse, error) {
	request := dtos.RaftRequestVoteRequest{
		Term:         int(req.GetTerm()),
		CandidateId:  int(req.GetCandidateId()),
		LastLogIndex: int(req.GetLastLogIndex()),
		LastLogTerm:  int(req.GetLastLogTerm()),
	}

	response := s.raftService.RequestVote(request)
	return &pb.RaftRequestVoteResponse{Term: int32(response.Term), VoteGranted: response.VoteGranted}, nil
}

func (s *GrpcHandler) ApplyMembership(ctx context.Context, req *pb.RaftApplyMembershipRequest) (*pb.RaftApplyMembershipResponse, error) {
	request := dtos.RaftApplyMembershipRequest{
		Address: *entity.NewAddress(req.Address.Host, req.Address.Port),
	}
	response := s.raftService.ApplyMembership(request)

	pbMembers := []*pb.Address{}
	for _, member := range response.Members {
		pbMembers = append(pbMembers, &pb.Address{Host: member.Host, Port: member.Port})
	}
	return &pb.RaftApplyMembershipResponse{Id: int32(response.Id), Members: pbMembers, Success: response.Success, Term: int32(response.Term)}, nil
}
