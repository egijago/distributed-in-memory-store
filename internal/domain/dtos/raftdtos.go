package dtos

import (
	entity "if3230-tubes-williamjeffersonclinton/internal/domain/entities"
)

type RaftAppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entity.Log
	LeaderCommit int
}

type RaftAppendEntriesResponse struct {
	Term    int
	Success bool
}

type RaftRequestVoteRequest struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}
type RaftRequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

type RaftApplyMembershipRequest struct {
	Address entity.Address
}
type RaftApplyMembershipResponse struct {
	Id      int
	Members []entity.Address
	Term    int
	Success bool
}
