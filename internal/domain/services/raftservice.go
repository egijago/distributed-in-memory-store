package services

import (
	"if3230-tubes-williamjeffersonclinton/internal/domain/clients"
	"if3230-tubes-williamjeffersonclinton/internal/domain/dtos"
	entity "if3230-tubes-williamjeffersonclinton/internal/domain/entities"
	repository "if3230-tubes-williamjeffersonclinton/internal/domain/repository"
	"if3230-tubes-williamjeffersonclinton/internal/utils"

	"context"
	"sync"
	"time"
)

const NO_VOTE = -1
const INVALID_ID = -1
const INVALID_TERM = -1

type RaftService struct {
	Id      int
	Members []clients.RaftClient
	Address entity.Address

	// Persistent State
	CurrentTerm   int
	VotedFor      int
	logRepository repository.LogRepositoryInterface
	kvRepository  repository.KeyValueRepositoryInterface

	// Volatile State
	CommitIndex int
	LastApplied int

	// Volatile State on Leaders
	IsLeader   bool
	NextIndex  map[clients.RaftClient]int
	MatchIndex map[clients.RaftClient]int
}

func NewRaftService(address entity.Address, kvRepository repository.KeyValueRepositoryInterface, logRepository repository.LogRepositoryInterface) *RaftService {
	return &RaftService{
		Id:      0,
		Members: []clients.RaftClient{},
		Address: address,

		CurrentTerm:   0,
		VotedFor:      NO_VOTE,
		logRepository: logRepository,
		kvRepository:  kvRepository,

		CommitIndex: 0,
		LastApplied: 0,

		IsLeader:   false,
		NextIndex:  make(map[clients.RaftClient]int),
		MatchIndex: make(map[clients.RaftClient]int),
	}
}

func NewRaftLeaderService(address entity.Address, kvRepository repository.KeyValueRepositoryInterface, logRepository repository.LogRepositoryInterface) *RaftService {
	service := &RaftService{
		Id:      0,
		Members: []clients.RaftClient{},
		Address: address,

		CurrentTerm:   0,
		VotedFor:      NO_VOTE,
		logRepository: logRepository,
		kvRepository:  kvRepository,

		CommitIndex: 0,
		LastApplied: 0,

		IsLeader:   true,
		NextIndex:  make(map[clients.RaftClient]int),
		MatchIndex: make(map[clients.RaftClient]int),
	}
	service.InitializeAsLeader()
	return service
}

func NewRaftFollowerService(address entity.Address, kvRepository repository.KeyValueRepositoryInterface, logRepository repository.LogRepositoryInterface, masterAddress entity.Address) *RaftService {
	master := clients.NewRaftClient(masterAddress)
	utils.LogInfo("[CREATE RAFT] trying to apply membership to ", master.Address.String())
	response, error := master.ApplyMembership(context.Background(), dtos.RaftApplyMembershipRequest{Address: address})
	if error != nil {
		utils.LogError("[CREATE RAFT] failed to apply membership: %v", error)
	}
	utils.LogInfo("[CREATE RAFT] membership granted from ", master.Address.String())

	utils.Print("Current members: ")
	members := []clients.RaftClient{}
	for _, member := range response.Members {
		client := clients.NewRaftClient(*entity.NewAddress(member.Host, member.Port))
		members = append(members, *client)
		utils.Print(client.Address.String(), ",")
	}
	utils.Print("\n")

	return &RaftService{
		Id:      response.Id,
		Members: members,
		Address: address,

		CurrentTerm:   response.Id,
		VotedFor:      NO_VOTE,
		logRepository: logRepository,
		kvRepository:  kvRepository,

		CommitIndex: 0,
		LastApplied: 0,

		IsLeader:   false,
		NextIndex:  make(map[clients.RaftClient]int),
		MatchIndex: make(map[clients.RaftClient]int),
	}
}

func (k *RaftService) AppendEntries(request dtos.RaftAppendEntriesRequest) (response *dtos.RaftAppendEntriesResponse) {
	utils.LogInfo("[APPEND] getting append request from ", k.Members[request.LeaderId].Address.String())
	/* Reply false if term < CurrentTerm */
	// if request.Term < k.CurrentTerm {
	// 	utils.LogInfo("[APPEND] Aborted: term < CurrentTerm")
	// 	return &dtos.RaftAppendEntriesResponse{
	// 		Term:    k.CurrentTerm,
	// 		Success: false,
	// 	}
	// }

	// /* Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm */
	// log, success := k.logRepository.Get(request.PrevLogIndex)
	// if !success || log.Term != request.PrevLogTerm {
	// 	utils.LogInfo("[APPEND] Aborted: log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm")
	// 	return &dtos.RaftAppendEntriesResponse{
	// 		Term:    k.CurrentTerm,
	// 		Success: false,
	// 	}
	// }

	// /* If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it */
	// for _, newLog := range request.Entries {
	// 	log1, indexExists := k.logRepository.Get(newLog.Index)
	// 	if indexExists {
	// 		// Conflict detected: same index but different term
	// 		if log1.Term != newLog.Term {
	// 			// Delete the existing entry and all that follow it
	// 			k.logRepository.DeleteLogsFromIndex(newLog.Index)
	// 			// Append the new log
	// 			k.logRepository.Append(newLog)

	// 			//  If leaderCommit > CommitIndex, set CommitIndex = min(leaderCommit, index of last new entry)
	// 			if request.LeaderCommit > k.CommitIndex {
	// 				k.CommitIndex = min(request.LeaderCommit, newLog.Index)
	// 			}
	// 		}
	// 	} else {
	// 		// No conflict, append the new log
	// 		k.logRepository.Append(newLog)

	// 		//  If leaderCommit > CommitIndex, set CommitIndex = min(leaderCommit, index of last new entry)
	// 		if request.LeaderCommit > k.CommitIndex {
	// 			k.CommitIndex = min(request.LeaderCommit, newLog.Index)
	// 		}
	// 	}
	// }
	// k.logRepository.Append(request.log)
	// k.kvRepository.Set(request.log.GetKeyValue())

	utils.LogInfo("[APPEND] Agree to append entries")
	return &dtos.RaftAppendEntriesResponse{
		Term:    k.CurrentTerm,
		Success: true,
	}
}

func (k *RaftService) RequestVote(request dtos.RaftRequestVoteRequest) (response *dtos.RaftRequestVoteResponse) {
	utils.LogInfo("[VOTE] getting vote request from ", request.CandidateId)
	lastLog, _ := k.logRepository.GetLastLog()
	/* Reply false if term < CurrentTerm or candidate's log is not as up-to-date as the receiver's log */
	if request.Term < k.CurrentTerm ||
		request.LastLogTerm < lastLog.Term ||
		request.LastLogIndex < lastLog.Index {
		return &dtos.RaftRequestVoteResponse{
			Term:        k.CurrentTerm,
			VoteGranted: false,
		}
	}

	/* Reply false if not VotedFor is null or candidateId log */
	if !(k.VotedFor == -1 || k.VotedFor == request.CandidateId) {
		return &dtos.RaftRequestVoteResponse{
			Term:        k.CurrentTerm,
			VoteGranted: false,
		}
	}

	return &dtos.RaftRequestVoteResponse{
		Term:        k.CurrentTerm,
		VoteGranted: true,
	}
}

func (k *RaftService) ApplyMembership(request dtos.RaftApplyMembershipRequest) (response *dtos.RaftApplyMembershipResponse) {
	utils.LogInfo("[MEMBERSHIP] getting membership request from ", request.Address.String())
	utils.LogInfo("[MEMBERSHIP] membership granted to ", request.Address.String())

	k.Members = append(k.Members, *clients.NewRaftClient(request.Address))

	memberAddresses := []entity.Address{}
	utils.Print("Current members: ")
	for _, member := range k.Members {
		utils.Print(member.Address.String(), ",")
		memberAddresses = append(memberAddresses, member.Address)
	}
	utils.Print("\n")

	return &dtos.RaftApplyMembershipResponse{
		Id:      k.Id,
		Members: memberAddresses,
		Term:    k.CurrentTerm,
		Success: true,
	}
}

func (k *RaftService) InitializeAsLeader() {
	k.IsLeader = true
	go func() {
		for {
			k.PropagateEntries()
			time.Sleep(1000 * time.Millisecond)
		}
	}()
}

func (k *RaftService) PropagateEntries() {
	var wg sync.WaitGroup
	countAgree := 0
	var mutex sync.Mutex
	for _, member := range k.Members {
		wg.Add(1)
		go func(member clients.RaftClient) {
			defer wg.Done()

			resp, err := member.AppendEntries(context.Background(), dtos.RaftAppendEntriesRequest{
				Term:         k.CurrentTerm,
				LeaderId:     k.Id,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []entity.Log{},
				LeaderCommit: k.CommitIndex,
			})

			if err != nil {
				utils.LogWarning("Error sending AppendEntries to member %v: %v\n", member, err)
				return
			}
			if resp.Success {
				mutex.Lock()
				countAgree++
				mutex.Unlock()
			}
		}(member)
	}
	wg.Wait()
	utils.Println("countAgree: ", countAgree)
	// if countAgree > len(k.Members)/2 {
	// 	k.CommitIndex++
	// }
}
