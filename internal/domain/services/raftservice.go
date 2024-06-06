package services

import (
	"if3230-tubes-williamjeffersonclinton/internal/clients"
	"if3230-tubes-williamjeffersonclinton/internal/domain/constants"
	"if3230-tubes-williamjeffersonclinton/internal/domain/dtos"
	entities "if3230-tubes-williamjeffersonclinton/internal/domain/entities"
	repository "if3230-tubes-williamjeffersonclinton/internal/domain/repositories"
	"if3230-tubes-williamjeffersonclinton/internal/utils"

	"sync"
	"time"
)

type RaftService struct {
	state  entities.State
	caller clients.RaftClientCaller

	logRepository repository.LogRepositoryInterface
	kvRepository  repository.KeyValueRepositoryInterface

	heartbeatRcvStop  chan struct{}
	heartbeatRcvReset chan struct{}
	heartbeatSendStop chan struct{}

	roleMutex sync.Mutex // TODO: Implement mutex for role
}

func NewRaftService(state entities.State, kvRepository repository.KeyValueRepositoryInterface, logRepository repository.LogRepositoryInterface) *RaftService {
	return &RaftService{
		state: state,

		caller: &clients.RaftClientCallerImpl{},

		logRepository: logRepository,
		kvRepository:  kvRepository,

		heartbeatRcvStop:  make(chan struct{}),
		heartbeatRcvReset: make(chan struct{}),
		heartbeatSendStop: make(chan struct{}),
	}
}

func NewRaftLeaderService(state entities.State, kvRepository repository.KeyValueRepositoryInterface, logRepository repository.LogRepositoryInterface) *RaftService {
	service := NewRaftService(state, kvRepository, logRepository)
	go service.startHeartbeatEmitter()
	return service
}

func NewRaftFollowerService(state entities.State, kvRepository repository.KeyValueRepositoryInterface, logRepository repository.LogRepositoryInterface, masterAddress entities.Address) *RaftService {
	service := NewRaftService(state, kvRepository, logRepository)

	master := entities.NewRaftClient(masterAddress)
	utils.LogInfo("[CREATE RAFT] trying to apply membership to ", masterAddress.String())
	response, err := service.caller.ApplyMembership(*master, dtos.RaftApplyMembershipRequest{Address: state.Address})
	if err != nil {
		utils.LogError("[CREATE RAFT] failed to apply membership: %v", err)
	}
	utils.LogInfo("[CREATE RAFT] membership granted from ", masterAddress.String())

	utils.Print("Current members: ")
	members := []entities.RaftClient{}
	for _, member := range response.Members {
		client := entities.NewRaftClient(*entities.NewAddress(member.Host, member.Port))
		members = append(members, *client)
		addr := client.Address
		addrString := (&addr).String
		utils.Print(addrString, ",")
	}
	utils.Print("\n")

	go service.startHeartbeatListener()
	return service
}

func (k *RaftService) AppendEntries(request dtos.RaftAppendEntriesRequest) (response *dtos.RaftAppendEntriesResponse) {
	k.heartbeatRcvReset <- struct{}{}
	// utils.LogInfo("[APPEND] getting append request from ", k.state.Members[request.LeaderId].Address.String())

	if request.LeaderId != k.state.LeaderId {

		k.state.LeaderId = request.LeaderId
		k.state.VotedFor = constants.INVALID_ID
	}
	/* Reply false if term < CurrentTerm */
	if request.Term < k.state.CurrentTerm {
		utils.LogInfo("[APPEND] Aborted: term < CurrentTerm")
		return &dtos.RaftAppendEntriesResponse{
			Term:    k.state.CurrentTerm,
			Success: false,
		}
	}

	/*
		Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
		Leader must retry with lower index.
	*/
	utils.LogInfo("PREV LOG IDX")
	utils.LogInfo(request.PrevLogIndex)
	if request.PrevLogIndex != constants.INVALID_ID {
		utils.LogInfo(request.PrevLogIndex)
		utils.LogInfo("HUAHAHA")
		log, success := k.logRepository.Get(request.PrevLogIndex)
		if !success || log.Term != request.PrevLogTerm {
			if !success {
				utils.LogInfo("HOHO")
			}
			utils.LogInfo("LOG TERM")
			utils.LogInfo(log.Term)
			utils.LogInfo("PREV LOG TERM")
			utils.LogInfo(request.PrevLogTerm)

			if log.Term != request.PrevLogTerm {
				utils.LogInfo("HIHI")
			}
			utils.LogInfo("[APPEND] Aborted: log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm")
			return &dtos.RaftAppendEntriesResponse{
				Term:    k.state.CurrentTerm,
				Success: false,
			}
		}
	}

	/* If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it */
	for _, newLog := range request.Entries {
		log1, indexExists := k.logRepository.Get(newLog.Index)
		if indexExists {
			// Conflict detected: same index but different term
			if log1.Term != newLog.Term {
				utils.LogInfo("INDEX EXISTS TERM DIFF")
				// Delete the existing entry and all that follow it
				k.logRepository.DeleteLogsFromIndex(newLog.Index)
				// Append the new log
				k.logRepository.Append(newLog)

				//  If leaderCommit > CommitIndex, set CommitIndex = min(leaderCommit, index of last new entry)
				if request.LeaderCommit > k.state.CommitIndex {
					k.state.CommitIndex = min(request.LeaderCommit, newLog.Index)
				}
			}
		} else {
			utils.LogInfo("No conflict")
			// No conflict, append the new log
			k.logRepository.Append(newLog)

			//  If leaderCommit > CommitIndex, set CommitIndex = min(leaderCommit, index of last new entry)
			if request.LeaderCommit > k.state.CommitIndex {
				k.state.CommitIndex = min(request.LeaderCommit, newLog.Index)
			}
		}
	}

	if k.state.CommitIndex < request.LeaderCommit {
		/* Execute every command from commitIndex + 1 to leaderCommit index */
		for i := k.state.CommitIndex + 1; i <= request.LeaderCommit; i++ {
			// Get log
			log, ok := k.logRepository.Get(i)

			if ok {
				k.state.CommitIndex++
				command, arg1, arg2 := log.Parse()
				if command == "mem" {
					k.state.AddMember(entities.Address{Host: arg1, Port: arg2})
				} else { /* Add Member */
					utils.LogInfo("Add Member")
					k.kvRepository.Execute(command, arg1, arg2)
				}
			}
		}
	}

	utils.LogInfo("[APPEND] Agree to append entries")
	utils.LogInfo(k.logRepository.GetLastIndex())
	return &dtos.RaftAppendEntriesResponse{
		Term:    k.state.CurrentTerm,
		Success: true,
	}
}

func (k *RaftService) RequestVote(request dtos.RaftRequestVoteRequest) (response *dtos.RaftRequestVoteResponse) {
	k.heartbeatRcvStop <- struct{}{}

	utils.LogInfo("[VOTE] getting vote request from candidate ", request.CandidateId, "(", k.state.Members[request.CandidateId].Address.String(), ")")
	lastLog, _ := k.logRepository.GetLastLog()
	/* Reply false if term < CurrentTerm or candidate's log is not as up-to-date as the receiver's log */
	if request.Term < k.state.CurrentTerm ||
		request.LastLogTerm < lastLog.Term ||
		request.LastLogIndex < lastLog.Index {
		return &dtos.RaftRequestVoteResponse{
			Term:        k.state.CurrentTerm,
			VoteGranted: false,
		}
	}

	/* Reply false if not VotedFor is null or candidateId log */
	if !(k.state.VotedFor == -1 || k.state.VotedFor == request.CandidateId) {
		return &dtos.RaftRequestVoteResponse{
			Term:        k.state.CurrentTerm,
			VoteGranted: false,
		}
	}

	k.state.VotedFor = request.CandidateId
	return &dtos.RaftRequestVoteResponse{
		Term:        k.state.CurrentTerm,
		VoteGranted: true,
	}
}

func (k *RaftService) ApplyMembership(request dtos.RaftApplyMembershipRequest) (response *dtos.RaftApplyMembershipResponse) {
	utils.LogInfo("[MEMBERSHIP] getting membership request from ", request.Address.String())
	utils.LogInfo("[MEMBERSHIP] membership granted to ", request.Address.String())

	// TODO: CHECK IF MASTER
	/* Append member to existing members */
	k.state.Members = append(k.state.Members, *entities.NewRaftClient(request.Address))

	memberAddresses := []entities.Address{}
	utils.Print("Current members: ")
	for _, member := range k.state.Members {
		utils.Print(member.Address.String(), ",")
		memberAddresses = append(memberAddresses, member.Address)
	}
	utils.Print("\n")

	/* Log for Add Member Command */
	log := entities.Log{
		Index:   k.logRepository.GetLastIndex() + 1,
		Command: "mem " + request.Address.Host + " " + request.Address.Port,
		Term:    k.state.CurrentTerm,
	}

	k.logRepository.Append(log)

	return &dtos.RaftApplyMembershipResponse{
		Id:      len(k.state.Members) - 1,
		Members: memberAddresses,
		Term:    k.state.CurrentTerm,
		Success: true,
	}
}

func (k *RaftService) startHeartbeatEmitter() {
	for {
		select {
		case <-k.heartbeatSendStop:
			return
		default:
			utils.LogInfo("[HEARTBEAT] Sending heartbeat...")
			utils.LogInfo(k.logRepository.GetLastIndex())
			lastLog, _ := k.logRepository.GetLastLog()

			utils.LogInfo(lastLog.Index)
			k.sendEntriesToAllMembers()
			time.Sleep(k.state.HeartbeatInterval)
		}
	}
}

func (k *RaftService) startHeartbeatListener() {

	timer := time.NewTimer(k.state.HeartbeatTimeout)
	defer timer.Stop()

	for {
		select {
		case <-k.heartbeatRcvStop:
			utils.LogInfo("[HEARTBEAT] Stop listening to heartbeat")
			return
		case <-timer.C:
			utils.LogInfo("[HEARTBEAT] Heartbeat timeout, start voting...")
			k.initializeVoting()
			timer.Reset(k.state.HeartbeatTimeout)
			return
		case <-k.heartbeatRcvReset:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(k.state.HeartbeatTimeout)
		}
	}
}

func (k *RaftService) sendEntriesToAllMembers() {
	var wg sync.WaitGroup
	countAgree := 0
	var mutex sync.Mutex

	for id, member := range k.state.Members {
		if id == k.state.Id {
			utils.LogInfo("[HEARTBEAT] Skip sending heartbeat to self")
			continue
		}
		prevLog, found := k.logRepository.Get(member.NextIndex - 1)

		prevLogIndex := constants.INVALID_ID
		prevLogTerm := constants.INVALID_ID
		if found {
			prevLogIndex = prevLog.Index
			prevLogTerm = prevLog.Term
		}

		utils.LogInfo("[HEARTBEAT] Sending heartbeat to ", member.Address.String(), "...")

		logMap := k.logRepository.GetBuffer()

		// Convert map[int] tp [int]
		var entries []entities.Log
		var i int
		for i = member.NextIndex; i <= k.logRepository.GetLastIndex(); i++ {
			entries = append(entries, logMap[i])
		}

		wg.Add(1)
		go func(member entities.RaftClient) {
			defer wg.Done()
			resp, err := k.caller.AppendEntries(member, dtos.RaftAppendEntriesRequest{
				Term:         k.state.CurrentTerm,
				LeaderId:     k.state.Id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: k.state.CommitIndex,
			})

			if err != nil {
				utils.LogWarning("Error sending AppendEntries to member %v: %v\n", member, err)
				return
			}
			if resp.Success {
				mutex.Lock()
				countAgree++
				mutex.Unlock()
				member.NextIndex = k.logRepository.GetLastIndex() + 1
			} else {
				member.NextIndex--
			}
		}(member)
	}
	wg.Wait()
	utils.Println("countAgree: ", countAgree)
	if countAgree > len(k.state.Members)/2 {
		utils.LogInfo("Success commit")
		k.state.CommitIndex = k.logRepository.GetLastIndex()
	}
}

func (k *RaftService) initializeVoting() {
	utils.Println("[ELECTION] Initialize voting")
	k.state.CurrentTerm++
	k.state.VotedFor = k.state.Id
	k.state.Role = constants.NODE_ROLE_CANDIDATE

	var wg sync.WaitGroup
	countAgree := 1
	var mutex sync.Mutex
	for id, member := range k.state.Members {
		if id == k.state.Id {
			continue
		}
		wg.Add(1)
		go func(member entities.RaftClient) {
			defer wg.Done()

			lastLog, found := k.logRepository.GetLastLog()
			var lastLogIndex int
			var lastLogTerm int
			if !found {
				lastLogIndex = -1
				lastLogTerm = -1
			} else {
				lastLogIndex = lastLog.Index
				lastLogTerm = lastLog.Term
			}
			resp, err := k.caller.RequestVote(member, dtos.RaftRequestVoteRequest{
				Term:         k.state.CurrentTerm,
				CandidateId:  k.state.Id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})

			if err != nil {
				utils.LogWarning("Error sending AppendEntries to member %v: %v\n", member, err)
				return
			}
			if resp.VoteGranted {
				mutex.Lock()
				countAgree++
				mutex.Unlock()
			}
		}(member)
	}

	wg.Wait()
	utils.Println("[ELECTION] count agree: ", countAgree)

	// 150 - 300 ms
	if countAgree > len(k.state.Members)/2 {
		utils.LogInfo(len(k.state.Members))
		utils.Println("[ELECTION] count agree > majority, start serving as a leader", countAgree)
		go k.startHeartbeatEmitter()
	} else {
		utils.Println("[ELECTION] count agree < majority, return to follower", countAgree)
		go k.startHeartbeatListener()
	}

	// if k.state.Role == constants.NODE_ROLE_CANDIDATE {
	// 	k.state.Role
}
