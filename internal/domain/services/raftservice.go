package services

import (
	"context"
	"if3230-tubes-williamjeffersonclinton/internal/clients"
	"if3230-tubes-williamjeffersonclinton/internal/domain/constants"
	"if3230-tubes-williamjeffersonclinton/internal/domain/dtos"
	entities "if3230-tubes-williamjeffersonclinton/internal/domain/entities"
	repository "if3230-tubes-williamjeffersonclinton/internal/domain/repositories"
	"if3230-tubes-williamjeffersonclinton/internal/utils"

	"fmt"
	"sync"
	"time"
)

type RaftService struct {
	state entities.State
	memberLock sync.Mutex

	caller clients.RaftClientCaller

	logRepository repository.LogRepositoryInterface
	kvRepository  repository.KeyValueRepositoryInterface

	heartbeatRcvStop  chan struct{}
	heartbeatRcvReset chan struct{}
	heartbeatSendStop chan struct{}
}

func NewRaftService(state entities.State, kvRepository repository.KeyValueRepositoryInterface, logRepository repository.LogRepositoryInterface) *RaftService {
	return &RaftService{
		state: state,

		caller: clients.NewRaftClientCaller(context.Background()),

		logRepository: logRepository,
		kvRepository:  kvRepository,

		heartbeatRcvStop:  make(chan struct{}),
		heartbeatRcvReset: make(chan struct{}),
		heartbeatSendStop: make(chan struct{}),
	}
}

func NewRaftLeaderService(state entities.State, kvRepository repository.KeyValueRepositoryInterface, logRepository repository.LogRepositoryInterface) *RaftService {
	service := NewRaftService(state, kvRepository, logRepository)
	service.state.Role = constants.NODE_ROLE_LEADER
	/* Add log for leader */
	log := entities.Log{
		Index:   service.logRepository.GetLastIndex() + 1,
		Command: "mem " + state.Address.Host + " " + state.Address.Port,
		Term:    service.state.CurrentTerm,
	}

	service.logRepository.Append(log)

	
	go service.startHeartbeatListener()
	go service.startHeartbeatEmitter()
	return service
}

func NewRaftFollowerService(state entities.State, kvRepository repository.KeyValueRepositoryInterface, logRepository repository.LogRepositoryInterface, masterAddress entities.Address) *RaftService {
	service := NewRaftService(state, kvRepository, logRepository)

	master := entities.NewRaftClient(masterAddress)
	utils.LogInfo("[CREATE RAFT] trying to apply membership to ", masterAddress.String())
	response, err := service.caller.ApplyMembership(context.Background(), *master, dtos.RaftApplyMembershipRequest{Address: state.Address})
	if err != nil {
		utils.LogError("[CREATE RAFT] failed to apply membership: %v", err)
	}
	utils.LogInfo("[CREATE RAFT] membership granted from ", masterAddress.String())

	utils.Print("[MEMBERSHIP] Current members: ")
	members := []entities.RaftClient{}
	for _, member := range response.Members {
		client := entities.NewRaftClient(*entities.NewAddress(member.Host, member.Port))
		members = append(members, *client)
		addr := client.Address
		addrString := (&addr).String
		utils.Print(addrString, ",")
	}

	/* Binding */
	service.state.Members = members
	utils.Print("\n")

	/* Update state id */
	service.state.Id = response.Id

	go service.startHeartbeatListener()
	return service
}

func (k *RaftService) ForceCommit(request dtos.RaftForceCommitRequest) (response *dtos.RaftForceCommitResponse) {
	log, success := k.logRepository.Get(request.Index)
	if (!success) {
		return &dtos.RaftForceCommitResponse{
			Success: false,
		}
	}

	command, arg1, arg2 := log.Parse()
	if command == "mem" {
		utils.LogInfo("[COMMIT] Add member ", arg1,":", arg2)
		k.state.AddMember(entities.Address{Host: arg1, Port: arg2})
	} else { 
		utils.LogInfo("[COMMIT] Execute command ", command,arg1, arg2)
		k.kvRepository.Execute(command, arg1, arg2)
	}
	if (!success) {
		return &dtos.RaftForceCommitResponse{
			Success: true,
		}
	}
}

func (k *RaftService) AppendEntries(request dtos.RaftAppendEntriesRequest) (response *dtos.RaftAppendEntriesResponse) {
	k.heartbeatRcvReset <- struct{}{}
	// utils.LogInfo("[APPEND] getting append request from ", k.state.Members[request.LeaderId].Address.String())

	if request.LeaderId != k.state.LeaderId {
		k.state.LeaderId = request.LeaderId
		k.state.VotedFor = constants.INVALID_ID
		k.state.IsElecting = false
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
	if request.PrevLogIndex != constants.INVALID_ID {
		log, success := k.logRepository.Get(request.PrevLogIndex)
		if !success || log.Term != request.PrevLogTerm {
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
			utils.LogInfo("New Log Term ", newLog.Term)
			utils.LogInfo("Log ", log1.Index, "Term ", log1.Term)
			if log1.Term != newLog.Term {
				utils.LogInfo("Term Difference")
				// Delete the existing entry and all that follow it
				k.logRepository.DeleteLogsFromIndex(newLog.Index)
				// Append the new log
				k.logRepository.Append(newLog)

			}
		} else {
			utils.LogInfo("No conflict")
			// No conflict, append the new log
			k.logRepository.Append(newLog)

		}
	}

	utils.LogInfo("CommitIndex: ", k.state.CommitIndex, "; LeaderCommit: ", request.LeaderCommit)
	if k.state.CommitIndex < request.LeaderCommit {
		/* Execute every command from commitIndex + 1 to leaderCommit index */
		/* Past Commit Index */
		startIdx := k.state.CommitIndex + 1

		// If leaderCommit > CommitIndex, set CommitIndex = min(leaderCommit, index of last new entry)
		/* Up to date Commit Index */
		if len(request.Entries) != 0 {
			k.state.CommitIndex = min(request.LeaderCommit, request.Entries[len(request.Entries)-1].Index)
		} else {
			k.state.CommitIndex = request.LeaderCommit
		}
		utils.LogInfo("Updated commit index: ", k.state.CommitIndex)

		// i = LeaderCommitIndex; i harus <= LeaderCommitIndex - 1
		utils.LogInfo("Start Idx: ", startIdx)
		utils.LogInfo("Commit index: ", k.state.CommitIndex)
		utils.LogInfo("Request len: ", len(request.Entries))

		for i := startIdx; i <= k.state.CommitIndex; i++ {
			// Get log
			log, ok := k.logRepository.Get(i)

			if ok {
				command, arg1, arg2 := log.Parse()
				if command == "mem" {
					utils.LogInfo("[COMMIT] Add member ", arg1, ":", arg2)
					k.state.AddMember(entities.Address{Host: arg1, Port: arg2})
				} else {
					utils.LogInfo("[COMMIT] Execute command ", command, arg1, arg2)
					k.kvRepository.Execute(command, arg1, arg2)
				}
			} else {
				utils.LogError("failed to get index: ", i, " from follower log. Current log last idx: ", k.logRepository.GetLastIndex())
			}
		}
	}

	/* Display members for testing purpose */
	utils.LogInfo("MEMBERS:")
	utils.LogInfo(k.state.Members)

	utils.LogInfo("[APPEND] Agree to append entries")
	return &dtos.RaftAppendEntriesResponse{
		Term:    k.state.CurrentTerm,
		Success: true,
	}
}

func (k *RaftService) RequestVote(request dtos.RaftRequestVoteRequest) (response *dtos.RaftRequestVoteResponse) {
	k.state.IsElecting = true
	k.heartbeatRcvReset <- struct{}{}

	utils.LogInfo("[VOTE] getting vote request from candidate ", request.CandidateId)
	// utils.LogInfo("[VOTE] getting vote request from candidate ", request.CandidateId, "(", k.state.Members[request.CandidateId].Address.String(), ")")
	lastLog, _ := k.logRepository.GetLastLog()

	/* Reply false if term < CurrentTerm */
	if request.Term < k.state.CurrentTerm {
		return &dtos.RaftRequestVoteResponse{
			Term:        k.state.CurrentTerm,
			VoteGranted: false,
		}
	}

	k.state.CurrentTerm = request.Term

	/*  Reply false if candidate's log is not as up-to-date as the receiver's log */
	if request.LastLogTerm < lastLog.Term ||
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

	/* Append member to existing members */
	k.state.Members = append(k.state.Members, *entities.NewRaftClient(request.Address))

	/* Log for Add Member Command */
	log := entities.Log{
		Index:   k.logRepository.GetLastIndex() + 1,
		Command: "mem " + request.Address.Host + " " + request.Address.Port,
		Term:    k.state.CurrentTerm,
	}

	k.logRepository.Append(log)

	return &dtos.RaftApplyMembershipResponse{
		Id:      len(k.state.Members) - 1,
		Members: []entities.Address{},
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
			/* Display members for testing purpose */
			// utils.LogInfo("MEMBERS:")
			// utils.LogInfo(k.state.Members)

			lastLog, _ := k.logRepository.GetLastLog()

			utils.LogInfo("Last log index ", lastLog.Index)

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
			utils.LogInfo("MELVIN")
			if (k.state.Role == constants.NODE_ROLE_FOLLOWER && k.state.LeaderId != constants.INVALID_ID) {
				utils.LogInfo("[HEARTBEAT] Heartbeat timeout, start voting...")
				go k.initializeVoting()
			}
			timer.Reset(k.state.HeartbeatTimeout)
		case <-k.heartbeatRcvReset:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(k.state.HeartbeatTimeout)
		}
	}
}

func (k *RaftService) PropagateLog() error {
	var wg sync.WaitGroup
	countAgree := 1
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

		// utils.LogInfo("NEXT IDX BEFORE LOOP: ", member.NextIndex)
		for i = member.NextIndex; i <= k.logRepository.GetLastIndex(); i++ {
			if i == -1 {
				continue
			}
			utils.LogInfo("FILLING ENTRIES")
			utils.LogInfo("Log: ", i, "; term: ", logMap[i].Term)
			entries = append(entries, logMap[i])
		}

		// fmt.Printf("[Propagate Log] entries content: %+v", entries)
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			resp, err := k.caller.AppendEntries(context.Background(), k.state.Members[id], dtos.RaftAppendEntriesRequest{
				Term:         k.state.CurrentTerm,
				LeaderId:     k.state.Id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: k.state.CommitIndex,
			})

			if err != nil {
				utils.LogWarning("Error sending AppendEntries to member %v: %v\n", k.state.Members[id], err)
				return
			}
			if resp.Success {

				mutex.Lock()
				countAgree++
				mutex.Unlock()
				k.state.Members[id].NextIndex = k.logRepository.GetLastIndex() + 1
				utils.LogInfo("UPDATED NEXT INDEX TO ", k.state.Members[id].NextIndex)
			} else {
				utils.LogInfo("DECREMENT NEXT INDEX")
				k.state.Members[id].NextIndex--
			}
		}(id)
	}
	wg.Wait()
	utils.Println("[CLIENT COMMAND] countAgree: ", countAgree)
	if countAgree < len(k.state.Members)/2 {
		return fmt.Errorf("quorum not reached")
	}

	// do leader commit
	lastIdx := k.logRepository.GetLastIndex()
	log, ok := k.logRepository.Get(lastIdx)

	if ok {
		command, arg1, arg2 := log.Parse()
		if command == "mem" {
			utils.LogInfo("[COMMIT] Add member ", arg1, ":", arg2)
			k.state.AddMember(entities.Address{Host: arg1, Port: arg2})
		} else {
			utils.LogInfo("[COMMIT] Execute command ", command, arg1, arg2)
			k.kvRepository.Execute(command, arg1, arg2)
		}
	} else {
		return fmt.Errorf("failed to get index: %d  from follower log. Current log last idx: %d", lastIdx, k.logRepository.GetLastIndex())
	}

	return nil
}

func (k *RaftService) sendEntriesToAllMembers() {
	var wg sync.WaitGroup
	countAgree := 1
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

		// utils.LogInfo("NEXT IDX BEFORE LOOP: ", member.NextIndex)
		for i = member.NextIndex; i <= k.logRepository.GetLastIndex(); i++ {
			if i == -1 {
				continue
			}
			utils.LogInfo("Fill entries")
			utils.LogInfo("Log ", i, " term: ", logMap[i].Term)
			entries = append(entries, logMap[i])
		}

		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			resp, err := k.caller.AppendEntries(context.Background(), k.state.Members[id], dtos.RaftAppendEntriesRequest{
				Term:         k.state.CurrentTerm,
				LeaderId:     k.state.Id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: k.state.CommitIndex,
			})

			if err != nil {
				utils.LogWarning("Error sending AppendEntries to member %v: %v\n", k.state.Members[id], err)
				return
			}
			if resp.Success {

				mutex.Lock()
				countAgree++
				mutex.Unlock()
				k.state.Members[id].NextIndex = k.logRepository.GetLastIndex() + 1
				utils.LogInfo("Update NextIndex: ", k.state.Members[id].NextIndex)
			} else {
				if (resp.Term > k.state.CurrentTerm ) {
					k.state.Role = constants.NODE_ROLE_FOLLOWER
					k.heartbeatSendStop <- struct{}{}
					go k.startHeartbeatListener()
				} else {
					utils.LogInfo("DECREMENT NEXT INDEX")
					k.state.Members[id].NextIndex--
				}
			}
		}(id)
	}
	wg.Wait()
	utils.Println("Agreement: ", countAgree)
	if countAgree > len(k.state.Members)/2 {
		utils.LogInfo("Success commit")
		k.state.CommitIndex = k.logRepository.GetLastIndex()
	}
}

func (k *RaftService) initializeVoting() {
	k.state.IsElecting = true
	utils.Println("[ELECTION] Initialize voting")
	utils.Println("Heartbeat timeout: ", k.state.HeartbeatTimeout)
	/* Display members for testing purpose */
	// utils.LogInfo("MEMBERS:")
	// utils.LogInfo(k.state.Members)

	k.state.CurrentTerm++
	k.state.VotedFor = k.state.Id
	k.state.Role = constants.NODE_ROLE_CANDIDATE

	var wg sync.WaitGroup
	countAgree := 1
	var mutex sync.Mutex

	/* Display members for testing purpose */
	// utils.LogInfo("MEMBERS:")
	// utils.LogInfo(k.state.Members)

	for id, member := range k.state.Members {
		if id == k.state.Id {
			continue
		}
		wg.Add(1)
		go func(member entities.RaftClient, id int) {
			defer wg.Done()
			utils.LogInfo("Sending to:", id)
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
			ctx, cancel := context.WithTimeout(context.Background(), k.state.HeartbeatTimeout)
			defer cancel()
			resp, err := k.caller.RequestVote(ctx, member, dtos.RaftRequestVoteRequest{
				Term:         k.state.CurrentTerm,
				CandidateId:  k.state.Id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})

			if err != nil {
				utils.LogWarning("Error sending RequestVote to member %v: %v\n", member, err)
				return
			}
			if resp.VoteGranted {
				utils.LogInfo("Ack from:", id)
				mutex.Lock()
				countAgree++
				mutex.Unlock()
			}
		}(member, id)
	}

	wg.Wait()
	utils.Println("[ELECTION] count agree: ", countAgree)

	if countAgree > len(k.state.Members)/2 {
		utils.Println("[ELECTION] count agree > majority, start serving as a leader", countAgree)
		go k.startHeartbeatEmitter()
		k.state.Role = constants.NODE_ROLE_LEADER
	} else {
		utils.Println("[ELECTION] count agree < majority, return to follower", countAgree)
		k.state.Role = constants.NODE_ROLE_FOLLOWER
	}
}


f