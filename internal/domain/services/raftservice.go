package services

import (
	"context"
	"encoding/json"
	"strconv"

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
	state entities.StateInterface

	caller clients.RaftClientCaller

	logRepository repository.LogRepositoryInterface
	kvRepository  repository.KeyValueRepositoryInterface

	heartbeatRcvStop  chan struct{}
	heartbeatRcvReset chan struct{}
	heartbeatSendStop chan struct{}

	quorumScale float64
	quorum      int
	preparing   bool
}

type RaftServiceInterface interface {
	/* Raft RPC */
	AppendEntries(request dtos.RaftAppendEntriesRequest) (response *dtos.RaftAppendEntriesResponse)
	RequestVote(request dtos.RaftRequestVoteRequest) (response *dtos.RaftRequestVoteResponse)
	ApplyMembership(request dtos.RaftApplyMembershipRequest) (response *dtos.RaftApplyMembershipResponse)
	PrepareAddMember(request dtos.RaftPrepareAddMemberRequest) (response *dtos.RaftPrepareAddMemberResponse)
	CommitAddMember(request dtos.RaftCommitAddMemberRequest) (response *dtos.RaftCommitAddMemberResponse)
	State(request dtos.RaftStateRequest) (response *dtos.RaftStateResponse)

	/* Global interface */
	AppendEntriesToAllMembers() int
	GetQuorum() int
}

func NewRaftService(state entities.StateInterface, kvRepository repository.KeyValueRepositoryInterface, logRepository repository.LogRepositoryInterface, quorumScale float64) *RaftService {
	return &RaftService{
		state: state,

		caller: clients.NewRaftClientCaller(context.Background()),

		logRepository: logRepository,
		kvRepository:  kvRepository,

		heartbeatRcvStop:  make(chan struct{}),
		heartbeatRcvReset: make(chan struct{}),
		heartbeatSendStop: make(chan struct{}),

		quorumScale: quorumScale,
	}
}

func NewRaftLeaderService(state entities.StateInterface, kvRepository repository.KeyValueRepositoryInterface, logRepository repository.LogRepositoryInterface, quorumScale float64) *RaftService {
	utils.LogInfo("[CREATE RAFT] Create new raft node, serving as a leader")
	service := NewRaftService(state, kvRepository, logRepository, quorumScale)
	service.state.AddMember(state.GetAddress())
	service.state.SetId(0)
	service.state.SetRole(constants.NODE_ROLE_LEADER)
	service.state.SetLeaderId(state.GetId())
	service.state.SetCurrentTerm(0)
	go service.startHeartbeatEmitter()
	return service
}

func NewRaftFollowerService(state entities.StateInterface, kvRepository repository.KeyValueRepositoryInterface, logRepository repository.LogRepositoryInterface, masterAddress entities.Address, quorumScale float64) *RaftService {
	service := NewRaftService(state, kvRepository, logRepository, quorumScale)
	service.state.SetRole(constants.NODE_ROLE_FOLLOWER)
	master := entities.NewRaftClient(masterAddress)
	utils.LogInfo("[CREATE RAFT] masterAddress: ", masterAddress)
	utils.LogInfo("[CREATE RAFT] trying to apply membership to ", masterAddress.String())
	ctx := context.Background()
	response, err := service.caller.ApplyMembership(
		ctx,
		*master,
		dtos.RaftApplyMembershipRequest{Address: state.GetAddress()},
	)
	if err != nil {
		utils.LogErrorf("[CREATE RAFT] failed to apply membership: %v", err)
	}
	if !response.Success {
		utils.LogError("[CREATE RAFT] failed to apply membership, wrong master address")
	}

	utils.LogInfo("[CREATE RAFT] membership granted from ", masterAddress.String())

	utils.Print("Current members: ")

	for _, member := range response.Members {
		service.state.AddMember(member)
	}
	service.recalculateQuorum(state.GetMembers())

	utils.Print("\n")
	service.state.SetId(response.Id)
	defer func() {
		utils.LogInfo("[CREATE RAFT] Start listening to heartbeat")
		go service.startHeartbeatListener()
	}()
	return service
}

func (k *RaftService) Get() (*repository.KeyValueRepositoryInterface, *repository.LogRepositoryInterface) {
	return &k.kvRepository, &k.logRepository
}

func (k *RaftService) State(request dtos.RaftStateRequest) (response *dtos.RaftStateResponse) {
	stateStr, err := json.Marshal(k.state)
	if err != nil {
		utils.LogWarning("[STATE] Unable to json-encode state: ", err)
	}
	logStr, err := json.Marshal(k.logRepository.GetBuffer())
	if err != nil {
		utils.LogWarning("[STATE] Unable to json-encode log: ", err)
	}
	smStr, err := json.Marshal(k.kvRepository.GetBuffer())
	if err != nil {
		utils.LogWarning("[STATE] Unable to json-encode log: ", err)
	}

	str := "Quorum: " + strconv.Itoa(k.GetQuorum()) + "/ Preparing: "
	if k.preparing {
		str += "true"
	} else {
		str += "false"
	}

	str += " isVoting: "
	if k.state.IsVoting() {
		str += "true"
	} else {
		str += "false"
	}

	return &dtos.RaftStateResponse{
		State: []string{
			string(stateStr),
			string(logStr),
			string(smStr),
			str,
		},
	}
}

func (k *RaftService) PrepareAddMember(request dtos.RaftPrepareAddMemberRequest) (response *dtos.RaftPrepareAddMemberResponse) {
	utils.LogInfof("[PREPAREMEM] getting prepare add member request")
	success := k.state.LockMembers()
	k.preparing = success
	// if success {
	// 	time.AfterFunc(k.state.GetTwoPCTimeout(), func() {
	// 		if k.preparing {
	// 			k.state.UnlockMembers()
	// 			k.preparing = false
	// 		}
	// 	})
	// }
	if !success {
		utils.LogInfo("[PREPAREMEM] Failed to lock members")
	}

	if k.state.GetRole() != constants.NODE_ROLE_FOLLOWER {
		utils.LogError("[PREPAREMEM] Anomaly, not a follower")
	}

	k.heartbeatRcvStop <- struct{}{}
	utils.LogInfof("[PREPAREMEM] Prepare add member request, success: %v", success)
	return &dtos.RaftPrepareAddMemberResponse{
		Term:    k.state.GetCurrentTerm(),
		Success: success,
	}
}

func (k *RaftService) CommitAddMember(request dtos.RaftCommitAddMemberRequest) (response *dtos.RaftCommitAddMemberResponse) {
	utils.LogInfof("[COMMITMEM] getting prepare commit member request for " + request.Address.String())
	defer func() {
		utils.LogInfo("[COMMITMEM] Commit add member request done")
		go k.startHeartbeatListener()
	}()

	if request.Term < k.state.GetCurrentTerm() {
		utils.LogInfo("[COMMITMEM] Aborted: term < CurrentTerm")
		return &dtos.RaftCommitAddMemberResponse{
			Term:    k.state.GetCurrentTerm(),
			Success: false,
		}
	}

	if request.LeaderId != k.state.GetLeaderId() {
		utils.LogInfo("[COMMITMEM] Aborted: leaderId != LeaderId")
		return &dtos.RaftCommitAddMemberResponse{
			Term:    k.state.GetCurrentTerm(),
			Success: false,
		}
	}

	if !request.Commit {
		if k.preparing {
			k.state.UnlockMembers()
		}
		k.preparing = false
		utils.LogInfo("[COMMITMEM] Aborted: commit == false")
		return &dtos.RaftCommitAddMemberResponse{
			Term:    k.state.GetCurrentTerm(),
			Success: true,
		}
	}

	if !k.preparing {
		utils.LogInfo("[COMMITMEM] Not preparing, abort")
		return &dtos.RaftCommitAddMemberResponse{
			Term:    k.state.GetCurrentTerm(),
			Success: false,
		}
	}

	utils.LogInfo("[COMMITMEM] Committing add member ", request.Address.String())

	defer k.state.UnlockMembers()
	k.preparing = false
	k.state.AddMember(request.Address)
	k.recalculateQuorum(k.state.GetMembersWOLock())
	return &dtos.RaftCommitAddMemberResponse{
		Term:    k.state.GetCurrentTerm(),
		Success: true,
	}
}

func (k *RaftService) AppendEntries(request dtos.RaftAppendEntriesRequest) (response *dtos.RaftAppendEntriesResponse) {
	k.heartbeatRcvReset <- struct{}{}
	utils.LogInfo("[APPEND] getting append request from ", k.state.GetMembers()[request.LeaderId].Address.String())
	jsonStr, err := json.MarshalIndent(request, "", "    ")
	if err != nil {
		utils.LogInfo("[APPEND] Unable to json-encode request: ", err)
	} else {
		utils.LogInfo("[APPEND] Request: ", string(jsonStr))
	}

	/* Reply false from prev term's leader */
	if request.Term < k.state.GetCurrentTerm() {
		utils.LogInfo("[APPEND] Aborted: term < CurrentTerm")
		return &dtos.RaftAppendEntriesResponse{
			Term:    k.state.GetCurrentTerm(),
			Success: false,
		}
	}

	/*
		ASSUMPTION: No other leader within the same term
	*/
	if request.LeaderId != k.state.GetLeaderId() {
		if request.Term == k.state.GetCurrentTerm() {
			k.state.SetLeaderId(request.LeaderId)
		}

		/* if was a leader */
		if k.state.GetRole() == constants.NODE_ROLE_LEADER {
			utils.LogInfo("[APPEND] Was a leader, return to follower")
			k.state.SetRole(constants.NODE_ROLE_FOLLOWER)
			k.heartbeatSendStop <- struct{}{}
			go k.startHeartbeatListener()
		}

	}
	k.state.SetCurrentTerm(request.Term)
	k.state.SetVotedFor(constants.INVALID_ID)
	k.state.SetLeaderId(request.LeaderId)
	k.state.SetIsVoting(false)
	/*
		Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
		Leader must retry with lower index.
	*/
	if request.PrevLogIndex != constants.INVALID_ID {
		log, success := k.logRepository.Get(request.PrevLogIndex)
		if !success || log.Term != request.PrevLogTerm { /* Ensure raft invariant assumption */

			utils.LogInfo("[APPEND] Aborted: log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm")
			return &dtos.RaftAppendEntriesResponse{
				Term:    k.state.GetCurrentTerm(),
				Success: false,
			}
		}
	}

	/* Append new entries */
	for _, newLog := range request.Entries {
		k.logRepository.Set(newLog.Index, newLog)
		// if !(idx == newLog.Index) {
		// 	utils.LogError("[APPEND] Aborted: log replication failed (index mismatch)")
		// }
		if !(newLog.Term == request.Term) {
			utils.LogError("[APPEND] Aborted: log replication failed (term mismatch)")
		}
	}

	// /* If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it */
	// for _, newLog := range request.Entries {
	// 	existingLog, indexExists := k.logRepository.Get(newLog.Index)
	// 	if indexExists {
	// 		/*	Conflict detected: same index but different term */
	// 		if existingLog.Term != newLog.Term {
	// 			/* Delete all log following it */
	// 			k.logRepository.DeleteLogsStartFrom(newLog.Index)
	// 			/* Append new log */
	// 			k.logRepository.Append(newLog)
	// 		}
	// 	} else {
	// 		k.logRepository.Append(newLog)
	// 	}
	// }

	isSorted := true
	for i := 1; i < len(request.Entries); i++ {
		if request.Entries[i-1].Index > request.Entries[i].Index {
			isSorted = false
			break
		}
	}
	if !isSorted {
		utils.LogError("[HEARTBEAT] Error: entries not sorted")
	}

	/* ASSUMPTION:  Logs from request are sorted */
	if k.state.GetCommitIndex() < request.LeaderCommit {
		/* Execute every command from commitIndex + 1 to leaderCommit index */
		for i := k.state.GetCommitIndex() + 1; i <= request.LeaderCommit; i++ {
			log, found := k.logRepository.Get(i)
			if (log.Term != request.Term) || (!found) {
				continue
			}
			if log.Command == constants.INVALID_COMMAND {
				continue
			}
			command, arg1, arg2 := log.Parse()
			utils.LogInfo("[COMMIT] Executing command: ", command, " ", arg1, " ", arg2)
			if command == "mem" { /* Add Member */
				k.state.AddMember(entities.Address{Host: arg1, Port: arg2})
				k.recomputeQuorum()
			} else { /* Commit state machine change */
				k.kvRepository.Execute(command, arg1, arg2)
			}
			k.state.SetCommitIndex(max((log.Index), k.state.GetCommitIndex()))
		}
	}
	logExists := len(request.Entries) > 0
	if logExists {
		lastLogRequest := request.Entries[len(request.Entries)-1]
		if k.state.GetCommitIndex() != min(lastLogRequest.Index, request.LeaderCommit) {
			utils.LogError("[APPEND] Aborted: commitIndex mismatch")
		}
	}

	utils.LogInfo("[APPEND] Agree to append entries")
	utils.LogInfo(k.logRepository.GetLastIndex())
	return &dtos.RaftAppendEntriesResponse{
		Term:    k.state.GetCurrentTerm(),
		Success: true,
	}
}

func (k *RaftService) RequestVote(request dtos.RaftRequestVoteRequest) (response *dtos.RaftRequestVoteResponse) {
	k.heartbeatRcvReset <- struct{}{}

	utils.LogInfo("[VOTE] getting vote request from candidate: ", request.CandidateId, "(", k.state.GetMembers()[request.CandidateId].Address.String(), ")", " term: ", request.Term)
	jsonStr, err := json.MarshalIndent(request, "", "    ")
	if err != nil {
		utils.LogInfo("[VOTE] Unable to json-encode request: ", err)
	} else {
		utils.LogInfo("[VOTE] Request: ", string(jsonStr))
	}

	/* Reply false if term <= CurrentTerm */
	if request.Term <= k.state.GetCurrentTerm() {
		utils.LogInfo("[VOTE] Reply false to: ", request.CandidateId, "(", k.state.GetMembers()[request.CandidateId].Address.String(), ")", " term: ", request.Term, " (term <= CurrentTerm)")
		return &dtos.RaftRequestVoteResponse{
			Term:        k.state.GetCurrentTerm(),
			VoteGranted: false,
		}
	}

	k.state.SetCurrentTerm(request.Term)
	lastLog, found := k.logRepository.GetLastLog()

	/*  Reply false if candidate's log is not as up-to-date as the receiver's log */
	if found && (request.LastLogTerm < lastLog.Term ||
		request.LastLogIndex < lastLog.Index) {
		utils.LogInfo("[VOTE] Reply false to: ", request.CandidateId, "(", k.state.GetMembers()[request.CandidateId].Address.String(), ")", " term: ", request.Term, " (log not up-to-date)")
		return &dtos.RaftRequestVoteResponse{
			Term:        k.state.GetCurrentTerm(),
			VoteGranted: false,
		}
	}

	/* Reply false if not VotedFor is null or candidateId log */
	if k.state.GetVotedFor() != constants.INVALID_ID && k.state.GetVotedFor() != request.CandidateId {
		utils.LogInfo("[VOTE] Reply false to: ", request.CandidateId, "(", k.state.GetMembers()[request.CandidateId].Address.String(), ")", " term: ", request.Term)
		return &dtos.RaftRequestVoteResponse{
			Term:        k.state.GetCurrentTerm(),
			VoteGranted: false,
		}
	}

	k.state.SetVotedFor(request.CandidateId)
	utils.LogInfo("[VOTE] Reply true to: ", request.CandidateId, "(", k.state.GetMembers()[request.CandidateId].Address.String(), ")", " term: ", request.Term)
	return &dtos.RaftRequestVoteResponse{
		Term:        k.state.GetCurrentTerm(),
		VoteGranted: true,
	}
}

func (k *RaftService) ApplyMembership(request dtos.RaftApplyMembershipRequest) (response *dtos.RaftApplyMembershipResponse) {
	utils.LogInfo("[MEMBERSHIP] getting membership request from ", request.Address.String())

	if k.state.IsVoting() { /* if election is happening, reject request */
		utils.LogInfo("[MEMBERSHIP] Voting is happening")
		return &dtos.RaftApplyMembershipResponse{
			Success: false,
		}
	}

	if k.state.GetRole() != constants.NODE_ROLE_LEADER { /* If not a leader, TODO: REDIRECT*/
		utils.LogInfo("[MEMBERSHIP] Not a leader")
		return &dtos.RaftApplyMembershipResponse{
			Success: false,
		}
	}

	exist := false
	members := *k.state.GetMembersPtr()
	for id := range members {
		member := &members[id]
		if member.Address.Host == request.Address.Host && member.Address.Port == request.Address.Port {
			// Update the member with the pointer returned by NewRaftClient
			*member = *entities.NewRaftClient(request.Address)
			exist = true
			break
		}
	}

	if exist { /* if member already exist, just return success */
		memberAddresses := []entities.Address{}
		for _, member := range k.state.GetMembers() {
			memberAddresses = append(memberAddresses, member.Address)
		}
		utils.LogInfo("[MEMBERSHIP] Member already exist")
		return &dtos.RaftApplyMembershipResponse{
			Success: true,
			Members: memberAddresses,
		}
	}

	k.heartbeatSendStop <- struct{}{}
	defer func() {
		utils.LogInfo("[MEMBERSHIP] Start listening to heartbeat")
		go k.startHeartbeatEmitter()
	}()
	k.state.LockMembers()
	defer k.state.UnlockMembers()
	wg := sync.WaitGroup{}
	acked := []entities.RaftClient{}
	mu := sync.Mutex{}
	for id, member := range k.state.GetMembersWOLock() {
		if id == k.state.GetId() {
			continue
		}
		wg.Add(1)
		go func(member entities.RaftClient) {
			defer wg.Done()
			context, cancel := context.WithTimeout(context.Background(), k.state.GetTwoPCTimeout())
			defer cancel()
			resp, err := k.caller.PrepareAddMember(
				context,
				member,
				dtos.RaftPrepareAddMemberRequest{
					Term:     k.state.GetCurrentTerm(),
					LeaderId: k.state.GetId(),
				},
			)
			if err != nil {
				utils.LogWarningf("Error sending PrepareAddMember to member %v: %v\n", member, err)
				return
			}
			if resp.Success {
				mu.Lock()
				acked = append(acked, member)
				mu.Unlock()
			}
		}(member)
	}
	wg.Wait()

	if len(acked) != len(k.state.GetMembersWOLock())-1 { /* if not all members acked, reject request */
		utils.LogInfo("[MEMBERSHIP] Not all members acked, rolling back")
		wg2 := sync.WaitGroup{}
		for id, member := range acked {
			if id == k.state.GetId() {
				continue
			}
			utils.LogInfo("[MEMBERSHIP] Rollback membership to ", member.Address.String())
			wg2.Add(1)
			go func(member entities.RaftClient) {
				defer wg2.Done()
				context := context.Background()
				resp, err := k.caller.CommitAddMember(
					context,
					member,
					dtos.RaftCommitAddMemberRequest{
						Term:     k.state.GetCurrentTerm(),
						LeaderId: k.state.GetId(),
						Commit:   false,
						Address:  request.Address,
					},
				)
				if err != nil {
					utils.LogWarningf("Error sending CommitAddMember to member %v: %v\n", member, err)
					return
				}
				if resp.Success {
					mu.Lock()
					acked = append(acked, member)
					mu.Unlock()
				}
			}(member)
		}
		wg2.Wait()
		utils.LogInfo("[MEMBERSHIP] Rollback membership done")
		return &dtos.RaftApplyMembershipResponse{
			Success: false,
		}
	}

	wg3 := sync.WaitGroup{}
	utils.LogInfo("[MEMBERSHIP] Commit membership ", request.Address.String())
	for _, member := range acked {
		wg3.Add(1)
		go func(member entities.RaftClient) {
			defer wg3.Done()
			context := context.Background()
			resp, err := k.caller.CommitAddMember(
				context,
				member,
				dtos.RaftCommitAddMemberRequest{
					Term:     k.state.GetCurrentTerm(),
					LeaderId: k.state.GetId(),
					Commit:   true,
					Address:  request.Address,
				},
			)
			if err != nil {
				utils.LogWarningf("Error sending CommitAddMember to member %v: %v\n", member, err)
				return
			}
			if resp.Success {
				mu.Lock()
				acked = append(acked, member)
				mu.Unlock()
			}
		}(member)
	}
	wg3.Wait()

	k.state.AddMember(request.Address)
	k.recalculateQuorum(k.state.GetMembersWOLock())

	memberAddresses := []entities.Address{}
	for _, member := range k.state.GetMembersWOLock() {
		memberAddresses = append(memberAddresses, member.Address)
	}

	utils.LogInfo("[MEMBERSHIP] Membership granted")

	return &dtos.RaftApplyMembershipResponse{
		Id:      len(k.state.GetMembersWOLock()) - 1,
		Members: memberAddresses,
		Term:    k.state.GetCurrentTerm(),
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
			k.appendEntriesToAllMembers()
			time.Sleep(k.state.GetHeartbeatInterval())
		}
	}
}

func (k *RaftService) startHeartbeatListener() {

	timer := time.NewTimer(k.state.GetHeartbeatTimeout())
	defer timer.Stop()

	for {
		select {
		case <-k.heartbeatRcvStop:
			utils.LogInfo("[HEARTBEAT] Stop listening to heartbeat")
			return
		case <-timer.C:
			utils.LogInfo("[HEARTBEAT] Heartbeat timeout, start voting...")
			go k.initializeVoting()
			timer.Reset(k.state.GetHeartbeatTimeout())
		case <-k.heartbeatRcvReset:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(k.state.GetHeartbeatTimeout())
		}
	}
}

func (k *RaftService) AppendEntriesToAllMembers() int {
	return k.appendEntriesToAllMembers()
}

func (k *RaftService) appendEntriesToAllMembers() int {
	var wg sync.WaitGroup
	countAgree := 1
	var mutex sync.Mutex
	membersPtr := k.state.GetMembersPtr()
	for id := range *membersPtr {
		if id == k.state.GetId() {
			utils.LogInfo("[HEARTBEAT] Skip sending heartbeat to self")
			continue
		}

		member := &(*membersPtr)[id]
		prevLog, found := k.logRepository.Get(member.NextIndex - 1)

		prevLogIndex := constants.INVALID_ID
		prevLogTerm := constants.INVALID_ID
		if found {
			prevLogIndex = prevLog.Index
			prevLogTerm = prevLog.Term
		}

		utils.LogInfo("[HEARTBEAT] Sending heartbeat to ", member.Address.String(), "...")

		entries := k.logRepository.GetLogsWhere(k.state.GetCurrentTerm(), member.NextIndex)
		isSorted := true
		for i := 1; i < len(entries); i++ {
			if entries[i-1].Index > entries[i].Index {
				isSorted = false
				break
			}
		}
		if !isSorted {
			utils.LogError("[HEARTBEAT] Error: entries not sorted")
		}

		wg.Add(1)
		go func(member *entities.RaftClient) {
			defer wg.Done()
			context, cancel := context.WithTimeout(context.Background(), k.state.GetHeartbeatRequestTimeout())
			defer cancel()
			resp, err := k.caller.AppendEntries(
				context,
				*member,
				dtos.RaftAppendEntriesRequest{
					Term:         k.state.GetCurrentTerm(),
					LeaderId:     k.state.GetId(),
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: k.state.GetCommitIndex(),
				})
			if err != nil {
				utils.LogWarningf("Error sending AppendEntries to member %v: %v\n", member, err)
				return
			}

			if !resp.Success {
				if resp.Term > k.state.GetCurrentTerm() {
					k.state.SetCurrentTerm(resp.Term)
					if k.state.GetRole() != constants.NODE_ROLE_FOLLOWER {
						utils.LogInfo("[HEARTBEAT] Getting invalid response from higher term, return to follower", resp.Term)
						k.state.SetRole(constants.NODE_ROLE_FOLLOWER)
						k.heartbeatSendStop <- struct{}{}
						go k.startHeartbeatListener()
					}
				} else {
					member.NextIndex--
				}
				return
			}

			mutex.Lock()
			countAgree++
			mutex.Unlock()
			anyEntriesSent := len(entries) > 0
			if anyEntriesSent {
				member.NextIndex = entries[len(entries)-1].Index + 1
			}
		}(member)
	}
	wg.Wait()
	return countAgree
}

func (k *RaftService) sendVoteRequestToAllMembers() int {
	var wg sync.WaitGroup
	countAgree := 0
	var mutex sync.Mutex
	for id, member := range *k.state.GetMembersPtr() {
		if id == k.state.GetId() {
			continue
		}
		wg.Add(1)
		go func(member entities.RaftClient) {
			defer wg.Done()

			lastLog, found := k.logRepository.GetLastLog()
			var lastLogIndex int
			var lastLogTerm int
			if !found {
				lastLogIndex = constants.INVALID_ID
				lastLogTerm = constants.INVALID_TERM
			} else {
				lastLogIndex = lastLog.Index
				lastLogTerm = lastLog.Term
			}

			context, cancel := context.WithTimeout(context.Background(), k.state.GetElectionTimeout())
			defer cancel()
			resp, err := k.caller.RequestVote(
				context,
				member,
				dtos.RaftRequestVoteRequest{
					Term:         k.state.GetCurrentTerm(),
					CandidateId:  k.state.GetId(),
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				})

			if err != nil {
				utils.LogWarningf("Error sending AppendEntries to member %v: %v\n", member, err)
				return
			}

			if !resp.VoteGranted {
				if resp.Term < k.state.GetCurrentTerm() { /*Invalid response*/
					utils.LogInfo("[ELECTION] Getting invalid response from lower term")
				} else if resp.Term == k.state.GetCurrentTerm() { /*If recepient already vote for others*/
					return
				} else { /* Recepient already on higher term*/
					if k.state.GetRole() != constants.NODE_ROLE_FOLLOWER {
						utils.LogInfo("[ELECTION] Receive response from higher term, return to follower")
						k.state.SetCurrentTerm(resp.Term)
						k.state.SetRole(constants.NODE_ROLE_FOLLOWER)
						k.heartbeatSendStop <- struct{}{}
						go k.startHeartbeatListener()
						return
					}
				}
			}
			if resp.VoteGranted {
				mutex.Lock()
				countAgree++
				mutex.Unlock()
			}
		}(member)
	}
	wg.Wait()
	return countAgree
}

func (k *RaftService) initializeVoting() {
	currentTerm := k.state.GetCurrentTerm()
	k.state.SetCurrentTerm(currentTerm + 1)
	k.state.SetIsVoting(true)
	defer func() {
		if k.state.GetCurrentTerm() == currentTerm+1 {
			k.state.SetIsVoting(false)
		}
	}()
	k.state.SetVotedFor(k.state.GetId())
	k.state.SetRole(constants.NODE_ROLE_CANDIDATE)
	utils.Printf("[ELECTION] Initialize voting, term: %d", k.state.GetCurrentTerm())
	countAgree := 1 + k.sendVoteRequestToAllMembers()

	utils.Println("[ELECTION] count agree: ", countAgree)

	if countAgree < k.GetQuorum() {
		if k.state.GetRole() != constants.NODE_ROLE_FOLLOWER {
			utils.Println("[ELECTION] count agree < majority, return to follower", countAgree)
			k.state.SetRole(constants.NODE_ROLE_FOLLOWER)
			k.heartbeatSendStop <- struct{}{}
			go k.startHeartbeatListener()
		}
		return
	}

	if k.state.GetRole() == constants.NODE_ROLE_FOLLOWER {
		utils.Print("[ELECTION] Already a follower, election aborted")
		return
	}
	utils.Println("[ELECTION] count agree > majority, start serving as a leader", countAgree)
	k.state.SetRole(constants.NODE_ROLE_LEADER)
	k.state.SetLeaderId(k.state.GetId())
	k.heartbeatRcvStop <- struct{}{}
	go k.startHeartbeatEmitter()
}
func (k *RaftService) recomputeQuorum() {
	k.quorum = int(float64(len(k.state.GetMembers()))*k.quorumScale) + 1
}

func (k *RaftService) recalculateQuorum(members []entities.RaftClient) {
	k.quorum = int(float64(len(members))*k.quorumScale) + 1
}

func (k *RaftService) GetQuorum() int {
	return k.quorum
}
