package services

import (
	"if3230-tubes-williamjeffersonclinton/internal/domain/clients"
	"if3230-tubes-williamjeffersonclinton/internal/domain/constants"
	"if3230-tubes-williamjeffersonclinton/internal/domain/dtos"
	entity "if3230-tubes-williamjeffersonclinton/internal/domain/entities"
	repository "if3230-tubes-williamjeffersonclinton/internal/domain/repository"
	"if3230-tubes-williamjeffersonclinton/internal/utils"

	"context"
	"math/rand"
	"sync"
	"time"
)

const NO_VOTE = -1
const INVALID_ID = -1
const INVALID_TERM = -1

type RaftService struct {
	Id       int
	Members  []clients.RaftClient
	Address  entity.Address
	LeaderId int

	// Persistent State
	CurrentTerm   int
	VotedFor      int
	logRepository repository.LogRepositoryInterface
	kvRepository  repository.KeyValueRepositoryInterface

	// Volatile State
	CommitIndex int
	LastApplied int

	// Volatile State on Leaders
	Role      int
	roleMutex sync.Mutex // TODO: Implement mutex for role

	NextIndex  map[clients.RaftClient]int
	MatchIndex map[clients.RaftClient]int

	// Time and Channels
	ElectionTimeout time.Duration

	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	heartbeatRcvStop  chan struct{}
	heartbeatRcvReset chan struct{}
	heartbeatSendStop chan struct{}
}

func NewRaftService(address entity.Address, kvRepository repository.KeyValueRepositoryInterface, logRepository repository.LogRepositoryInterface) *RaftService {
	electionTimeout := rand.Intn(150) + 150
	heartbeatTimeout := 3000 + rand.Intn(500)
	return &RaftService{
		Id:       0,
		Members:  []clients.RaftClient{},
		Address:  address,
		LeaderId: INVALID_ID,

		CurrentTerm:   INVALID_TERM,
		VotedFor:      NO_VOTE,
		logRepository: logRepository,
		kvRepository:  kvRepository,

		CommitIndex: 0,
		LastApplied: 0,

		Role:       constants.NODE_ROLE_FOLLOWER,
		NextIndex:  make(map[clients.RaftClient]int),
		MatchIndex: make(map[clients.RaftClient]int),

		ElectionTimeout:   time.Duration(electionTimeout) * time.Millisecond,
		HeartbeatInterval: time.Duration(3000) * time.Millisecond,
		HeartbeatTimeout:  time.Duration(heartbeatTimeout) * time.Millisecond,
		heartbeatRcvStop:  make(chan struct{}),
		heartbeatRcvReset: make(chan struct{}),
		heartbeatSendStop: make(chan struct{}),
	}
}

func NewRaftLeaderService(address entity.Address, kvRepository repository.KeyValueRepositoryInterface, logRepository repository.LogRepositoryInterface) *RaftService {
	electionTimeout := rand.Intn(150) + 150
	heartbeatTimeout := 3000 + rand.Intn(500)

	member := clients.NewRaftClient(address)

	service := &RaftService{
		Id:       0,
		Members:  []clients.RaftClient{*member},
		Address:  address,
		LeaderId: 0,

		CurrentTerm:   0,
		VotedFor:      NO_VOTE,
		logRepository: logRepository,
		kvRepository:  kvRepository,

		CommitIndex: INVALID_ID,
		LastApplied: INVALID_ID,

		Role:       constants.NODE_ROLE_LEADER,
		NextIndex:  make(map[clients.RaftClient]int),
		MatchIndex: make(map[clients.RaftClient]int),

		ElectionTimeout:   time.Duration(electionTimeout) * time.Millisecond,
		HeartbeatInterval: time.Duration(3000) * time.Millisecond,
		HeartbeatTimeout:  time.Duration(heartbeatTimeout) * time.Millisecond,
		heartbeatRcvStop:  make(chan struct{}),
		heartbeatRcvReset: make(chan struct{}),
		heartbeatSendStop: make(chan struct{}),
	}
	go service.StartHeartbeatSender()
	return service
}

func NewRaftFollowerService(address entity.Address, kvRepository repository.KeyValueRepositoryInterface, logRepository repository.LogRepositoryInterface, masterAddress entity.Address) *RaftService {
	master := clients.NewRaftClient(masterAddress)

	utils.LogInfo("[CREATE RAFT] trying to apply membership to ", master.Address.String())
	response, err := master.ApplyMembership(context.Background(), dtos.RaftApplyMembershipRequest{Address: address})
	if err != nil {
		utils.LogError("[CREATE RAFT] failed to apply membership: %v", err)
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

	electionTimeout := rand.Intn(150) + 150
	heartbeatTimeout := 3000 + rand.Intn(500)

	service := &RaftService{
		Id:      response.Id,
		Members: members,
		Address: address,

		CurrentTerm: response.Term,

		VotedFor: NO_VOTE,

		logRepository: logRepository,
		kvRepository:  kvRepository,

		CommitIndex: INVALID_ID,
		LastApplied: INVALID_ID,

		Role:       constants.NODE_ROLE_FOLLOWER,
		NextIndex:  make(map[clients.RaftClient]int),
		MatchIndex: make(map[clients.RaftClient]int),

		ElectionTimeout: time.Duration(electionTimeout) * time.Millisecond,

		HeartbeatInterval: time.Duration(3000) * time.Millisecond,
		HeartbeatTimeout:  time.Duration(heartbeatTimeout) * time.Millisecond,
		heartbeatRcvStop:  make(chan struct{}),
		heartbeatRcvReset: make(chan struct{}),
		heartbeatSendStop: make(chan struct{}),
	}

	go service.StartHeartbeatListener()
	return service
}

// REQUEST MASUK
func (k *RaftService) AppendEntries(request dtos.RaftAppendEntriesRequest) (response *dtos.RaftAppendEntriesResponse) {
	k.heartbeatRcvReset <- struct{}{}
	utils.LogInfo("[APPEND] getting append request from ", k.Members[request.LeaderId].Address.String())

	if request.LeaderId != k.LeaderId {

		k.LeaderId = request.LeaderId
		k.VotedFor = INVALID_ID
	}
	/* Reply false if term < CurrentTerm */
	if request.Term < k.CurrentTerm {
		utils.LogInfo("[APPEND] Aborted: term < CurrentTerm")
		return &dtos.RaftAppendEntriesResponse{
			Term:    k.CurrentTerm,
			Success: false,
		}
	}

	
	/* 
		Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
		Leader must retry with lower index.
	*/
	if request.PrevLogIndex != INVALID_ID {
		log, success := k.logRepository.Get(request.PrevLogIndex)
		if !success || log.Term != request.PrevLogTerm {
			utils.LogInfo("[APPEND] Aborted: log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm")
			return &dtos.RaftAppendEntriesResponse{
				Term:    k.CurrentTerm,
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
				// Delete the existing entry and all that follow it
				k.logRepository.DeleteLogsFromIndex(newLog.Index)
				// Append the new log
				k.logRepository.Append(newLog)

				//  If leaderCommit > CommitIndex, set CommitIndex = min(leaderCommit, index of last new entry)
				if request.LeaderCommit > k.CommitIndex {
					k.CommitIndex = min(request.LeaderCommit, newLog.Index)
				}
			}
		} else {
			// No conflict, append the new log
			k.logRepository.Append(newLog)

			//  If leaderCommit > CommitIndex, set CommitIndex = min(leaderCommit, index of last new entry)
			if request.LeaderCommit > k.CommitIndex {
				k.CommitIndex = min(request.LeaderCommit, newLog.Index)
			}
		}
	}

	utils.LogInfo("[APPEND] Agree to append entries")
	return &dtos.RaftAppendEntriesResponse{
		Term:    k.CurrentTerm,
		Success: true,
	}
}

// REQUEST MASUK
func (k *RaftService) RequestVote(request dtos.RaftRequestVoteRequest) (response *dtos.RaftRequestVoteResponse) {
	k.heartbeatRcvStop <- struct{}{}

	utils.LogInfo("[VOTE] getting vote request from candidate ", request.CandidateId, "(", k.Members[request.CandidateId].Address.String(), ")")
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

	k.VotedFor = request.CandidateId
	return &dtos.RaftRequestVoteResponse{
		Term:        k.CurrentTerm,
		VoteGranted: true,
	}
}

// REQUEST MASUK
func (k *RaftService) ApplyMembership(request dtos.RaftApplyMembershipRequest) (response *dtos.RaftApplyMembershipResponse) {
	utils.LogInfo("[MEMBERSHIP] getting membership request from ", request.Address.String())
	utils.LogInfo("[MEMBERSHIP] membership granted to ", request.Address.String())

	// TODO: CHECK IF MASTER
	k.Members = append(k.Members, *clients.NewRaftClient(request.Address))

	memberAddresses := []entity.Address{}
	utils.Print("Current members: ")
	for _, member := range k.Members {
		utils.Print(member.Address.String(), ",")
		memberAddresses = append(memberAddresses, member.Address)
	}
	utils.Print("\n")

	return &dtos.RaftApplyMembershipResponse{
		Id:      len(k.Members) - 1,
		Members: memberAddresses,
		Term:    k.CurrentTerm,
		Success: true,
	}
}

func (k *RaftService) StartHeartbeatSender() {
	for {
		select {
		case <-k.heartbeatSendStop:
			return
		default:
			utils.LogInfo("[HEARTBEAT] Sending heartbeat...")
			k.PropagateEntries()
			time.Sleep(k.HeartbeatInterval)
		}
	}
}

func (k *RaftService) StartHeartbeatListener() {

	timer := time.NewTimer(k.HeartbeatTimeout)
	defer timer.Stop()

	for {
		select {
		case <-k.heartbeatRcvStop:
			utils.LogInfo("[HEARTBEAT] Stop listening to heartbeat")
			return
		case <-timer.C:
			utils.LogInfo("[HEARTBEAT] Heartbeat timeout, start voting...")
			k.InitializeVoting()
			timer.Reset(k.HeartbeatTimeout)
			return
		case <-k.heartbeatRcvReset:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(k.HeartbeatTimeout)
		}
	}
}

// REQUEST KELUAR
func (k *RaftService) PropagateEntries() {
	var wg sync.WaitGroup
	countAgree := 0
	var mutex sync.Mutex

	log, found := k.logRepository.GetLastLog()

	prevLogIndex := INVALID_ID
	if found {
		prevLogIndex = log.Index
	}
	
	for id, member := range k.Members {
		if id == k.Id {
			continue
		}

		wg.Add(1)
		go func(member clients.RaftClient) {
			defer wg.Done()

			resp, err := member.AppendEntries(context.Background(), dtos.RaftAppendEntriesRequest{
				Term:         k.CurrentTerm,
				LeaderId:     k.Id,
				PrevLogIndex: prevLogIndex,
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

// REQUEST KELUAR
func (k *RaftService) InitializeVoting() {
	utils.Println("[ELECTION] Initialize voting")
	k.CurrentTerm++
	k.VotedFor = k.Id
	k.Role = constants.NODE_ROLE_CANDIDATE

	var wg sync.WaitGroup
	countAgree := 1
	var mutex sync.Mutex
	for id, member := range k.Members {
		if id == k.Id {
			continue
		}
		wg.Add(1)
		go func(member clients.RaftClient) {
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
			resp, err := member.RequestVote(context.Background(), dtos.RaftRequestVoteRequest{
				Term:         k.CurrentTerm,
				CandidateId:  k.Id,
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
	if countAgree > len(k.Members)/2 {
		utils.Println("[ELECTION] count agree > majority, start serving as a leader", countAgree)
		go k.StartHeartbeatSender()
	} else {
		utils.Println("[ELECTION] count agree < majority, return to follower", countAgree)
		go k.StartHeartbeatListener()
	}

	// if k.Role == constants.NODE_ROLE_CANDIDATE {
	// 	k.Role
}
