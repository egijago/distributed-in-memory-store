package entities

import (
	"math/rand"
	"sync"
	"time"

	"if3230-tubes-williamjeffersonclinton/internal/domain/constants"
)

type StateInterface interface {
	AddMember(address Address) (success bool)
	RemoveMember(address Address) (success bool)
	SetRole(role int)
	GetRole() int
	SetCurrentTerm(term int)
	GetCurrentTerm() int
	SetVotedFor(vote int)
	GetVotedFor() int
	GetLeaderId() int
	SetLeaderId(leaderId int)
	GetAddress() Address
	SetAddress(address Address)
	GetId() int
	SetId(id int)
	GetCommitIndex() int
	SetCommitIndex(index int)
	GetLastApplied() int
	SetLastApplied(index int)
	GetMembers() []RaftClient
	GetMembersPtr() *[]RaftClient
	SetMembers(members []RaftClient)
	GetElectionTimeout() time.Duration
	GetHeartbeatInterval() time.Duration
	GetHeartbeatTimeout() time.Duration
	GetHeartbeatRequestTimeout() time.Duration
	IsVoting() bool
	SetIsVoting(isVoting bool)
	GetTwoPCTimeout() time.Duration
	LockMembers() bool
	UnlockMembers()
	GetMembersWOLock() []RaftClient
	RecalculateHeartbeatTimeout()
}

type State struct {
	Id       int
	Members  []RaftClient
	Address  Address
	LeaderId int

	// Persistent State
	CurrentTerm int
	VotedFor    int

	// Volatile State
	CommitIndex int
	LastApplied int

	// Volatile State on Leaders
	Role int

	ElectionTimeout time.Duration

	HeartbeatInterval       time.Duration
	HeartbeatTimeout        time.Duration
	heartbeatRequestTimeout time.Duration

	/* base */
	heartbeatTimeoutSlack int
	heartbeatTimeout      int

	TwoPCTimeout time.Duration

	isVoting bool

	// Mutexes
	roleMutex        sync.RWMutex
	currentTermMutex sync.RWMutex
	votedForMutex    sync.RWMutex
	isVotingMutex    sync.RWMutex
	memberMutex      sync.RWMutex
}

func NewState(address Address, electionTimeout int, heartbeatInterval int, heartbeatRequestTimeout int, heartbeatTimeout int, heartbeatTimeoutSlack int, twoPCTimeout int) *State {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	factor := rand.Float64()
	heartbeatTimeout = heartbeatTimeout + int(float64(heartbeatTimeoutSlack)*factor)
	return &State{
		Id:       0,
		Members:  []RaftClient{},
		Address:  address,
		LeaderId: constants.INVALID_ID,

		CurrentTerm: constants.INVALID_TERM,
		VotedFor:    constants.NO_VOTE,

		CommitIndex: constants.INVALID_ID,
		LastApplied: constants.INVALID_ID,

		Role: constants.NODE_ROLE_FOLLOWER,

		ElectionTimeout:         time.Duration(electionTimeout) * time.Millisecond,
		HeartbeatInterval:       time.Duration(heartbeatInterval) * time.Millisecond,
		HeartbeatTimeout:        time.Duration(heartbeatTimeout) * time.Millisecond,
		heartbeatRequestTimeout: time.Duration(heartbeatRequestTimeout) * time.Millisecond,
		TwoPCTimeout:            time.Duration(twoPCTimeout) * time.Millisecond,
		heartbeatTimeoutSlack:   heartbeatTimeoutSlack,
		heartbeatTimeout:        heartbeatTimeout,
	}
}

func (k *State) RecalculateHeartbeatTimeout() {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	factor := rand.Float64()
	k.HeartbeatTimeout = time.Duration(k.heartbeatTimeout+int(float64(k.heartbeatTimeoutSlack)*factor)) * time.Millisecond
}

func (k *State) GetTwoPCTimeout() time.Duration {
	return k.TwoPCTimeout
}

func (k *State) GetHeartbeatRequestTimeout() time.Duration {
	return k.heartbeatRequestTimeout
}

func (k *State) GetElectionTimeout() time.Duration {
	return k.ElectionTimeout
}

func (k *State) GetHeartbeatInterval() time.Duration {
	return k.HeartbeatInterval
}

func (k *State) GetHeartbeatTimeout() time.Duration {
	return k.HeartbeatTimeout
}

func (k *State) LockMembers() bool {
	locked := k.memberMutex.TryLock()
	return locked
}

func (k *State) UnlockMembers() {
	k.memberMutex.Unlock()
}

func (k *State) GetMembers() []RaftClient {
	k.memberMutex.Lock()
	defer k.memberMutex.Unlock()
	return k.Members
}

func (k *State) GetMembersWOLock() []RaftClient {
	return k.Members
}

func (k *State) GetMembersPtr() *[]RaftClient {
	k.memberMutex.Lock()
	defer k.memberMutex.Unlock()
	return &k.Members
}

func (k *State) SetMembers(members []RaftClient) {
	k.memberMutex.Lock()
	defer k.memberMutex.Unlock()
	k.Members = members
}

func (k *State) GetLastApplied() int {
	return k.LastApplied
}

func (k *State) SetLastApplied(index int) {
	k.LastApplied = index
}

func (k *State) GetCommitIndex() int {
	return k.CommitIndex
}

func (k *State) SetCommitIndex(index int) {
	k.CommitIndex = index
}

func (k *State) GetId() int {
	return k.Id
}

func (k *State) SetId(id int) {
	k.Id = id
}

func (k *State) AddMember(address Address) (success bool) {
	for id, member := range k.Members {
		if member.Address.Host == address.Host && member.Address.Port == address.Port {
			newMember := NewRaftClient(*NewAddress(address.Host, address.Port))
			k.Members[id] = *newMember
			return true // ensure idempotence, regenerate known state
		}
	}
	newMember := NewRaftClient(*NewAddress(address.Host, address.Port))
	k.Members = append(k.Members, *newMember)
	return true
}

func (k *State) RemoveMember(address Address) (success bool) {
	for id, member := range k.Members {
		if member.Address.Host == address.Host && member.Address.Port == address.Port {
			k.Members = append(k.Members[:id], k.Members[id+1:]...)
			return true
		}
	}
	return false
}

func (k *State) SetRole(role int) {
	k.roleMutex.Lock()
	k.Role = role
	k.roleMutex.Unlock()
}

func (k *State) GetRole() int {
	k.roleMutex.RLock()
	defer k.roleMutex.RUnlock()
	return k.Role
}

func (k *State) SetCurrentTerm(term int) {
	k.currentTermMutex.Lock()
	if term != k.CurrentTerm {
		k.SetVotedFor(constants.NO_VOTE)
		k.RecalculateHeartbeatTimeout()
	}
	k.CurrentTerm = term
	k.currentTermMutex.Unlock()
}

func (k *State) GetCurrentTerm() int {
	k.currentTermMutex.RLock()
	defer k.currentTermMutex.RUnlock()
	return k.CurrentTerm
}

func (k *State) SetVotedFor(vote int) {
	k.votedForMutex.Lock()
	k.VotedFor = vote
	k.votedForMutex.Unlock()
}

func (k *State) GetVotedFor() int {
	k.votedForMutex.RLock()
	defer k.votedForMutex.RUnlock()
	return k.VotedFor
}

func (k *State) GetLeaderId() int {
	return k.LeaderId
}

func (k *State) SetLeaderId(leaderId int) {
	k.LeaderId = leaderId
}

func (k *State) GetAddress() Address {
	return k.Address
}

func (k *State) SetAddress(address Address) {
	k.Address = address
}

func (k *State) IsVoting() bool {
	k.isVotingMutex.RLock()
	defer k.isVotingMutex.RUnlock()
	return k.isVoting
}

func (k *State) SetIsVoting(isVoting bool) {
	k.isVotingMutex.Lock()
	k.isVoting = isVoting
	k.isVotingMutex.Unlock()
}
