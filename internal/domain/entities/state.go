package entities

import (
	"if3230-tubes-williamjeffersonclinton/internal/domain/constants"
	"math/rand"
	"sync"
	"time"
)

type State struct {
	Id       int
	Members  []RaftClient
	Address  Address
	LeaderId int

	// Persistent State
	CurrentTerm int
	VotedFor    int
	IsElecting	bool

	// Volatile State
	CommitIndex int
	LastApplied int

	// Volatile State on Leaders
	Role int

	NextIndex  map[RaftClient]int
	MatchIndex map[RaftClient]int

	ElectionTimeout time.Duration

	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration

	// Mutexes
	roleMutex        sync.RWMutex
	currentTermMutex sync.RWMutex
	votedForMutex    sync.RWMutex
}

func NewState(address Address, electionTimeout int, heartbeatInterval int) *State {
	rand.Seed(time.Now().UnixNano())
	/* 0 - 600 */
	randomInt := rand.Intn(600)
	heartbeatTimeout := heartbeatInterval + 200 + randomInt
	return &State{
		Id:       0,
		Members:  []RaftClient{},
		Address:  address,
		LeaderId: constants.INVALID_ID,

		CurrentTerm: constants.INVALID_TERM,
		VotedFor:    constants.NO_VOTE,
		IsElecting:  false,

		CommitIndex: -1,
		LastApplied: -1,

		Role:       constants.NODE_ROLE_FOLLOWER,
		NextIndex:  make(map[RaftClient]int),
		MatchIndex: make(map[RaftClient]int),

		ElectionTimeout:   time.Duration(electionTimeout) * time.Millisecond,
		HeartbeatInterval: time.Duration(heartbeatInterval) * time.Millisecond,
		HeartbeatTimeout:  time.Duration(heartbeatTimeout) * time.Millisecond,
	}
}

func (k *State) AddMember(address Address) (success bool) {
	newMember := NewRaftClient(*NewAddress(address.Host, address.Port))
	k.Members = append(k.Members, *newMember)

	return true
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
