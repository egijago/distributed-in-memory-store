package entities

import (
	"if3230-tubes-williamjeffersonclinton/internal/domain/constants"
	"math/rand"
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
}

func NewState(address Address, electionTimeout int, heartbeatInterval int) *State {
	rand.Seed(time.Now().UnixNano())
	randomInt := rand.Intn(500)
	heartbeatTimeout := heartbeatInterval + 100 + randomInt
	return &State{
		Id:       0,
		Members:  []RaftClient{},
		Address:  address,
		LeaderId: constants.INVALID_ID,

		CurrentTerm: constants.INVALID_TERM,
		VotedFor:    constants.NO_VOTE,

		CommitIndex: 0,
		LastApplied: 0,

		Role:       constants.NODE_ROLE_FOLLOWER,
		NextIndex:  make(map[RaftClient]int),
		MatchIndex: make(map[RaftClient]int),

		ElectionTimeout:   time.Duration(electionTimeout) * time.Millisecond,
		HeartbeatInterval: time.Duration(heartbeatInterval) * time.Millisecond,
		HeartbeatTimeout:  time.Duration(heartbeatTimeout) * time.Millisecond,
	}
}
