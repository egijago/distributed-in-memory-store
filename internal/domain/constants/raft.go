package constants

// Raft node roles
const (
	NODE_ROLE_LEADER = iota + 1
	NODE_ROLE_CANDIDATE
	NODE_ROLE_FOLLOWER
)

const NO_VOTE = -1
const INVALID_ID = -1
const INVALID_TERM = -1