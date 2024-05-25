package constants


// Raft NodeType
type NodeType struct {
	LEADER   int
	FOLLOWER int
}

var NODE_TYPE = NodeType{
	LEADER:   1,
	FOLLOWER: 2,
}