package dtos

import "if3230-tubes-williamjeffersonclinton/internal/domain/entities"

type ClientStrlnRequest struct {
	Key string
}

type ClientStrlnResponse struct {
	Length  int
	Success bool
	LeaderPort string
}

type ClientDeleteRequest struct {
	Key string
}

type ClientDeleteResponse struct {
	Value   string
	Success bool
	LeaderPort string
}

type ClientGetRequest struct {
	Key string
}

type ClientGetResponse struct {
	Value   string
	Success bool
	LeaderPort string
}

type ClientSetRequest struct {
	Key   string
	Value string
}

type ClientSetResponse struct {
	Success bool
	LeaderPort string
}

type ClientAppendRequest struct {
	Key   string
	Value string
}

type ClientAppendResponse struct {
	Value   string
	Success bool
	LeaderPort string
}

type ClientAddMemberRequest struct {
	Address entities.Address
}

type ClientAddMemberResponse struct {
	Success bool
	LeaderPort string
}