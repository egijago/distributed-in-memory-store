package dtos

import "if3230-tubes-williamjeffersonclinton/internal/domain/entities"

type ClientStrlnRequest struct {
	Key string
}

type ClientStrlnResponse struct {
	Length  int
	Success bool
}

type ClientDeleteRequest struct {
	Key string
}

type ClientDeleteResponse struct {
	Value   string
	Success bool
}

type ClientGetRequest struct {
	Key string
}

type ClientGetResponse struct {
	Value   string
	Success bool
}

type ClientSetRequest struct {
	Key   string
	Value string
}

type ClientSetResponse struct {
	Success bool
}

type ClientAppendRequest struct {
	Key   string
	Value string
}

type ClientAppendResponse struct {
	Value   string
	Success bool
}

type ClientAddMemberRequest struct {
	Address entities.Address
}

type ClientAddMemberResponse struct {
	Success bool
}