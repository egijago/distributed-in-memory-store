package services

import (
	"github.com/melvinkj/raft-consensus/internal/domain/entities"
	"github.com/melvinkj/raft-consensus/internal/domain/repositories"
	"github.com/melvinkj/raft-consensus/internal/domain/dtos"
	// "github.com/melvinkj/raft-consensus/internal/utils"
)

type ClientService struct {
	lastApplied int
	currentTerm int
	kvRepository repository.KeyValueRepositoryInterface
	logRepository repository.LogRepositoryInterface
}

func NewClientService(kvRepository repository.KeyValueRepositoryInterface, logRepository repository.LogRepositoryInterface) *ClientService {
	return &ClientService{
		kvRepository: kvRepository,
		logRepository: logRepository,
	}
}

func (k *ClientService) Set(request dtos.ClientSetRequest) (response *dtos.ClientSetResponse) {
	
	// if its a leader
	newLog := entity.Log {
		Index: k.lastApplied,
		Command: "set " + request.Key + " " + request.Value,
		Term: k.currentTerm,
	}
	k.logRepository.Append(newLog)

	// propagate the log to other node 


	
	// apply the log to the state machine 

	// if less than majority
	if (false) {
		return &dtos.ClientSetResponse{
			Success: false,
		}
	}
	k.kvRepository.Set(request.Key, request.Value)

	return &dtos.ClientSetResponse{
		Success: true,
	}
}



func (k *ClientService) Get(request dtos.ClientGetRequest) (response *dtos.ClientGetResponse) {
	if (false) { // if election is happening
		return &dtos.ClientGetResponse {
			Value: "",
			Success: false,
		}		
	}
	
	Value, found := k.kvRepository.Get(request.Key)

	if (!found) {
		return &dtos.ClientGetResponse {
			Value: "",
			Success: false,
		}	
	}
	return &dtos.ClientGetResponse {
		Value: Value,
		Success: true,
	}
}




func (k *ClientService) Delete(request dtos.ClientDeleteRequest) (response *dtos.ClientDeleteResponse) {
	if (false) { // if election is happening
		return &dtos.ClientDeleteResponse {
			Success: false,
		}		
	}
	
	value, found := k.kvRepository.Get(request.Key)

	if (!found) {
		return &dtos.ClientDeleteResponse {
			Success: false,
		}	
	}

	k.kvRepository.Delete(request.Key)

	return &dtos.ClientDeleteResponse {
		Value: value,
		Success: true,
	}
}


func (k *ClientService) Append(request dtos.ClientAppendRequest) (response *dtos.ClientAppendResponse) {
	if (false) { // if election is happening
		return &dtos.ClientAppendResponse {
			Success: false,
		}		
	}
	
	Value, found := k.kvRepository.Get(request.Key)

	if (found) {
		Value = Value + request.Value
	} else {
		Value = request.Value
	}

	k.kvRepository.Set(request.Key, Value)

	return &dtos.ClientAppendResponse {
		Value: Value,
		Success: true,
	}
}




func (k *ClientService) Strln(request dtos.ClientStrlnRequest) (response *dtos.ClientStrlnResponse) {
	if (false) { // if election is happening
		return &dtos.ClientStrlnResponse {
			Success: false,
		}		
	}
	
	Value, found := k.kvRepository.Get(request.Key)

	length := 0
	if (found) {
		length = len(Value)
		return &dtos.ClientStrlnResponse {
			Length: length,
			Success: true,
		}
	}

	return &dtos.ClientStrlnResponse {
		Length: length,
		Success: false,
	}
}