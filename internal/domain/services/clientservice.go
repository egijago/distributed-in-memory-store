package services

import (
	"if3230-tubes-williamjeffersonclinton/internal/domain/dtos"
	entity "if3230-tubes-williamjeffersonclinton/internal/domain/entities"
	"if3230-tubes-williamjeffersonclinton/internal/domain/repositories"
	"if3230-tubes-williamjeffersonclinton/internal/domain/constants"
	// "if3230-tubes-williamjeffersonclinton/internal/utils"

	"fmt"
	"strings"
)

type ClientService struct {
	lastApplied   int
	currentTerm   int
	kvRepository  repositories.KeyValueRepositoryInterface
	logRepository repositories.LogRepositoryInterface
	raftService   *RaftService	
}

func NewClientService(kvRepository repositories.KeyValueRepositoryInterface, logRepository repositories.LogRepositoryInterface, raftService *RaftService) *ClientService {
	return &ClientService{
		kvRepository:  kvRepository,
		logRepository: logRepository,
		raftService: raftService,
	}
}

func (k *ClientService) Set(request dtos.ClientSetRequest) (response *dtos.ClientSetResponse) {
	// If election is happening
	if (k.raftService.state.IsElecting) {
		return &dtos.ClientSetResponse{
			Success: false,
		}
	}

	// Redirect to leader if node type is follower
	if (k.raftService.state.Role != constants.NODE_ROLE_LEADER){
		fmt.Println("[ERROR] Request accepted by a follower")
		return &dtos.ClientSetResponse{
			Success: false,
			LeaderPort: k.raftService.state.Members[k.raftService.state.LeaderId].Address.Port,
		}
	}


	newLog := entity.Log{
		Index:   k.logRepository.GetLastIndex() + 1,
		Command: "set " + request.Key + " " + request.Value,
		Term:    k.currentTerm,
	}

	k.logRepository.Append(newLog)
	logIndex := k.logRepository.GetLastIndex()
	
	// Propagate leader logs
	if err := k.raftService.PropagateLog(); err != nil {
		fmt.Printf("[ERROR] error on propagate log: %v", err)

		k.logRepository.DeleteLogsFromIndex(logIndex)
		return &dtos.ClientSetResponse{
			Success: false,
		}
	}

	// if less than majority
	if false {
		fmt.Println("[ERROR] error set call")

		k.logRepository.DeleteLogsFromIndex(logIndex)
		return &dtos.ClientSetResponse{
			Success: false,
		}
	}

	return &dtos.ClientSetResponse{
		Success: true,
	}
}

func (k *ClientService) Get(request dtos.ClientGetRequest) (response *dtos.ClientGetResponse) {
	// If election is happening
	if (k.raftService.state.IsElecting) {
		return &dtos.ClientGetResponse{
			Success: false,
		}
	}

	// Redirect to leader if node type is follower
	if (k.raftService.state.Role != constants.NODE_ROLE_LEADER){
		fmt.Println("[ERROR] Request accepted by a follower")
		return &dtos.ClientGetResponse{
			Success: false,
			Value:   "",
			LeaderPort: k.raftService.state.Members[k.raftService.state.LeaderId].Address.Port,
		}
	}

	Value, found := k.kvRepository.Get(request.Key)

	if !found {
		return &dtos.ClientGetResponse{
			Value:   "",
			Success: false,
		}
	}
	return &dtos.ClientGetResponse{
		Value:   Value,
		Success: true,
	}
}

func (k *ClientService) Delete(request dtos.ClientDeleteRequest) (response *dtos.ClientDeleteResponse) {
	// If election is happening
	if (k.raftService.state.IsElecting) {
		return &dtos.ClientDeleteResponse{
			Success: false,
		}
	}

	// Redirect to leader if node type is follower
	if (k.raftService.state.Role != constants.NODE_ROLE_LEADER){
		fmt.Println("[ERROR] Request accepted by a follower")
		return &dtos.ClientDeleteResponse{
			Success: false,
			LeaderPort: k.raftService.state.Members[k.raftService.state.LeaderId].Address.Port,
		}
	}

	value, found := k.kvRepository.Get(request.Key)

	if !found {
		return &dtos.ClientDeleteResponse{
			Success: false,
		}
	}

	newLog := entity.Log{
		Index:   k.logRepository.GetLastIndex() + 1,
		Command: "delete " + request.Key,
		Term:    k.currentTerm,
	}

	k.logRepository.Append(newLog)
	logIndex := k.logRepository.GetLastIndex()
	
	// Propagate leader logs
	if err := k.raftService.PropagateLog(); err != nil {
		fmt.Printf("[ERROR] error on propagate log: %v", err)

		k.logRepository.DeleteLogsFromIndex(logIndex)
		return &dtos.ClientDeleteResponse{
			Success: false,
		}
	}

	return &dtos.ClientDeleteResponse{
		Value:   value,
		Success: found,
	}
}

func (k *ClientService) Append(request dtos.ClientAppendRequest) (response *dtos.ClientAppendResponse) {
	// If election is happening
	if (k.raftService.state.IsElecting) {
		return &dtos.ClientAppendResponse{
			Success: false,
		}
	}

	// Redirect to leader if node type is follower
	if (k.raftService.state.Role != constants.NODE_ROLE_LEADER){
		fmt.Println("[ERROR] Request accepted by a follower")
		return &dtos.ClientAppendResponse{
			Success: false,
			LeaderPort: k.raftService.state.Members[k.raftService.state.LeaderId].Address.Port,
		}
	}

	newLog := entity.Log{
		Index:   k.logRepository.GetLastIndex() + 1,
		Command: "append " + request.Key + " " + request.Value,
		Term:    k.currentTerm,
	}

	k.logRepository.Append(newLog)
	logIndex := k.logRepository.GetLastIndex()
	
	// Propagate leader logs
	if err := k.raftService.PropagateLog(); err != nil {
		fmt.Printf("[ERROR] error on propagate log: %v", err)

		k.logRepository.DeleteLogsFromIndex(logIndex)
		return &dtos.ClientAppendResponse{
			Success: false,
		}
	}

	data, _ := k.kvRepository.Get(request.Key)

	return &dtos.ClientAppendResponse{
		Value:  data,
		Success: true,
	}
}

func (k *ClientService) Strln(request dtos.ClientStrlnRequest) (response *dtos.ClientStrlnResponse) {
	// If election is happening
	if (k.raftService.state.IsElecting) {
		return &dtos.ClientStrlnResponse{
			Success: false,
		}
	}

	// Redirect to leader if node type is follower
	if (k.raftService.state.Role != constants.NODE_ROLE_LEADER){
		fmt.Println("[ERROR] Request accepted by a follower")
		return &dtos.ClientStrlnResponse{
			Length: 0,
			Success: false,
			LeaderPort: k.raftService.state.Members[k.raftService.state.LeaderId].Address.Port,
		}
	}

	Value, found := k.kvRepository.Get(request.Key)

	length := 0
	if found {
		length = len(Value)
		return &dtos.ClientStrlnResponse{
			Length:  length,
			Success: true,
		}
	}

	return &dtos.ClientStrlnResponse{
		Length:  length,
		Success: false,
	}
}

func (k *ClientService) ReqLog() (response *dtos.ClientRequestLogResponse) {
	// If election is happening
	if (k.raftService.state.IsElecting) {
		return &dtos.ClientRequestLogResponse{
			Success: false,
		}
	}

	// Redirect to leader if node type is follower
	if (k.raftService.state.Role != constants.NODE_ROLE_LEADER){
		fmt.Println("[ERROR] Request accepted by a follower")
		return &dtos.ClientRequestLogResponse{
			Value: "",
			Success: false,
			LeaderPort: k.raftService.state.Members[k.raftService.state.LeaderId].Address.Port,
		}
	}

	/* Get All Key Value Pairs */
	var keyValuePairs []string
    for key, value := range k.kvRepository.GetBuffer() {
        keyValuePairs = append(keyValuePairs, fmt.Sprintf("%s:%s", key, value))
    }
    allValue := strings.Join(keyValuePairs, ", ")

	return &dtos.ClientRequestLogResponse{
		Value: allValue,
		Success: true,
	}
}