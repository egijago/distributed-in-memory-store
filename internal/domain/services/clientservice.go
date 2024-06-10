package services

import (
	"fmt"
	"strings"

	"if3230-tubes-williamjeffersonclinton/internal/domain/constants"
	"if3230-tubes-williamjeffersonclinton/internal/domain/dtos"
	entity "if3230-tubes-williamjeffersonclinton/internal/domain/entities"
	"if3230-tubes-williamjeffersonclinton/internal/domain/repositories"
	"if3230-tubes-williamjeffersonclinton/internal/utils"
	// "if3230-tubes-williamjeffersonclinton/internal/utils"
)

type ClientService struct {
	raftService   RaftServiceInterface
	state         entity.StateInterface
	kvRepository  repositories.KeyValueRepositoryInterface
	logRepository repositories.LogRepositoryInterface
}

func NewClientService(state entity.StateInterface, kvRepository repositories.KeyValueRepositoryInterface, logRepository repositories.LogRepositoryInterface, raftService RaftServiceInterface) *ClientService {
	return &ClientService{
		state:         state,
		kvRepository:  kvRepository,
		logRepository: logRepository,
		raftService:   raftService,
	}
}

func (k *ClientService) Set(request dtos.ClientSetRequest) (response *dtos.ClientSetResponse) {
	if k.state.IsVoting() { /* if election is happening, reject request */
		utils.LogInfo("[SET] Voting is happening")
		return &dtos.ClientSetResponse{
			Success: false,
		}
	}

	if k.state.GetRole() != constants.NODE_ROLE_LEADER { /* if not a leader, redirect request */
		utils.LogInfo("[SET] Not a leader")
		return &dtos.ClientSetResponse{
			Success:    false,
			LeaderPort: k.state.GetMembers()[k.state.GetLeaderId()].Address.Port,
		}
	}

	index := k.logRepository.GetNewIndex()
	k.logRepository.Set(index, entity.Log{
		Command: "set " + request.Key + " " + request.Value,
		Index:   index,
		Term:    k.state.GetCurrentTerm(),
	})

	countAgree := k.raftService.AppendEntriesToAllMembers()

	if countAgree < k.raftService.GetQuorum() { /* if not meet the quorum, reject request */
		k.logRepository.Set(index, entity.Log{
			Command: constants.INVALID_COMMAND,
			Index:   index,
			Term:    k.state.GetCurrentTerm(),
		})
		membersPtr := k.state.GetMembersPtr()
		for id := range *membersPtr { /* correcting the prev (probably) sent entry*/
			member := &(*membersPtr)[id]
			member.NextIndex = min(member.NextIndex, index)
		}

		return &dtos.ClientSetResponse{
			Success: false,
		}
	}

	k.kvRepository.Set(request.Key, request.Value)
	k.state.SetCommitIndex(max(k.state.GetCommitIndex(), index))

	return &dtos.ClientSetResponse{
		Success: true,
	}
}

func (k *ClientService) Get(request dtos.ClientGetRequest) (response *dtos.ClientGetResponse) {
	if k.state.IsVoting() { /* if election is happening, reject request */
		utils.LogInfo("[APPEND] Voting is happening")
		return &dtos.ClientGetResponse{
			Success: false,
		}
	}

	if k.state.GetRole() != constants.NODE_ROLE_LEADER { /* if not a leader, redirect request */
		utils.LogInfo("[APPEND] Not a leader")
		return &dtos.ClientGetResponse{
			Success:    false,
			LeaderPort: k.state.GetMembers()[k.state.GetLeaderId()].Address.Port,
		}
	}

	value, found := k.kvRepository.Get(request.Key)

	if !found {
		return &dtos.ClientGetResponse{
			Value:   "NOT FOUND",
			Success: true,
		}
	}

	return &dtos.ClientGetResponse{
		Value:   value,
		Success: true,
	}
}

func (k *ClientService) Delete(request dtos.ClientDeleteRequest) (response *dtos.ClientDeleteResponse) {
	if k.state.IsVoting() { /* if election is happening, reject request */
		utils.LogInfo("[DELETE] Voting is happening")
		return &dtos.ClientDeleteResponse{
			Success: false,
		}
	}

	if k.state.GetRole() != constants.NODE_ROLE_LEADER { /* if not a leader, redirect request */
		utils.LogInfo("[DELETE] Not a leader")
		return &dtos.ClientDeleteResponse{
			Success:    false,
			LeaderPort: k.state.GetMembers()[k.state.GetLeaderId()].Address.Port,
		}
	}

	index := k.logRepository.GetNewIndex()
	k.logRepository.Set(index, entity.Log{
		Command: "delete " + request.Key,
		Index:   index,
		Term:    k.state.GetCurrentTerm(),
	})

	countAgree := k.raftService.AppendEntriesToAllMembers()

	if countAgree < k.raftService.GetQuorum() { /* if not meet the quorum, reject request */
		k.logRepository.Set(index, entity.Log{
			Command: constants.INVALID_COMMAND,
			Index:   index,
			Term:    k.state.GetCurrentTerm(),
		})
		membersPtr := k.state.GetMembersPtr()
		for id := range *membersPtr { /* correcting the prev (probably) sent entry*/
			member := &(*membersPtr)[id]
			member.NextIndex = min(member.NextIndex, index)
		}

		return &dtos.ClientDeleteResponse{
			Success: false,
		}
	}

	val, found := k.kvRepository.Get(request.Key)
	if !found {
		return &dtos.ClientDeleteResponse{
			Success: true,
			Value:   "NOT FOUND",
		}
	}
	k.kvRepository.Delete(request.Key)
	k.state.SetCommitIndex(max(k.state.GetCommitIndex(), index))
	return &dtos.ClientDeleteResponse{
		Success: true,
		Value:   val,
	}
}

func (k *ClientService) Append(request dtos.ClientAppendRequest) (response *dtos.ClientAppendResponse) {
	if k.state.IsVoting() { /* if election is happening, reject request */
		utils.LogInfo("[APPEND] Voting is happening")
		return &dtos.ClientAppendResponse{
			Success: false,
		}
	}

	if k.state.GetRole() != constants.NODE_ROLE_LEADER { /* if not a leader, redirect request */
		utils.LogInfo("[APPEND] Not a leader")
		return &dtos.ClientAppendResponse{
			Success:    false,
			LeaderPort: k.state.GetMembers()[k.state.GetLeaderId()].Address.Port,
		}
	}

	index := k.logRepository.GetNewIndex()
	k.logRepository.Set(index, entity.Log{
		Command: "append " + request.Key + " " + request.Value,
		Index:   index,
		Term:    k.state.GetCurrentTerm(),
	})

	countAgree := k.raftService.AppendEntriesToAllMembers()

	if countAgree < k.raftService.GetQuorum() { /* if not meet the quorum, reject request */
		k.logRepository.Set(index, entity.Log{
			Command: constants.INVALID_COMMAND,
			Index:   index,
			Term:    k.state.GetCurrentTerm(),
		})
		membersPtr := k.state.GetMembersPtr()
		for id := range *membersPtr { /* correcting the prev (probably) sent entry*/
			member := &(*membersPtr)[id]
			member.NextIndex = min(member.NextIndex, index)
		}

		return &dtos.ClientAppendResponse{
			Success: false,
		}
	}

	value, found := k.kvRepository.Get(request.Key)
	if !found {
		k.kvRepository.Set(request.Key, request.Value)
	} else {
		k.kvRepository.Set(request.Key, value+request.Value)
	}
	k.state.SetCommitIndex(max(k.state.GetCommitIndex(), index))
	return &dtos.ClientAppendResponse{
		Success: true,
		Value:   value + request.Value,
	}
}

func (k *ClientService) Strln(request dtos.ClientStrlnRequest) (response *dtos.ClientStrlnResponse) {
	if k.state.IsVoting() { /* if election is happening, reject request */
		utils.LogInfo("[APPEND] Voting is happening")
		return &dtos.ClientStrlnResponse{
			Success: false,
		}
	}

	if k.state.GetRole() != constants.NODE_ROLE_LEADER { /* if not a leader, redirect request */
		utils.LogInfo("[APPEND] Not a leader")
		return &dtos.ClientStrlnResponse{
			Success:    false,
			LeaderPort: k.state.GetMembers()[k.state.GetLeaderId()].Address.Port,
		}
	}

	value, found := k.kvRepository.Get(request.Key)

	if !found {
		return &dtos.ClientStrlnResponse{
			Length:  0,
			Success: true,
		}
	}
	length := len(value)
	return &dtos.ClientStrlnResponse{
		Length:  length,
		Success: true,
	}

}

func (k *ClientService) ReqLog() (response *dtos.ClientRequestLogResponse) {
	if k.state.IsVoting() {
		return &dtos.ClientRequestLogResponse{
			Success: false,
		}
	}

	if k.state.GetRole() != constants.NODE_ROLE_LEADER {
		fmt.Println("[ERROR] Request accepted by a follower")
		return &dtos.ClientRequestLogResponse{
			Value:      "",
			Success:    false,
			LeaderPort: k.state.GetMembers()[k.state.GetLeaderId()].Address.Port,
		}
	}

	var logs []string
	for _, log := range k.logRepository.GetBuffer() {
		logs = append(logs, fmt.Sprintf(log.Command))
	}
	allValue := strings.Join(logs, ", ")

	return &dtos.ClientRequestLogResponse{
		Value:   allValue,
		Success: true,
	}
}
