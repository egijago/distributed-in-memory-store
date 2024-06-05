package repositories

import (
	"if3230-tubes-williamjeffersonclinton/internal/domain/entities"
	"if3230-tubes-williamjeffersonclinton/internal/utils"
)

type LogRepositoryInterface interface {
	GetBuffer() map[int]entities.Log
	Append(entities.Log)
	Get(int) (entities.Log, bool)
	Delete(int)
	GetLastLog() (entities.Log, bool)
	FindLogIndex(int) int
	GetLastIndex() int
	DeleteLogsFromIndex(int)
}

type LogRepository struct {
	lastIndex int
	buffer    map[int]entities.Log /* Dictionary { Key: Integer, Value: Entities.Log } */
}

func NewLogRepository() *LogRepository {
	return &LogRepository{
		lastIndex: -1,
		buffer:    make(map[int]entities.Log),
	}
}

func (l *LogRepository) GetBuffer() map[int]entities.Log {
	return l.buffer
}

func (l *LogRepository) Append(value entities.Log) {
	l.lastIndex++
	l.buffer[l.lastIndex] = value
}

func (l *LogRepository) Get(idx int) (entities.Log, bool) {
	if idx < 0 || idx > l.lastIndex {
		utils.LogInfo(l.lastIndex)
		utils.LogInfo("HOHOSSSSS")
		
		return entities.Log{}, false
	}

	value, exists := l.buffer[idx]
	return value, exists
}

func (l *LogRepository) Delete(key int) {
	delete(l.buffer, key)
}

func (l *LogRepository) GetLastIndex() int {
	return l.lastIndex
}

func (l *LogRepository) GetLastLog() (entities.Log, bool) {
	value, exists := l.buffer[l.lastIndex]
	return value, exists
}

func (repo *LogRepository) FindLogIndex(index int) int {
	log, exists := repo.buffer[index]
	if !exists {
		return -1
	}
	return log.Index
}

func (repo *LogRepository) DeleteLogsFromIndex(startIndex int) {
	for i := startIndex; i <= repo.lastIndex; i++ {
		delete(repo.buffer, i)
	}
	repo.lastIndex = startIndex - 1
}
