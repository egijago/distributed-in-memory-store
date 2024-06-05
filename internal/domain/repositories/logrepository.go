package repositories

import (
	"if3230-tubes-williamjeffersonclinton/internal/domain/entities"
)

type LogRepositoryInterface interface {
	Append(entities.Log)
	Get(int) (entities.Log, bool)
	Delete(int)
	GetLastLog() (entities.Log, bool)
	FindLogIndex(int) int
	DeleteLogsFromIndex(int)
}

type LogRepository struct {
	lastIndex int
	buffer    map[int]entities.Log
}

func NewLogRepository() *LogRepository {
	return &LogRepository{
		lastIndex: 0,
		buffer:    make(map[int]entities.Log),
	}
}

func (l *LogRepository) Append(value entities.Log) {
	l.lastIndex++
	l.buffer[l.lastIndex] = value
}

func (l *LogRepository) Get(key int) (entities.Log, bool) {
	if key < 0 || key > l.lastIndex {
		return entities.Log{}, false
	}
	value, exists := l.buffer[key]
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
