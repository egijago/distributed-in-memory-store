package repository

import (
	entity "if3230-tubes-williamjeffersonclinton/internal/domain/entities"
)

type LogRepositoryInterface interface {
	Append(entity.Log)
	Get(int) (entity.Log, bool)
	Delete(int)
	GetLastLog() (entity.Log, bool)
	FindLogIndex(int) int
	DeleteLogsFromIndex(int)
}

type LogRepository struct {
	lastIndex int
	buffer    map[int]entity.Log
}

func NewLogRepository() *LogRepository {
	return &LogRepository{
		lastIndex: 0,
		buffer:    make(map[int]entity.Log),
	}
}

func (l *LogRepository) Append(value entity.Log) {
	l.lastIndex++
	l.buffer[l.lastIndex] = value
}

func (l *LogRepository) Get(key int) (entity.Log, bool) {
	value, exists := l.buffer[key]
	return value, exists
}

func (l *LogRepository) Delete(key int) {
	delete(l.buffer, key)
}

func (l *LogRepository) GetLastIndex() int {
	return l.lastIndex
}

func (l *LogRepository) GetLastLog() (entity.Log, bool) {
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
