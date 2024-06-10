package repositories

import (
	"sync"

	"if3230-tubes-williamjeffersonclinton/internal/domain/entities"
)

type LogRepositoryInterface interface {
	Append(entities.Log) int
	Get(int) (entities.Log, bool)
	Set(int, entities.Log)
	Delete(int)
	GetLastLog() (entities.Log, bool)
	FindLogIndex(int) int
	GetLastIndex() int
	DeleteLogsStartFrom(int)
	DeleteLogsRange(int, int)
	GetBuffer() map[int]entities.Log
	GetLogs() []entities.Log
	GetNewIndex() int
	GetLogsFromTerm(int) []entities.Log
	GetLogsWhere(int, int) []entities.Log
}

type LogRepository struct {
	lastIndex int
	buffer    map[int]entities.Log
	indexLock sync.Mutex
}

func NewLogRepository() *LogRepository {
	return &LogRepository{
		lastIndex: -1,
		buffer:    make(map[int]entities.Log),
	}
}

func (l *LogRepository) GetNewIndex() int {
	l.indexLock.Lock()
	defer l.indexLock.Unlock()
	l.lastIndex++
	return l.lastIndex
}

func (l *LogRepository) GetBuffer() map[int]entities.Log {
	return l.buffer
}

func (l *LogRepository) GetLogs() []entities.Log {
	var logs []entities.Log

	for id := 0; id <= l.lastIndex; id++ {
		log, exists := l.buffer[id]
		if exists {
			logs = append(logs, log)
		}
	}
	return logs
}

func (l *LogRepository) GetLogsFromTerm(term int) []entities.Log {
	var logs []entities.Log

	for id := 0; id <= l.lastIndex; id++ {
		log, exists := l.buffer[id]
		if exists && log.Term == term {
			logs = append(logs, log)
		}
	}
	return logs
}

func (l *LogRepository) Append(value entities.Log) int {
	l.lastIndex++
	value.Index = l.lastIndex
	l.buffer[l.lastIndex] = value
	return l.lastIndex
}

func (l *LogRepository) Get(idx int) (entities.Log, bool) {
	value, exists := l.buffer[idx]

	if !exists {
		return entities.Log{}, false
	}

	return value, true
}

func (l *LogRepository) Set(idx int, value entities.Log) {
	l.lastIndex = max(l.lastIndex, idx)
	l.buffer[idx] = value
}

func (l *LogRepository) Delete(key int) {
	l.buffer[key] = entities.Log{}
}

func (l *LogRepository) GetLastIndex() int {
	return l.lastIndex
}

func (l *LogRepository) GetLastLog() (entities.Log, bool) {
	if l.lastIndex == -1 {
		return entities.Log{}, false
	}

	return l.buffer[l.lastIndex], true
}

func (l *LogRepository) FindLogIndex(index int) int {
	log := l.buffer[index]
	return log.Index
}

func (l *LogRepository) DeleteLogsStartFrom(startIndex int) {
	for i := startIndex; i <= l.lastIndex; i++ {
		delete(l.buffer, i)
	}
}

func (l *LogRepository) DeleteLogsRange(startIndex int, endIndex int) {
	if startIndex < 0 || endIndex < 0 || startIndex > endIndex || endIndex > l.lastIndex {
		return
	}

	for i := startIndex; i <= endIndex; i++ {
		delete(l.buffer, i)
	}
}

func (l *LogRepository) GetLogsWhere(term, startIdx int) []entities.Log {
	var logs []entities.Log

	for id := startIdx; id <= l.lastIndex; id++ {
		log, exists := l.buffer[id]
		if exists && log.Term == term {
			logs = append(logs, log)
		}
	}
	return logs
}
