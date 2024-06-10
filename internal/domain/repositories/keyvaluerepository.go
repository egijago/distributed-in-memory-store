package repositories

import (
	"if3230-tubes-williamjeffersonclinton/internal/domain/constants"
)

type KeyValueRepositoryInterface interface {
	Set(string, string)
	Get(string) (string, bool)
	Delete(string)
	Append(string, string)
	Execute(string, string, string)
	GetBuffer() map[string]string
}

type KeyValueRepository struct {
	buffer map[string]string
}

func NewKeyValueRepository() *KeyValueRepository {
	return &KeyValueRepository{
		buffer: make(map[string]string),
	}
}

func (l *KeyValueRepository) GetBuffer() map[string]string {
	return l.buffer
}

func (l *KeyValueRepository) Set(key string, value string) {
	l.buffer[key] = value
}

func (l *KeyValueRepository) Get(key string) (string, bool) {
	value, exists := l.buffer[key]
	return value, exists
}

func (l *KeyValueRepository) Delete(key string) {
	delete(l.buffer, key)
}

func (l *KeyValueRepository) Append(key string, value string) {
	data, ok := l.Get(key)
	if ok {
		data = data + value
	} else {
		data = value
	}

	l.Set(key, data)
}

func (l *KeyValueRepository) Execute(command, arg1, arg2 string) {
	switch command {
	case "set":
		l.Set(arg1, arg2)
	case "delete":
		l.Delete(arg1)
	case "append":
		l.Append(arg1, arg2)
	case constants.INVALID_COMMAND:
		return
	}
}
