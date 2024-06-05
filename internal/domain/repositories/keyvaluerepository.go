package repositories

type KeyValueRepositoryInterface interface {
	Set(string, string)
	Get(string) (string, bool)
	Delete(string)
}

type KeyValueRepository struct {
	buffer map[string]string
}

func NewKeyValueRepository() *KeyValueRepository {
	return &KeyValueRepository{
		buffer: make(map[string]string),
	}
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
