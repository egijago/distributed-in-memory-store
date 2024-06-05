package entities

import (
	"strings"
)

type Log struct {
	Index   int
	Command string
	Term    int
}

func (l *Log) GetKeyValue() (string, string) {
	words := strings.Fields(l.Command)
	key := words[1]

	value := ""
	if len(words) > 2 {
		value = words[2]
	}

	return key, value
}
