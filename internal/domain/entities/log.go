package entities

import (
	"strings"
)

type Log struct {
	Index   int
	Command string
	Term    int
}


func (l *Log) Parse() (string, string, string) {
	words := strings.Fields(l.Command)
	command := words[0]
	key := words[1]

	value := ""
	if len(words) > 2 {
		value = words[2]
	}

	return command, key, value
}

// func (l *Log) GetKeyValue() (string, string) {
// 	words := strings.Fields(l.Command)
// 	key := words[1]

// 	value := ""
// 	if len(words) > 2 {
// 		value = words[2]
// 	}

// 	return key, value
// }

// func (l *Log) GetCommand() (string) {
// 	words := strings.Fields(l.Command)
// 	command := words[0]

// 	return command
// }