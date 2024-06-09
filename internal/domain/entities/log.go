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
	arg1 := words[1]

	arg2 := ""
	if len(words) > 2 {
		arg2 = words[2]
	}

	return command, arg1, arg2
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