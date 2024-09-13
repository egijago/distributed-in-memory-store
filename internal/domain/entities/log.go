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

