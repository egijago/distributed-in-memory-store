package utils

import (
	"fmt"
	"log"
	"os"
)

var logger = log.New(os.Stdout, "", log.LstdFlags)

func LogInfo(messages ...interface{}) {
	logger.Print(messages...)
}

func LogWarning(messages ...interface{}) {
	fmt.Print("\033[33m[WARNING]\033[0m")
	logger.Println(messages...)

}

func LogError(messages ...interface{}) {
	fmt.Print("\x1b[ERROR]\x1b[0m")
	logger.Println(messages...)
	os.Exit(1)
}

func Println(messages ...interface{}) {
	fmt.Println(messages...)
}

func Print(messages ...interface{}) {
	fmt.Print(messages...)
}
