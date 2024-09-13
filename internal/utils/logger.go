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

func LogInfof(message string, args ...interface{}) {
	logger.Printf(message, args...)
}

func LogWarning(messages ...interface{}) {
	fmt.Print("\033[33m[WARNING]\033[0m")
	logger.Println(messages...)
}

func LogWarningf(message string, args ...interface{}) {
	fmt.Print("\033[33m[WARNING]\033[0m")
	logger.Printf(message, args...)
}

func LogError(messages ...interface{}) {
	fmt.Print("\x1b[ERROR]\x1b[0m")
	logger.Println(messages...)
	os.Exit(1)
}

func LogErrorf(message string, args ...interface{}) {
	fmt.Print("\x1b[ERROR]\x1b[0m")
	logger.Printf(message, args...)
	os.Exit(1)
}

func Println(messages ...interface{}) {
	fmt.Println(messages...)
}

func Print(messages ...interface{}) {
	fmt.Print(messages...)
}

func Printf(message string, args ...interface{}) {
	fmt.Printf(message, args...)
}
