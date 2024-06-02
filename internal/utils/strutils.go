package utils

import (
	"strconv"
	"log"
)

func SplitString(s string) []string {
	var result []string
	for _, str := range s {
		result = append(result, string(str))
	}
	return result
}

func ParseNumbers(numbersStr string) []int {
	var numbers []int
	numStrSlice := SplitString(numbersStr)

	for _, numStr := range numStrSlice {
		num, err := strconv.Atoi(numStr)
		if err != nil {
			log.Printf("Error parsing number %q: %v\n", numStr, err)
			continue
		}
		numbers = append(numbers, num)
	}
	return numbers
}

