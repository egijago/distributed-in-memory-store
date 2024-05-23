package internal

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		args := strings.Fields(line)
		if len(args) == 0 {
			continue
		}

		switch args[0] {
		case "ping":
			fmt.Println("PONG")

		case "get":
			if len(args) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			// Dummy response for testing
			fmt.Println("\"dummy-value\"")

		case "set":
			if len(args) != 3 {
				fmt.Println("Usage: set <key> <value>")
				continue
			}
			// Dummy response for testing
			fmt.Println("OK")

		case "strln":
			if len(args) != 2 {
				fmt.Println("Usage: strln <key>")
				continue
			}
			// Dummy response for testing
			fmt.Println("10")

		case "del":
			if len(args) != 2 {
				fmt.Println("Usage: del <key>")
				continue
			}
			// Dummy response for testing
			fmt.Println("\"deleted-value\"")

		case "append":
			if len(args) != 3 {
				fmt.Println("Usage: append <key> <value>")
				continue
			}
			// Dummy response for testing
			fmt.Println("OK")

		default:
			fmt.Println("Unknown command:", args[0])
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading input:", err)
	}
}