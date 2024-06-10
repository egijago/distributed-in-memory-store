package utils

import (
	"fmt"
	"sync"
	"time"
)

type TimeoutMutex struct {
	mu     sync.Mutex
	IsHeld bool
}

func (tm *TimeoutMutex) LockWithTimeout(timeout time.Duration) {
	tm.mu.Lock()
	tm.IsHeld = true

	time.AfterFunc(timeout, func() {
		tm.mu.Lock()
		defer tm.mu.Unlock()

		if tm.IsHeld {
			fmt.Println("Timeout reached. Automatically releasing lock.")
			tm.IsHeld = false
			tm.mu.Unlock()
		}
	})
}

func (tm *TimeoutMutex) Unlock() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.IsHeld {
		tm.IsHeld = false
		tm.mu.Unlock()
	}
}
