package worker

import "errors"

var (
	// ErrQueueNotInitialized is returned when queue operations are attempted before initialization
	ErrQueueNotInitialized = errors.New("task queue not initialized")

	// ErrTaskFailed is returned when a task execution fails
	ErrTaskFailed = errors.New("task execution failed")
)