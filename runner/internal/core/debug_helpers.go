package core

import (
	"bufio"
	"log"
	"os/exec"
	"strings"
)

// executeWithDebugStreaming runs a command with real-time stdout/stderr streaming
// Returns the collected stdout for further processing (e.g., JSON parsing)
func executeWithDebugStreaming(cmd *exec.Cmd, label string) ([]byte, error) {
	// Create pipes for stdout and stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// Create channels to collect output
	outputChan := make(chan []byte, 1)
	done := make(chan struct{})

	// Stream stderr in real-time
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			log.Printf("%s [stderr] %s", label, scanner.Text())
		}
	}()

	// Stream stdout while collecting for JSON parsing
	go func() {
		defer close(done)
		var outputLines []string
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			log.Printf("%s [stdout] %s", label, line)
			outputLines = append(outputLines, line)
		}

		// Join all lines back together for JSON parsing
		fullOutput := strings.Join(outputLines, "\n")
		outputChan <- []byte(fullOutput)
	}()

	// Wait for command completion
	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	// Wait for output collection
	<-done

	// Get the collected output
	var output []byte
	select {
	case output = <-outputChan:
	default:
		// Return empty if no output (e.g., for upload commands)
		return []byte{}, nil
	}

	return output, nil
}

// executeWithDebugStreamingNoOutput runs a command with real-time stderr streaming
// Used for commands that don't need stdout collection (like upload)
func executeWithDebugStreamingNoOutput(cmd *exec.Cmd, label string) error {
	// Create pipe for stderr only
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		return err
	}

	// Stream stderr in real-time
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			log.Printf("%s [stderr] %s", label, scanner.Text())
		}
	}()

	// Wait for command completion
	return cmd.Wait()
}