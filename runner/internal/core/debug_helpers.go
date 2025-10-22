package core

import (
	"bufio"
	"io"
	"log"
	"os/exec"
	"strings"
)

// progressWriter wraps an io.Writer and calls a progress callback
type progressWriter struct {
	writer     io.Writer
	onProgress func(int)
}

func (pw *progressWriter) Write(p []byte) (n int, err error) {
	n, err = pw.writer.Write(p)
	if pw.onProgress != nil {
		pw.onProgress(n)
	}
	return n, err
}

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

	// Stream stdout while collecting for JSON parsing - optimized for large outputs
	go func() {
		defer close(done)

		// Use io.Copy to handle arbitrarily large outputs without line limits
		var outputBuffer strings.Builder

		// Create a custom writer that logs progress periodically
		const logInterval = 1024 * 1024 // Log every 1MB of data
		bytesRead := 0
		lastLoggedBytes := 0

		// Create a tee reader to copy data while monitoring progress
		progressWriter := &progressWriter{
			writer: &outputBuffer,
			onProgress: func(n int) {
				bytesRead += n
				if bytesRead-lastLoggedBytes >= logInterval {
					log.Printf("%s [stdout] Progress: %.2f MB read...", label, float64(bytesRead)/(1024*1024))
					lastLoggedBytes = bytesRead
				}
			},
		}

		// Copy all stdout data to our buffer
		_, err := io.Copy(progressWriter, stdout)

		if err != nil {
			log.Printf("%s [stdout] Copy error: %v", label, err)
		}

		totalMB := float64(bytesRead) / (1024 * 1024)
		log.Printf("%s [stdout] Completed - processed %.2f MB total", label, totalMB)

		// Send the collected output
		outputChan <- []byte(outputBuffer.String())
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

// truncateString truncates a string to maxLength with ellipsis if needed
func truncateString(s string, maxLength int) string {
	if len(s) <= maxLength {
		return s
	}
	return s[:maxLength-3] + "..."
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