package core

import (
	"context"
	"time"
)

// ExecutionConfig holds configuration for binary execution operations
type ExecutionConfig struct {
	DebugMode     bool                // Whether to enable debug logging and subprocess output
	Timeout       time.Duration       // Default timeout for binary operations
	ScraperPaths  ScraperBinaryPaths  // Paths to scraper binaries
	DokitoPaths   DokitoBinaryPaths   // Paths to dokito processing binaries
	// Can easily add more config options here:
	// RetryCount   int
	// LogLevel     string
	// etc.
}

// executionConfigKey is the key type for storing ExecutionConfig in context
type executionConfigKey struct{}

// WithExecutionConfig creates a new context with the provided ExecutionConfig
func WithExecutionConfig(ctx context.Context, config *ExecutionConfig) context.Context {
	return context.WithValue(ctx, executionConfigKey{}, config)
}

// GetExecutionConfig retrieves the ExecutionConfig from context
// Returns a default config if none is found in the context
func GetExecutionConfig(ctx context.Context) *ExecutionConfig {
	if config, ok := ctx.Value(executionConfigKey{}).(*ExecutionConfig); ok {
		return config
	}

	// Return sensible defaults if no config is found
	return &ExecutionConfig{
		DebugMode:    false,
		Timeout:      5 * time.Minute, // Default 5 minute timeout
		ScraperPaths: GetScraperPaths(),
		DokitoPaths:  GetDokitoPaths(),
	}
}

// NewExecutionConfig creates a new ExecutionConfig with sensible defaults
func NewExecutionConfig() *ExecutionConfig {
	return &ExecutionConfig{
		DebugMode:    false,
		Timeout:      5 * time.Minute,
		ScraperPaths: GetScraperPaths(),
		DokitoPaths:  GetDokitoPaths(),
	}
}

// NewExecutionConfigWithDebug creates a new ExecutionConfig with debug mode enabled
func NewExecutionConfigWithDebug() *ExecutionConfig {
	return &ExecutionConfig{
		DebugMode:    true,
		Timeout:      10 * time.Minute, // Longer timeout for debug mode
		ScraperPaths: GetScraperPaths(),
		DokitoPaths:  GetDokitoPaths(),
	}
}

// SetDebugMode returns a copy of the config with debug mode set
func (c *ExecutionConfig) SetDebugMode(enabled bool) *ExecutionConfig {
	newConfig := *c
	newConfig.DebugMode = enabled
	return &newConfig
}

// SetTimeout returns a copy of the config with timeout set
func (c *ExecutionConfig) SetTimeout(timeout time.Duration) *ExecutionConfig {
	newConfig := *c
	newConfig.Timeout = timeout
	return &newConfig
}