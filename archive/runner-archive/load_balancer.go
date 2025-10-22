package main

import (
	"fmt"
	"sync"
)

// LoadBalancingStrategy defines the strategy for assigning work to workers
type LoadBalancingStrategy string

const (
	// RoundRobin assigns work in a circular fashion
	RoundRobin LoadBalancingStrategy = "round-robin"

	// LeastLoaded assigns work to the worker with the fewest active jobs
	LeastLoaded LoadBalancingStrategy = "least-loaded"
)

// WorkerAssignment represents a govID assigned to a worker
type WorkerAssignment struct {
	GovID    string
	WorkerID string
	JobID    string
}

// LoadBalancer manages work distribution across workers
type LoadBalancer struct {
	// Strategy for load balancing
	strategy LoadBalancingStrategy

	// Worker IDs available for assignment
	workerIDs []string

	// Current assignment state (govID -> workerID)
	assignments map[string]string

	// Worker load tracking (workerID -> count of active jobs)
	workerLoads map[string]int

	// Round-robin index
	roundRobinIndex int

	// Mutex for thread-safe operations
	mu sync.RWMutex
}

// NewLoadBalancer creates a new load balancer
func NewLoadBalancer(workerIDs []string, strategy LoadBalancingStrategy) *LoadBalancer {
	if strategy == "" {
		strategy = LeastLoaded // Default to least-loaded strategy
	}

	lb := &LoadBalancer{
		strategy:        strategy,
		workerIDs:       workerIDs,
		assignments:     make(map[string]string),
		workerLoads:     make(map[string]int),
		roundRobinIndex: 0,
	}

	// Initialize worker loads to 0
	for _, workerID := range workerIDs {
		lb.workerLoads[workerID] = 0
	}

	return lb
}

// AssignGovIDToWorker assigns a govID to the best available worker
func (lb *LoadBalancer) AssignGovIDToWorker(govID string) string {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Check if already assigned
	if workerID, exists := lb.assignments[govID]; exists {
		return workerID
	}

	// Select worker based on strategy
	var workerID string
	switch lb.strategy {
	case RoundRobin:
		workerID = lb.selectWorkerRoundRobin()
	case LeastLoaded:
		workerID = lb.selectWorkerLeastLoaded()
	default:
		workerID = lb.selectWorkerLeastLoaded()
	}

	// Record assignment
	lb.assignments[govID] = workerID
	lb.workerLoads[workerID]++

	return workerID
}

// AssignBatchToWorkers assigns multiple govIDs to workers in a balanced way
func (lb *LoadBalancer) AssignBatchToWorkers(govIDs []string) map[string][]string {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Create map of workerID -> govIDs
	workerAssignments := make(map[string][]string)
	for _, workerID := range lb.workerIDs {
		workerAssignments[workerID] = make([]string, 0)
	}

	// Assign each govID
	for _, govID := range govIDs {
		// Skip if already assigned
		if _, exists := lb.assignments[govID]; exists {
			continue
		}

		// Select worker
		var workerID string
		switch lb.strategy {
		case RoundRobin:
			workerID = lb.selectWorkerRoundRobin()
		case LeastLoaded:
			workerID = lb.selectWorkerLeastLoaded()
		default:
			workerID = lb.selectWorkerLeastLoaded()
		}

		// Record assignment
		lb.assignments[govID] = workerID
		lb.workerLoads[workerID]++
		workerAssignments[workerID] = append(workerAssignments[workerID], govID)
	}

	return workerAssignments
}

// ReleaseGovID removes a govID assignment (when work completes or fails)
func (lb *LoadBalancer) ReleaseGovID(govID string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if workerID, exists := lb.assignments[govID]; exists {
		delete(lb.assignments, govID)
		lb.workerLoads[workerID]--
		if lb.workerLoads[workerID] < 0 {
			lb.workerLoads[workerID] = 0
		}
	}
}

// ReassignGovID reassigns a govID to a different worker (for retries)
func (lb *LoadBalancer) ReassignGovID(govID string) string {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Release current assignment if exists
	if workerID, exists := lb.assignments[govID]; exists {
		delete(lb.assignments, govID)
		lb.workerLoads[workerID]--
		if lb.workerLoads[workerID] < 0 {
			lb.workerLoads[workerID] = 0
		}
	}

	// Select new worker
	var workerID string
	switch lb.strategy {
	case RoundRobin:
		workerID = lb.selectWorkerRoundRobin()
	case LeastLoaded:
		workerID = lb.selectWorkerLeastLoaded()
	default:
		workerID = lb.selectWorkerLeastLoaded()
	}

	// Record new assignment
	lb.assignments[govID] = workerID
	lb.workerLoads[workerID]++

	return workerID
}

// GetWorkerLoad returns the number of active jobs for a worker
func (lb *LoadBalancer) GetWorkerLoad(workerID string) int {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	return lb.workerLoads[workerID]
}

// GetWorkerAssignment returns the worker assigned to a govID
func (lb *LoadBalancer) GetWorkerAssignment(govID string) (string, bool) {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	workerID, exists := lb.assignments[govID]
	return workerID, exists
}

// GetAllWorkerLoads returns the load for all workers
func (lb *LoadBalancer) GetAllWorkerLoads() map[string]int {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	loads := make(map[string]int)
	for workerID, load := range lb.workerLoads {
		loads[workerID] = load
	}
	return loads
}

// GetAssignmentCount returns the total number of active assignments
func (lb *LoadBalancer) GetAssignmentCount() int {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	return len(lb.assignments)
}

// RebalanceWorker redistributes work from a specific worker (e.g., on failure)
func (lb *LoadBalancer) RebalanceWorker(failedWorkerID string) []string {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	reassigned := make([]string, 0)

	// Find all govIDs assigned to the failed worker
	for govID, workerID := range lb.assignments {
		if workerID == failedWorkerID {
			// Remove assignment
			delete(lb.assignments, govID)
			lb.workerLoads[workerID]--
			reassigned = append(reassigned, govID)
		}
	}

	// Ensure failed worker load is reset
	lb.workerLoads[failedWorkerID] = 0

	return reassigned
}

// selectWorkerRoundRobin selects the next worker in round-robin order
// Must be called with lock held
func (lb *LoadBalancer) selectWorkerRoundRobin() string {
	if len(lb.workerIDs) == 0 {
		return ""
	}

	workerID := lb.workerIDs[lb.roundRobinIndex]
	lb.roundRobinIndex = (lb.roundRobinIndex + 1) % len(lb.workerIDs)
	return workerID
}

// selectWorkerLeastLoaded selects the worker with the fewest active jobs
// Must be called with lock held
func (lb *LoadBalancer) selectWorkerLeastLoaded() string {
	if len(lb.workerIDs) == 0 {
		return ""
	}

	// Find worker with minimum load
	minLoad := -1
	selectedWorker := ""

	for _, workerID := range lb.workerIDs {
		load := lb.workerLoads[workerID]
		if minLoad == -1 || load < minLoad {
			minLoad = load
			selectedWorker = workerID
		}
	}

	return selectedWorker
}

// GetLoadBalanceStats returns statistics about load balancing
func (lb *LoadBalancer) GetLoadBalanceStats() LoadBalanceStats {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	stats := LoadBalanceStats{
		Strategy:         string(lb.strategy),
		TotalWorkers:     len(lb.workerIDs),
		ActiveAssignments: len(lb.assignments),
		WorkerLoads:      make(map[string]int),
	}

	// Copy worker loads
	for workerID, load := range lb.workerLoads {
		stats.WorkerLoads[workerID] = load
	}

	// Calculate balance metrics
	if len(lb.workerIDs) > 0 {
		totalLoad := 0
		maxLoad := 0
		minLoad := -1

		for _, load := range lb.workerLoads {
			totalLoad += load
			if load > maxLoad {
				maxLoad = load
			}
			if minLoad == -1 || load < minLoad {
				minLoad = load
			}
		}

		stats.AverageLoad = float64(totalLoad) / float64(len(lb.workerIDs))
		stats.MaxLoad = maxLoad
		stats.MinLoad = minLoad
		stats.LoadImbalance = float64(maxLoad - minLoad)
	}

	return stats
}

// LoadBalanceStats represents load balancing statistics
type LoadBalanceStats struct {
	Strategy          string         `json:"strategy"`
	TotalWorkers      int            `json:"total_workers"`
	ActiveAssignments int            `json:"active_assignments"`
	WorkerLoads       map[string]int `json:"worker_loads"`
	AverageLoad       float64        `json:"average_load"`
	MaxLoad           int            `json:"max_load"`
	MinLoad           int            `json:"min_load"`
	LoadImbalance     float64        `json:"load_imbalance"`
}

// PrintStats prints load balancing statistics
func (lb *LoadBalancer) PrintStats() {
	stats := lb.GetLoadBalanceStats()
	fmt.Printf("Load Balancer Stats:\n")
	fmt.Printf("  Strategy: %s\n", stats.Strategy)
	fmt.Printf("  Total Workers: %d\n", stats.TotalWorkers)
	fmt.Printf("  Active Assignments: %d\n", stats.ActiveAssignments)
	fmt.Printf("  Average Load: %.2f\n", stats.AverageLoad)
	fmt.Printf("  Max Load: %d\n", stats.MaxLoad)
	fmt.Printf("  Min Load: %d\n", stats.MinLoad)
	fmt.Printf("  Load Imbalance: %.2f\n", stats.LoadImbalance)
	fmt.Printf("  Worker Loads:\n")
	for workerID, load := range stats.WorkerLoads {
		fmt.Printf("    %s: %d\n", workerID, load)
	}
}
