package ratelimiters

import (
	"sync"
	"time"
)

// IPBucketState tracks state for a single IP
type IPBucketState struct {
	Current       int
	LastResetTime time.Time
}

type TokenBucketAlgo struct {
	Capacity     int
	ResetTime    time.Duration
	Counter      map[string]*IPBucketState
	mu           sync.RWMutex // Protects Counter from concurrent access
	tokensPerSec float64      // Tokens added per second
}

// NewTokenBucketAlgo creates a new token bucket rate limiter
// resetTime: duration after which tokens are fully replenished
// capacity: maximum tokens per IP
func NewTokenBucketAlgo(resetTime time.Duration, capacity int) *TokenBucketAlgo {
	if resetTime == 0 {
		resetTime = 5 * time.Second
	}
	if capacity == 0 {
		capacity = 10
	}

	tb := &TokenBucketAlgo{
		ResetTime:    resetTime,
		Capacity:     capacity,
		Counter:      make(map[string]*IPBucketState),
		tokensPerSec: float64(capacity) / resetTime.Seconds(),
	}

	// Start background cleanup task
	go tb.cleanUpTask()

	return tb
}

func (tb *TokenBucketAlgo) Process(ip string) bool {
	if ip == "" {
		return false
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()
	bucket, exists := tb.Counter[ip]
	if !exists {
		bucket = &IPBucketState{
			Current:       tb.Capacity,
			LastResetTime: time.Now(),
		}
	}
	tb.refillTokens(bucket)
	if bucket.Current > 0 && bucket.Current <= tb.Capacity {
		bucket.Current -= 1
		return true
	}
	return false
}

func (tb *TokenBucketAlgo) refillTokens(bucket *IPBucketState) {
	now := time.Now()
	elapsed := now.Sub(bucket.LastResetTime).Seconds()

	// Calculate how many tokens to add
	tokensToAdd := int(elapsed * tb.tokensPerSec)

	if tokensToAdd > 0 {
		bucket.Current += tokensToAdd
		// Cap at capacity (don't exceed max)
		if bucket.Current > tb.Capacity {
			bucket.Current = tb.Capacity
		}
		bucket.LastResetTime = now
	}
}

func (tb *TokenBucketAlgo) cleanUpTask() {
	timer := time.NewTicker(1 * time.Minute)
	defer timer.Stop()
	inactivityTime := 10 * time.Minute
	for range timer.C {
		tb.mu.Lock()
		defer tb.mu.Unlock()
		now := time.Now()
		for ip, bucket := range tb.Counter {
			if now.Sub(bucket.LastResetTime) > inactivityTime {
				delete(tb.Counter, ip)
			}
		}
	}
}
