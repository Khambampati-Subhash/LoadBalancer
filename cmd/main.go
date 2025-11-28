package cmd

// this is the LB

import (
	"log"
	"net/http"
	"time"

	"github.com/Khambampati-Subhash/LoadBalancer/internal/ratelimiters"
	"github.com/Khambampati-Subhash/LoadBalancer/internal/routingalgorithms"
)

const (
	resetTime = 5 * time.Second
	capacity  = 10
)

func main() {
	urls := []string{
		"http://localhost:8081",
		"http://localhost:8082",
		"http://localhost:8083",
		"http://localhost:8084",
		"http://localhost:8085",
	}
	ip := "103.23.22.1"
	rateLimiter := ratelimiters.NewTokenBucketAlgo(resetTime, capacity)
	routingAlgo := routingalgorithms.NewRoundRobinAlgo(urls, rateLimiter, ip)
	if err := http.ListenAndServe(":8022", routingAlgo); err != nil {
		log.Fatal(err)
	}
}
