package routingalgorithms

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"

	"github.com/Khambampati-Subhash/LoadBalancer/internal/ratelimiters"
	"github.com/labstack/gommon/log"
)

type LeastConnNode struct {
	URL         string
	Proxy       *httputil.ReverseProxy
	activeConns int64 // number of in-flight requests
}

type LeastConnectionsBackend struct {
	pool         []*LeastConnNode
	numOfServers int
	ip           string
	rateLimiter  *ratelimiters.TokenBucketAlgo
}

func NewLeastConnectionsAlgo(urls []string, rateLimiter *ratelimiters.TokenBucketAlgo, ip string) *LeastConnectionsBackend {
	if len(urls) == 0 {
		log.Fatal("No urls")
		return nil
	}

	backend := &LeastConnectionsBackend{
		ip:          ip,
		rateLimiter: rateLimiter,
	}

	for _, rawURL := range urls {
		parsedURL, err := url.Parse(rawURL)
		if err != nil {
			log.Errorf("Error while parsing the url, err:%s", err)
			continue
		}

		reverseProxy := httputil.NewSingleHostReverseProxy(parsedURL)
		node := &LeastConnNode{
			URL:   rawURL,
			Proxy: reverseProxy,
		}
		backend.pool = append(backend.pool, node)
	}

	backend.numOfServers = len(backend.pool)

	if backend.numOfServers == 0 {
		log.Fatal("No valid backend URLs after parsing")
		return nil
	}

	return backend
}

// RouteToBackend picks the backend with the least active connections.
func (b *LeastConnectionsBackend) RouteToBackend() *LeastConnNode {
	if len(b.pool) == 0 {
		return nil
	}

	// Start with the first backend as the current minimum.
	var chosen *LeastConnNode
	var minConns int64

	for i, node := range b.pool {
		active := atomic.LoadInt64(&node.activeConns)

		if i == 0 || active < minConns {
			chosen = node
			minConns = active
		}
	}

	return chosen
}

func (b *LeastConnectionsBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !b.rateLimiter.Process(b.ip) {
		log.Error("No tokens in bucket so rejecting the request")
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	backend := b.RouteToBackend()
	if backend == nil {
		http.Error(w, "no backends available", http.StatusServiceUnavailable)
		return
	}

	// Increment active connections for this backend
	atomic.AddInt64(&backend.activeConns, 1)
	defer atomic.AddInt64(&backend.activeConns, -1)

	// Add proxy headers
	r.Header.Set("X-Forwarded-Host", r.Host)
	r.Header.Set("X-Forwarded-Proto", "http") // or https if you terminate TLS

	backend.Proxy.ServeHTTP(w, r)
}
