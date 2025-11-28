package routingalgorithms

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"

	"github.com/Khambampati-Subhash/LoadBalancer/internal/ratelimiters"
	"github.com/labstack/gommon/log"
)

type RoundRobin struct {
	URL   string
	Proxy *httputil.ReverseProxy
}

type Backend struct {
	pool         []*RoundRobin
	current      int64
	numOfServers int
	ip           string
	rateLimiter  *ratelimiters.TokenBucketAlgo
}

func NewRoundRobinAlgo(urls []string, rateLimiter *ratelimiters.TokenBucketAlgo, ip string) *Backend {
	if len(urls) == 0 {
		log.Fatal("No urls")
		return nil
	}
	backend := &Backend{}
	backend.ip = ip
	backend.rateLimiter = rateLimiter

	for _, rawURL := range urls {
		parsedURL, err := url.Parse(rawURL)
		if err != nil {
			log.Errorf("Error while parsing the url, err:%s", err)
			continue
		}
		reverProxy := httputil.NewSingleHostReverseProxy(parsedURL)
		roundRobin := &RoundRobin{
			rawURL, reverProxy,
		}
		backend.pool = append(backend.pool, roundRobin)
	}

	backend.numOfServers = len(backend.pool)

	return backend
}

func (b *Backend) RouteToBackend() *RoundRobin {
	if len(b.pool) == 0 {
		return nil
	}
	idx := atomic.AddInt64(&b.current, 1)

	return b.pool[int((idx-1)%int64(b.numOfServers))]
}

func (b *Backend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !b.rateLimiter.Process(b.ip) {
		log.Error("No tokens in bucket so rejecting the request")
	} else {

		backend := b.RouteToBackend()
		if backend == nil {
			http.Error(w, "no backends available", http.StatusServiceUnavailable)
			return
		}

		// Add some proxy headers
		r.Header.Set("X-Forwarded-Host", r.Host)
		r.Header.Set("X-Forwarded-Proto", "http") // or https if you terminate TLS

		backend.Proxy.ServeHTTP(w, r)
	}
}
