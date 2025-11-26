package routingalgorithms

import (
	"net/http/httputil"
	"net/url"
	"sync/atomic"

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
}

func NewRoundRobinAlgo(urls []string) *Backend {
	if len(urls) == 0 {
		log.Fatal("No urls")
		return nil
	}
	backend := &Backend{}

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

func (b *Backend) routeToBackend() *RoundRobin {
	if len(b.pool) == 0 {
		return nil
	}
	idx := atomic.AddInt64(&b.current, 1)

	return b.pool[int((idx-1)%int64(b.numOfServers))]
}
