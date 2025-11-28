package router

import "net/http"

type Router struct{}

func (p *) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    backend := p.NextBackend()
    if backend == nil {
        http.Error(w, "no backends available", http.StatusServiceUnavailable)
        return
    }

    // Add some proxy headers
    r.Header.Set("X-Forwarded-Host", r.Host)
    r.Header.Set("X-Forwarded-Proto", "http") // or https if you terminate TLS
    r.Host = backend.URL.Host

    backend.ReverseProxy.ServeHTTP(w, r)
}
