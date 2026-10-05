// Network shaping for the opt-in MinIO benchmark. Standard library only.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const chunkBytes = 64 * 1024
const maxTraceRecords = 100000

type profile struct {
	LatencyMS int64  `json:"latency_ms"`
	JitterMS  int64  `json:"jitter_ms"`
	Mbps      int64  `json:"mbps"`
	Seed      string `json:"seed"`
}

func (p profile) validate() error {
	if p.LatencyMS < 0 || p.LatencyMS > 60000 || p.JitterMS < 0 || p.JitterMS > p.LatencyMS || p.Mbps < 0 || p.Mbps > 100000 || len(p.Seed) > 128 {
		return errors.New("invalid latency, jitter, bandwidth or seed")
	}
	return nil
}

func (p profile) delay(r *http.Request) time.Duration {
	us := p.LatencyMS * 1000
	if p.JitterMS != 0 {
		// Identity-based jitter is stable across scheduling order and connections.
		h := sha256.Sum256([]byte(p.Seed + "\x00" + r.Method + "\x00" + r.URL.RequestURI() + "\x00" + r.Header.Get("Range")))
		span := p.JitterMS * 1000
		us += int64(binary.BigEndian.Uint64(h[:8])%uint64(2*span+1)) - span
	}
	return time.Duration(us) * time.Microsecond
}

func wait(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

type limiter struct {
	mu   sync.Mutex
	next time.Time
	mbps int64
}

func (l *limiter) take(ctx context.Context, bytes int) error {
	if l.mbps == 0 {
		return ctx.Err()
	}
	l.mu.Lock()
	now := time.Now()
	// Keep timer overshoot from lowering sustained throughput. Catch-up is
	// bounded to one chunk, so idle time cannot accumulate a large burst.
	if l.next.Before(now.Add(-time.Duration(chunkBytes * 8000 / l.mbps))) {
		l.next = now
	}
	l.next = l.next.Add(time.Duration(int64(bytes) * 8000 / l.mbps))
	end := l.next
	l.mu.Unlock()
	// ponytail: a cancelled reservation wastes at most one chunk per request.
	// Reset the schedule between idle invocations; reclaim slots if that matters.
	return wait(ctx, time.Until(end))
}

type requestRecord struct {
	Method        string `json:"method"`
	Object        string `json:"object"`
	Class         string `json:"object_class"`
	Range         string `json:"range"`
	RequestID     string `json:"request_id"`
	Status        int    `json:"status"`
	Bytes         int64  `json:"response_bytes"`
	ContentLength int64  `json:"advertised_content_length"`
	StartedNS     int64  `json:"started_ns"`
	EndedNS       int64  `json:"ended_ns"`
	DelayUS       int64  `json:"delay_us"`
	Incomplete    bool   `json:"incomplete_body"`
}

type snapshot struct {
	Profile   profile         `json:"profile"`
	Active    int             `json:"active"`
	Requests  int64           `json:"requests"`
	Bytes     int64           `json:"response_bytes"`
	Trace     bool            `json:"trace_enabled"`
	Truncated bool            `json:"trace_truncated"`
	Records   []requestRecord `json:"records,omitempty"`
}

type proxy struct {
	profile profile
	limit   limiter
	forward *httputil.ReverseProxy
	mu      sync.Mutex
	stats   snapshot
}

func newProxy(target *url.URL, p profile) *proxy {
	s := &proxy{profile: p, limit: limiter{mbps: p.Mbps}, stats: snapshot{Profile: p}}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.MaxIdleConns = 2048
	transport.MaxIdleConnsPerHost = 2048
	transport.ResponseHeaderTimeout = 60 * time.Second
	s.forward = &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.Out.URL.Scheme, r.Out.URL.Host = target.Scheme, target.Host
			// SigV4 signs Host, escaped paths and query parameters. Preserve them.
			r.Out.Host = r.In.Host
			r.Out.URL.RawQuery = r.In.URL.RawQuery
		},
		Transport:     transport,
		FlushInterval: -1,
		ErrorLog:      log.New(io.Discard, "", 0),
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, _ error) {
			http.Error(w, "benchmark upstream request failed", http.StatusBadGateway)
		},
	}
	return s
}

type shapedWriter struct {
	http.ResponseWriter
	ctx    context.Context
	limit  *limiter
	record *requestRecord
}

func (w *shapedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *shapedWriter) WriteHeader(status int) {
	if status < 200 {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	if w.record.Status != 0 {
		return
	}
	w.record.Status = status
	w.record.RequestID = w.Header().Get("X-Amz-Request-Id")
	w.record.ContentLength = -1
	if length := w.Header().Get("Content-Length"); length != "" {
		fmt.Sscan(length, &w.record.ContentLength)
	}
	w.ResponseWriter.WriteHeader(status)
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *shapedWriter) Write(data []byte) (int, error) {
	if w.record.Status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	total := 0
	for len(data) > 0 {
		count := min(len(data), chunkBytes)
		if err := w.limit.take(w.ctx, count); err != nil {
			return total, err
		}
		n, err := w.ResponseWriter.Write(data[:count])
		total += n
		w.record.Bytes += int64(n)
		if err != nil {
			return total, err
		}
		if n != count {
			return total, io.ErrShortWrite
		}
		if err := http.NewResponseController(w.ResponseWriter).Flush(); err != nil {
			return total, err
		}
		data = data[count:]
	}
	return total, nil
}

func (s *proxy) control(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method == "POST" {
		if s.stats.Active != 0 {
			http.Error(w, "requests still active", http.StatusConflict)
			return
		}
		s.stats = snapshot{Profile: s.profile, Trace: r.URL.Query().Get("trace") == "true"}
		s.limit.mu.Lock()
		s.limit.next = time.Time{}
		s.limit.mu.Unlock()
	} else if r.Method != "GET" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.stats)
}

func (s *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/_benchmark/network" {
		s.control(w, r)
		return
	}
	if (r.Method != "GET" && r.Method != "HEAD") || (r.URL.Path != "/selective-read" && !strings.HasPrefix(r.URL.Path, "/selective-read/")) {
		http.Error(w, "only benchmark bucket reads are allowed", http.StatusForbidden)
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/selective-read/")
	class := "other"
	switch {
	case r.URL.Path == "/selective-read" || key == "":
		key, class = "", "listing"
		if r.Method == "HEAD" {
			class = "bucket"
		}
	case strings.Contains("/"+key, "/_delta_log/"):
		class = "delta_log"
	case strings.HasSuffix(key, ".parquet"):
		class = "parquet"
	case strings.Contains("/"+key, "/deletion_vector_"):
		class = "deletion_vector"
	}
	delay := s.profile.delay(r)
	record := requestRecord{Method: r.Method, Object: key, Class: class, Range: r.Header.Get("Range"), StartedNS: time.Now().UnixNano(), DelayUS: delay.Microseconds(), ContentLength: -1}
	s.mu.Lock()
	s.stats.Active++
	s.mu.Unlock()
	defer func() {
		record.EndedNS = time.Now().UnixNano()
		record.Incomplete = record.Status == 0 || (r.Method != "HEAD" && record.Status != 204 && record.Status != 304 && record.ContentLength >= 0 && record.Bytes < record.ContentLength)
		s.mu.Lock()
		defer s.mu.Unlock()
		s.stats.Active--
		s.stats.Requests++
		s.stats.Bytes += record.Bytes
		if s.stats.Trace {
			if len(s.stats.Records) < maxTraceRecords {
				s.stats.Records = append(s.stats.Records, record)
			} else {
				s.stats.Truncated = true
			}
		}
	}()
	if wait(r.Context(), delay) != nil {
		return
	}
	s.forward.ServeHTTP(&shapedWriter{ResponseWriter: w, ctx: r.Context(), limit: &s.limit, record: &record}, r)
}

func loopback(value string) bool {
	host, _, err := net.SplitHostPort(value)
	return err == nil && host == "127.0.0.1"
}

func main() {
	listen := flag.String("listen", "127.0.0.1:19002", "loopback proxy address")
	upstream := flag.String("upstream", "http://127.0.0.1:19000", "dedicated loopback MinIO")
	p := profile{}
	flag.Int64Var(&p.LatencyMS, "latency-ms", 200, "additional request latency")
	flag.Int64Var(&p.JitterMS, "jitter-ms", 20, "symmetric uniform jitter bound")
	flag.Int64Var(&p.Mbps, "mbps", 150, "shared decimal Mbit/s; zero removes the bandwidth cap")
	flag.StringVar(&p.Seed, "seed", "0", "deterministic jitter seed")
	flag.Parse()
	target, err := url.Parse(*upstream)
	if err != nil || p.validate() != nil || !loopback(*listen) || target.Scheme != "http" || !loopback(target.Host) || target.User != nil || target.Path != "" || target.RawQuery != "" || target.Fragment != "" {
		log.Fatal("invalid loopback addresses or network profile")
	}
	server := &http.Server{Addr: *listen, Handler: newProxy(target, p), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
