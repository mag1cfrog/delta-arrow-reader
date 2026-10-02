package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"
)

// One real-HTTP check covers forwarding, progressive delivery, shared rate,
// cancellation and capture. It is opt-in and does not run in Rust CI.
func TestNetworkProxy(t *testing.T) {
	body := bytes.Repeat([]byte("0123456789abcdef"), 512*1024)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "" && (r.Host != "signed.example:1234" || r.Header.Get("Authorization") != "preserved-signature" || r.URL.RawPath != "/selective-read/a%2Fb.parquet" || r.URL.RawQuery != "key=a%2Bb&key=two") {
			http.Error(w, "signature inputs changed", 403)
			return
		}
		w.Header().Set("X-Amz-Request-Id", "fake-request")
		http.ServeContent(w, r, "file", time.Unix(0, 0), bytes.NewReader(body))
	}))
	defer upstream.Close()
	target, _ := url.Parse(upstream.URL)
	p := profile{LatencyMS: 200, JitterMS: 20, Mbps: 150, Seed: "test"}
	handler := newProxy(target, p)
	server := httptest.NewServer(handler)
	defer server.Close()
	client := server.Client()
	client.Timeout = 10 * time.Second
	control := func(reset, trace bool) snapshot {
		t.Helper()
		method := "GET"
		if reset {
			method = "POST"
		}
		r, _ := http.NewRequest(method, server.URL+fmt.Sprintf("/_benchmark/network?trace=%t", trace), nil)
		resp, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var value snapshot
		if resp.StatusCode != 200 {
			t.Fatalf("control status %d", resp.StatusCode)
		}
		if err := json.NewDecoder(resp.Body).Decode(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	control(true, true)

	r, _ := http.NewRequest("GET", server.URL+"/selective-read/a%2Fb.parquet?key=a%2Bb&key=two", nil)
	r.Host = "signed.example:1234"
	r.Header.Set("Authorization", "preserved-signature")
	r.Header.Set("Range", "bytes=11-23")
	started := time.Now()
	resp, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != 206 || !bytes.Equal(got, body[11:24]) || time.Since(started) < 180*time.Millisecond {
		t.Fatalf("forwarded range/latency mismatch: status=%d body=%q error=%v", resp.StatusCode, got, err)
	}
	value := control(false, true)
	if len(value.Records) != 1 || value.Records[0].DelayUS != p.delay(r).Microseconds() {
		t.Fatal(value)
	}
	for i := 0; i < 100; i++ {
		r.URL.RawQuery = fmt.Sprintf("index=%d", i)
		delay := p.delay(r)
		if delay < 180*time.Millisecond || delay > 220*time.Millisecond || delay != p.delay(r) {
			t.Fatal(delay)
		}
	}

	resp, err = client.Head(server.URL + "/selective-read/test.parquet")
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || len(got) != 0 || resp.ContentLength != int64(len(body)) {
		t.Fatal("HEAD body/length changed")
	}

	control(true, true)
	started = time.Now()
	resp, err = client.Get(server.URL + "/selective-read/test.parquet")
	if err != nil {
		t.Fatal(err)
	}
	first := make([]byte, 32768)
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatal(err)
	}
	firstAt := time.Since(started)
	rest, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	elapsed := time.Since(started)
	if err != nil || sha256.Sum256(append(first, rest...)) != sha256.Sum256(body) {
		t.Fatal("stream bytes changed", err)
	}
	if firstAt > elapsed/2 || elapsed-firstAt < 350*time.Millisecond {
		t.Fatalf("not progressively streamed: first=%v total=%v", firstAt, elapsed)
	}
	value = control(false, true)
	if value.Bytes != int64(len(body)) || value.Requests != 1 || value.Active != 0 || value.Records[0].Incomplete {
		t.Fatal(value)
	}
	t.Logf("8 MiB: first data %v, finished %v", firstAt, elapsed)

	control(true, false)
	started = time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, _ := http.NewRequest("GET", server.URL+"/selective-read/test.parquet", nil)
			r.Header.Set("Range", "bytes=0-4194303")
			resp, err := client.Do(r)
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			n, err := io.Copy(io.Discard, resp.Body)
			if err != nil || n != 4*1024*1024 {
				t.Errorf("concurrent transfer: %d %v", n, err)
			}
		}()
	}
	wg.Wait()
	elapsed = time.Since(started)
	expected := time.Duration(16 * 1024 * 1024 * 8000 / p.Mbps)
	if elapsed < expected+170*time.Millisecond || elapsed > 2*expected+300*time.Millisecond {
		t.Fatalf("shared bandwidth mismatch: %v expected payload %v", elapsed, expected)
	}
	value = control(false, false)
	if value.Bytes != 16*1024*1024 || value.Requests != 4 || len(value.Records) != 0 {
		t.Fatal(value)
	}
	t.Logf("4 concurrent requests, 16 MiB combined: %v", elapsed)

	control(true, true)
	ctx, cancel := context.WithCancel(context.Background())
	r, _ = http.NewRequestWithContext(ctx, "GET", server.URL+"/selective-read/test.parquet", nil)
	resp, err = client.Do(r)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	_, _ = io.ReadFull(resp.Body, first)
	cancel()
	resp.Body.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		value = control(false, true)
		if value.Active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cancelled request did not drain")
		}
		time.Sleep(time.Millisecond)
	}
	if value.Bytes >= int64(len(body)) || len(value.Records) != 1 || !value.Records[0].Incomplete {
		t.Fatal(value)
	}
	control(true, false)
}
