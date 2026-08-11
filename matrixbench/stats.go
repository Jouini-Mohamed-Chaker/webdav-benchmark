package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type cycleRecord struct {
	SessionID int       `json:"session_id"`
	Cycle     int       `json:"cycle"`
	Start     time.Time `json:"start"`
	ReadMS    float64   `json:"read_ms"`
	ComputeMS float64   `json:"compute_ms"`
	WriteMS   float64   `json:"write_ms"`
	TotalMS   float64   `json:"total_ms"`
	Err       string    `json:"err,omitempty"`
}

type sessionCounters struct {
	cycles    atomic.Int64
	errors    atomic.Int64
	degraded  atomic.Bool
	lastFatal atomic.Value // string
}

type statsSink struct {
	mu       sync.Mutex
	file     *os.File
	enc      *json.Encoder
	perSess  []*sessionCounters
	start    time.Time
	totalOK  atomic.Int64
	totalErr atomic.Int64
}

func newStatsSink(f *os.File, sessions int) *statsSink {
	s := &statsSink{
		file:    f,
		enc:     json.NewEncoder(f),
		perSess: make([]*sessionCounters, sessions),
		start:   time.Now(),
	}
	for i := range s.perSess {
		s.perSess[i] = &sessionCounters{}
	}
	return s
}

func (s *statsSink) record(rec cycleRecord) {
	s.mu.Lock()
	_ = s.enc.Encode(rec)
	s.mu.Unlock()

	c := s.perSess[rec.SessionID]
	c.cycles.Add(1)
	if rec.Err != "" {
		c.errors.Add(1)
		s.totalErr.Add(1)
	} else {
		s.totalOK.Add(1)
	}
}

func (s *statsSink) markDegraded(sessionID, consecFail int) {
	c := s.perSess[sessionID]
	if !c.degraded.Load() {
		c.degraded.Store(true)
		fmt.Printf("[WARN] session %02d marked DEGRADED after %d consecutive failures (still retrying)\n", sessionID, consecFail)
	}
}

func (s *statsSink) recordFatal(sessionID int, err error) {
	fmt.Printf("[FATAL] session %02d could not start: %v\n", sessionID, err)
	if sessionID >= 0 && sessionID < len(s.perSess) {
		s.perSess[sessionID].lastFatal.Store(err.Error())
	}
}

func (s *statsSink) printSummary() {
	elapsed := time.Since(s.start).Seconds()
	ok := s.totalOK.Load()
	errs := s.totalErr.Load()
	total := ok + errs
	var rate float64
	if elapsed > 0 {
		rate = float64(total) / elapsed
	}

	degradedCount := 0
	for _, c := range s.perSess {
		if c.degraded.Load() {
			degradedCount++
		}
	}

	fmt.Printf("[%6.0fs] cycles=%d ok=%d err=%d (%.1f cycles/s total) degraded_sessions=%d/%d\n",
		elapsed, total, ok, errs, rate, degradedCount, len(s.perSess))
}

func (s *statsSink) printFinal() {
	fmt.Println("\n=== final per-session summary ===")
	for i, c := range s.perSess {
		status := "OK"
		if c.degraded.Load() {
			status = "DEGRADED"
		}
		if v := c.lastFatal.Load(); v != nil {
			status = fmt.Sprintf("FATAL: %v", v)
		}
		fmt.Printf("session %02d: cycles=%d errors=%d status=%s\n", i, c.cycles.Load(), c.errors.Load(), status)
	}
	fmt.Printf("\ntotal ok=%d total err=%d elapsed=%.1fs\n", s.totalOK.Load(), s.totalErr.Load(), time.Since(s.start).Seconds())
}