package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"gonum.org/v1/gonum/mat"
)

func runWorkload(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	dir := fs.String("dir", "", "dataset directory produced by 'generate' (required)")
	resultsDir := fs.String("results-dir", "", "directory for per-session result output (required)")
	sessions := fs.Int("sessions", 20, "number of concurrent read->multiply->write sessions")
	duration := fs.Duration("duration", 30*time.Minute, "how long to run (0 = until cycles limit or Ctrl-C)")
	cycles := fs.Int("cycles", 0, "max cycles per session (0 = unlimited, bounded by -duration instead)")
	statsPath := fs.String("stats", "matrixbench_stats.jsonl", "path to write per-cycle JSONL stats")
	reportEvery := fs.Duration("report-interval", 10*time.Second, "how often to print a summary to stdout")
	maxConsecFail := fs.Int("max-consecutive-failures", 5, "mark a session degraded after this many consecutive cycle failures (it keeps retrying regardless)")
	fs.Parse(args)

	if *dir == "" || *resultsDir == "" {
		fmt.Fprintln(os.Stderr, "run: -dir and -results-dir are required")
		fs.Usage()
		os.Exit(2)
	}

	manifest, err := loadManifest(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: loading manifest from %s: %v\n(did you run 'matrixbench generate' first?)\n", *dir, err)
		os.Exit(1)
	}
	if manifest.MatrixCount < 2 {
		fmt.Fprintln(os.Stderr, "run: dataset has fewer than 2 matrices, can't multiply")
		os.Exit(1)
	}

	if err := os.MkdirAll(*resultsDir, 0o775); err != nil {
		fmt.Fprintf(os.Stderr, "run: mkdir %s: %v\n", *resultsDir, err)
		os.Exit(1)
	}

	statsFile, err := os.Create(*statsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: creating stats file %s: %v\n", *statsPath, err)
		os.Exit(1)
	}
	defer statsFile.Close()
	stats := newStatsSink(statsFile, *sessions)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	fmt.Printf("matrixbench run: %d sessions, dataset=%d matrices (%dx%d, %.2fGB) at %s\n",
		*sessions, manifest.MatrixCount, manifest.MatrixDim, manifest.MatrixDim,
		float64(manifest.TotalBytes)/(1<<30), *dir)
	fmt.Printf("results -> %s | stats -> %s | duration=%s cycles=%d(0=unlimited)\n",
		*resultsDir, *statsPath, duration.String(), *cycles)

	var wg sync.WaitGroup
	for i := 0; i < *sessions; i++ {
		wg.Add(1)
		go func(sessionID int) {
			defer wg.Done()
			sessionResultDir := filepath.Join(*resultsDir, fmt.Sprintf("session-%02d", sessionID))
			if err := os.MkdirAll(sessionResultDir, 0o775); err != nil {
				stats.recordFatal(sessionID, fmt.Errorf("mkdir session result dir: %w", err))
				return
			}
			runSession(ctx, sessionID, *dir, sessionResultDir, manifest, *cycles, *maxConsecFail, stats)
		}(i)
	}

	// Periodic human-readable summary while sessions run.
	go func() {
		ticker := time.NewTicker(*reportEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stats.printSummary()
			}
		}
	}()

	wg.Wait()
	stats.printSummary()
	stats.printFinal()
}

func runSession(ctx context.Context, sessionID int, datasetDir, resultDir string, m *Manifest, maxCycles, maxConsecFail int, stats *statsSink) {
	rng := rand.New(rand.NewSource(int64(sessionID) + 1))
	consecFail := 0
	cycle := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if maxCycles > 0 && cycle >= maxCycles {
			return
		}
		cycle++

		rec := cycleRecord{SessionID: sessionID, Cycle: cycle, Start: time.Now()}

		idxA := rng.Intn(m.MatrixCount)
		idxB := rng.Intn(m.MatrixCount)
		for idxB == idxA {
			idxB = rng.Intn(m.MatrixCount)
		}

		t0 := time.Now()
		a, err := readMatrix(filepath.Join(datasetDir, fmt.Sprintf("matrix_%05d.bin", idxA)), m.MatrixDim)
		if err == nil {
			var b *mat.Dense
			b, err = readMatrix(filepath.Join(datasetDir, fmt.Sprintf("matrix_%05d.bin", idxB)), m.MatrixDim)
			rec.ReadMS = time.Since(t0).Seconds() * 1000

			if err == nil {
				t1 := time.Now()
				var c mat.Dense
				c.Mul(a, b)
				rec.ComputeMS = time.Since(t1).Seconds() * 1000

				t2 := time.Now()
				err = writeMatrix(filepath.Join(resultDir, "result.bin"), &c, m.MatrixDim)
				rec.WriteMS = time.Since(t2).Seconds() * 1000
			}
		}

		rec.TotalMS = time.Since(t0).Seconds() * 1000
		if err != nil {
			consecFail++
			rec.Err = err.Error()
			stats.record(rec)
			if consecFail >= maxConsecFail {
				stats.markDegraded(sessionID, consecFail)
			}
			// Deliberately keep looping - a single bad cycle should not
			// kill the session. Stability means surviving faults, not
			// crashing on the first one. We just log and retry.
			continue
		}
		consecFail = 0
		stats.record(rec)
	}
}

func readMatrix(path string, expectDim int) (*mat.Dense, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 4<<20)
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, fmt.Errorf("read header %s: %w", path, err)
	}
	dim := int(binary.LittleEndian.Uint32(hdr[:]))
	if dim != expectDim {
		return nil, fmt.Errorf("%s: dim mismatch, file has %d, manifest says %d", path, dim, expectDim)
	}

	n := dim * dim
	data := make([]float64, n)
	buf := make([]byte, 4)
	for i := 0; i < n; i++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("read data %s at elem %d: %w", path, i, err)
		}
		bits := binary.LittleEndian.Uint32(buf)
		data[i] = float64(math.Float32frombits(bits))
	}
	return mat.NewDense(dim, dim, data), nil
}

func writeMatrix(path string, m *mat.Dense, dim int) error {
	// Write to a temp file then rename, so a partial/failed write never
	// leaves a corrupt result.bin behind for a later reader.
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	w := bufio.NewWriterSize(f, 4<<20)

	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(dim))
	if _, err := w.Write(hdr[:]); err != nil {
		f.Close()
		return err
	}

	buf := make([]byte, 4)
	r, c := m.Dims()
	for i := 0; i < r; i++ {
		for j := 0; j < c; j++ {
			binary.LittleEndian.PutUint32(buf, math.Float32bits(float32(m.At(i, j))))
			if _, err := w.Write(buf); err != nil {
				f.Close()
				return err
			}
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func loadManifest(dir string) (*Manifest, error) {
	f, err := os.Open(filepath.Join(dir, manifestName))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var m Manifest
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return nil, err
	}
	return &m, nil
}