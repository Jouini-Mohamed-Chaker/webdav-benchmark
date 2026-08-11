package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
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

	transport := fs.String("transport", "local", "'local' (read/write ramdisk or disk directly on this box) or 'webdav' (read/write over HTTP, direct or through the nginx proxy)")

	// local transport flags
	dir := fs.String("dir", "", "[local] dataset directory produced by 'generate' (required for -transport=local)")
	resultsDir := fs.String("results-dir", "", "[local] directory for per-session result output (required for -transport=local)")

	// webdav transport flags
	webdavURL := fs.String("webdav-url", "", "[webdav] base DAV URL, e.g. http://rhel-backend:8080/dav or http://<proxy-host>:8080/dav (required for -transport=webdav)")
	httpTimeout := fs.Duration("http-timeout", 60*time.Second, "[webdav] per-request HTTP timeout")

	sessions := fs.Int("sessions", 20, "number of concurrent read->multiply->write sessions")
	duration := fs.Duration("duration", 30*time.Minute, "how long to run (0 = until cycles limit or Ctrl-C)")
	cycles := fs.Int("cycles", 0, "max cycles per session (0 = unlimited, bounded by -duration instead)")
	statsPath := fs.String("stats", "matrixbench_stats.jsonl", "path to write per-cycle JSONL stats")
	reportEvery := fs.Duration("report-interval", 10*time.Second, "how often to print a summary to stdout")
	maxConsecFail := fs.Int("max-consecutive-failures", 5, "mark a session degraded after this many consecutive cycle failures (it keeps retrying regardless)")
	fs.Parse(args)

	var datasetStore, resultsStore Storage

	switch *transport {
	case "local":
		if *dir == "" || *resultsDir == "" {
			fmt.Fprintln(os.Stderr, "run -transport=local: -dir and -results-dir are required")
			fs.Usage()
			os.Exit(2)
		}
		if err := os.MkdirAll(*resultsDir, 0o775); err != nil {
			fmt.Fprintf(os.Stderr, "run: mkdir %s: %v\n", *resultsDir, err)
			os.Exit(1)
		}
		datasetStore = newLocalStorage(*dir)
		resultsStore = newLocalStorage(*resultsDir)

	case "webdav":
		if *webdavURL == "" {
			fmt.Fprintln(os.Stderr, "run -transport=webdav: -webdav-url is required, e.g. http://rhel-backend:8080/dav")
			fs.Usage()
			os.Exit(2)
		}
		datasetStore = newWebdavStorage(*webdavURL+"/matrixbench/dataset", *httpTimeout)
		resultsStore = newWebdavStorage(*webdavURL+"/matrixbench/results", *httpTimeout)
		if err := resultsStore.EnsureCollection(""); err != nil {
			fmt.Fprintf(os.Stderr, "run: creating results collection: %v\n", err)
			os.Exit(1)
		}

	default:
		fmt.Fprintf(os.Stderr, "run: unknown -transport %q (want 'local' or 'webdav')\n", *transport)
		os.Exit(2)
	}

	manifest, err := loadManifest(datasetStore)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: loading manifest: %v\n(did you run 'matrixbench generate' first, on the backend?)\n", err)
		os.Exit(1)
	}
	if manifest.MatrixCount < 2 {
		fmt.Fprintln(os.Stderr, "run: dataset has fewer than 2 matrices, can't multiply")
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

	fmt.Printf("matrixbench run: transport=%s sessions=%d dataset=%d matrices (%dx%d, %.2fGB)\n",
		*transport, *sessions, manifest.MatrixCount, manifest.MatrixDim, manifest.MatrixDim,
		float64(manifest.TotalBytes)/(1<<30))
	fmt.Printf("dataset <- %s | results -> %s | stats -> %s | duration=%s cycles=%d(0=unlimited)\n",
		datasetStore.Describe(), resultsStore.Describe(), *statsPath, duration.String(), *cycles)

	var wg sync.WaitGroup
	for i := 0; i < *sessions; i++ {
		wg.Add(1)
		go func(sessionID int) {
			defer wg.Done()

			sessionResults := resultsStore
			if *transport == "local" {
				// Local mode keeps the original per-session subdirectory
				// layout (cheap on a real/tmpfs filesystem, easy to
				// inspect by hand).
				subDir := filepath.Join(*resultsDir, fmt.Sprintf("session-%02d", sessionID))
				if err := os.MkdirAll(subDir, 0o775); err != nil {
					stats.recordFatal(sessionID, fmt.Errorf("mkdir session result dir: %w", err))
					return
				}
				sessionResults = newLocalStorage(subDir)
			}

			runSession(ctx, sessionID, datasetStore, sessionResults, manifest, *cycles, *maxConsecFail, *transport, stats)
		}(i)
	}

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

// runSession repeatedly picks two random matrices from the dataset store,
// reads them, multiplies them, and writes the result to this session's
// result store. Errors are logged and retried, never fatal to the loop -
// that's the stability contract this tool is testing for.
func runSession(ctx context.Context, sessionID int, dataset, results Storage, m *Manifest, maxCycles, maxConsecFail int, transport string, stats *statsSink) {
	rng := rand.New(rand.NewSource(int64(sessionID) + 1))
	consecFail := 0
	cycle := 0

	resultName := "result.bin"
	if transport == "webdav" {
		resultName = fmt.Sprintf("session-%02d-result.bin", sessionID)
	}

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
		a, err := readMatrix(dataset, fmt.Sprintf("matrix_%05d.bin", idxA), m.MatrixDim)
		var b *mat.Dense
		if err == nil {
			b, err = readMatrix(dataset, fmt.Sprintf("matrix_%05d.bin", idxB), m.MatrixDim)
		}
		rec.ReadMS = time.Since(t0).Seconds() * 1000

		if err == nil {
			t1 := time.Now()
			var c mat.Dense
			c.Mul(a, b)
			rec.ComputeMS = time.Since(t1).Seconds() * 1000

			t2 := time.Now()
			err = writeMatrix(results, resultName, &c, m.MatrixDim)
			rec.WriteMS = time.Since(t2).Seconds() * 1000
		}

		rec.TotalMS = time.Since(t0).Seconds() * 1000
		if err != nil {
			consecFail++
			rec.Err = err.Error()
			stats.record(rec)
			if consecFail >= maxConsecFail {
				stats.markDegraded(sessionID, consecFail)
			}
			continue
		}
		consecFail = 0
		stats.record(rec)
	}
}

// --- matrix <-> byte encoding, shared by local and webdav storage ---
// Layout: [uint32 dim little-endian][dim*dim float32 little-endian]

func encodeMatrix(m *mat.Dense, dim int) []byte {
	buf := make([]byte, 4+dim*dim*4)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(dim))
	r, c := m.Dims()
	off := 4
	for i := 0; i < r; i++ {
		for j := 0; j < c; j++ {
			binary.LittleEndian.PutUint32(buf[off:off+4], math.Float32bits(float32(m.At(i, j))))
			off += 4
		}
	}
	return buf
}

func decodeMatrix(data []byte, expectDim int) (*mat.Dense, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("short read: %d bytes", len(data))
	}
	dim := int(binary.LittleEndian.Uint32(data[0:4]))
	if dim != expectDim {
		return nil, fmt.Errorf("dim mismatch: file has %d, manifest says %d", dim, expectDim)
	}
	want := 4 + dim*dim*4
	if len(data) != want {
		return nil, fmt.Errorf("size mismatch: got %d bytes, want %d", len(data), want)
	}
	out := make([]float64, dim*dim)
	off := 4
	for i := range out {
		out[i] = float64(math.Float32frombits(binary.LittleEndian.Uint32(data[off : off+4])))
		off += 4
	}
	return mat.NewDense(dim, dim, out), nil
}

func readMatrix(s Storage, name string, expectDim int) (*mat.Dense, error) {
	data, err := s.Read(name)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	m, err := decodeMatrix(data, expectDim)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", name, err)
	}
	return m, nil
}

func writeMatrix(s Storage, name string, m *mat.Dense, dim int) error {
	if err := s.Write(name, encodeMatrix(m, dim)); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	return nil
}

func loadManifest(s Storage) (*Manifest, error) {
	data, err := s.Read(manifestName)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}