// webdav-benchmark exercises a WebDAV server (direct or through a proxy)
// with real WebDAV operations - PUT (upload), GET (download), PROPFIND
// (list a directory), MKCOL (create a directory) - at increasing
// concurrency levels, and reports median throughput / ops-per-second.
//
// Meant to be run alongside iperf3 (see bench.sh) so raw network
// throughput and WebDAV throughput can be compared directly to show how
// much overhead WebDAV adds on top of the network.
//
// Usage:
//
//	webdav-benchmark -url http://192.168.95.1:8080/dav -size 200 -repeats 3
//	webdav-benchmark -url http://192.168.95.2:8080/dav -size 200 -ops upload,download
package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var levelsDefault = []int{2, 4, 8, 16, 32, 48, 64}

func die(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "ERROR: "+format+"\n", a...)
	os.Exit(1)
}

// ---------------------------------------------------------------------
// WebDAV client - minimal set of operations needed for the benchmark.
// ---------------------------------------------------------------------

type davClient struct {
	base   string
	client *http.Client
}

func newDavClient(base string, timeout time.Duration) *davClient {
	return &davClient{
		base: strings.TrimRight(base, "/"),
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        256,
				MaxIdleConnsPerHost: 256,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (d *davClient) url(name string) string { return d.base + "/" + name }

func (d *davClient) upload(name string, data []byte) error {
	req, err := http.NewRequest(http.MethodPut, d.url(name), newBytesReader(data))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(data))
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("PUT %s: status %s", name, resp.Status)
	}
	return nil
}

func (d *davClient) download(name string) (int64, error) {
	resp, err := d.client.Get(d.url(name))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return 0, fmt.Errorf("GET %s: status %s", name, resp.Status)
	}
	n, err := io.Copy(io.Discard, resp.Body)
	return n, err
}

func (d *davClient) delete(name string) error {
	req, _ := http.NewRequest(http.MethodDelete, d.url(name), nil)
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return nil
}

func (d *davClient) mkcol(name string) error {
	req, err := http.NewRequest("MKCOL", d.url(name), nil)
	if err != nil {
		return err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	// 201 created, 405 already exists - both fine for benchmarking purposes.
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusMethodNotAllowed {
		return fmt.Errorf("MKCOL %s: status %s", name, resp.Status)
	}
	return nil
}

// propfind lists a directory (Depth: 1) - this is what "opening a folder"
// does under the hood in a WebDAV client/OS integration.
func (d *davClient) propfind(name string) error {
	body := `<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:allprop/></D:propfind>`
	req, err := http.NewRequest("PROPFIND", d.url(name), newBytesReader([]byte(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Depth", "1")
	req.Header.Set("Content-Type", "application/xml")
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 207 && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("PROPFIND %s: status %s", name, resp.Status)
	}
	return nil
}

func newBytesReader(b []byte) *strings.Reader { return strings.NewReader(string(b)) }

// ---------------------------------------------------------------------
// Sweep logic - run each operation at increasing concurrency, N times,
// report the median.
// ---------------------------------------------------------------------

type levelResult struct {
	n      int
	median float64 // Gbit/s for upload/download, ops/s for list/mkdir
	fails  int
}

func gbit(totalBytes int64, secs float64) float64 {
	if secs <= 0 {
		return 0
	}
	return float64(totalBytes*8) / secs / 1e9
}

func median(xs []float64) float64 {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	n := len(s)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

// runAtConcurrency runs fn n times concurrently (one goroutine each,
// released together via a WaitGroup barrier) and returns how many
// succeeded plus the wall-clock elapsed time.
func runAtConcurrency(n int, fn func(i int) error) (success int, elapsed time.Duration) {
	var ready, start sync.WaitGroup
	ready.Add(n)
	start.Add(1)
	results := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			ready.Done()
			start.Wait()
			results[i] = fn(i)
		}(i)
	}
	ready.Wait()
	t0 := time.Now()
	start.Done()
	wg.Wait()
	elapsed = time.Since(t0)
	for _, e := range results {
		if e == nil {
			success++
		}
	}
	return
}

func sweepTransfer(d *davClient, direction string, srcData []byte, sizeMB int, levels []int, repeats int, tag string) map[int]levelResult {
	out := map[int]levelResult{}
	remoteSeed := tag + "_dl_src"
	if direction == "download" {
		if err := d.upload(remoteSeed, srcData); err != nil {
			die("seeding download source failed: %v", err)
		}
		defer d.delete(remoteSeed)
	}

	for _, n := range levels {
		var gbits []float64
		fails := 0
		for r := 1; r <= repeats; r++ {
			fmt.Printf("\r%s: %d streams, run %d/%d...          ", direction, n, r, repeats)
			names := make([]string, n)
			for i := range names {
				names[i] = fmt.Sprintf("%s_%s_%d_%d_%d", tag, direction, n, r, i)
			}
			var success int
			var elapsed time.Duration
			if direction == "upload" {
				success, elapsed = runAtConcurrency(n, func(i int) error {
					return d.upload(names[i], srcData)
				})
				for i := 0; i < n; i++ {
					d.delete(names[i])
				}
			} else {
				success, elapsed = runAtConcurrency(n, func(i int) error {
					_, err := d.download(remoteSeed)
					return err
				})
			}
			fails += n - success
			gbits = append(gbits, gbit(int64(success)*int64(sizeMB)*1<<20, elapsed.Seconds()))
		}
		med := median(gbits)
		out[n] = levelResult{n: n, median: med, fails: fails}
		fmt.Printf("\r%-10s (%3d streams): %8.3f Gbit/s (median)  [%d failures across %d runs]          \n",
			strings.Title(direction), n, med, fails, repeats)
	}
	return out
}

func sweepOps(d *davClient, opName string, levels []int, repeats int, tag string, op func(name string) error, needsCleanup bool) map[int]levelResult {
	out := map[int]levelResult{}
	for _, n := range levels {
		var rates []float64
		fails := 0
		for r := 1; r <= repeats; r++ {
			fmt.Printf("\r%s: %d concurrent, run %d/%d...          ", opName, n, r, repeats)
			names := make([]string, n)
			for i := range names {
				names[i] = fmt.Sprintf("%s_%s_%d_%d_%d", tag, opName, n, r, i)
			}
			success, elapsed := runAtConcurrency(n, func(i int) error {
				return op(names[i])
			})
			if needsCleanup {
				for i := 0; i < n; i++ {
					d.delete(names[i])
				}
			}
			fails += n - success
			secs := elapsed.Seconds()
			if secs > 0 {
				rates = append(rates, float64(success)/secs)
			} else {
				rates = append(rates, 0)
			}
		}
		med := median(rates)
		out[n] = levelResult{n: n, median: med, fails: fails}
		fmt.Printf("\r%-10s (%3d concurrent): %8.1f ops/s (median)  [%d failures across %d runs]          \n",
			opName, n, med, fails, repeats)
	}
	return out
}

func printSummary(title, unit string, results map[int]levelResult, levels []int) {
	fmt.Printf("\n=== %s (%s, median) ===\n", title, unit)
	fmt.Printf("%-10s%14s\n", "Streams", "Result")
	best := results[levels[0]]
	for _, n := range levels {
		r := results[n]
		fmt.Printf("%-10d%14.3f\n", n, r.median)
		if r.median > best.median {
			best = r
		}
	}
	fmt.Printf("Best: %.3f %s at %d\n", best.median, unit, best.n)
}

func main() {
	url := flag.String("url", "", "WebDAV base URL, e.g. http://192.168.95.1:8080/dav (proxy) or http://192.168.95.2:8080/dav (direct)")
	sizeMB := flag.Int("size", 200, "per-stream file size in MB (upload/download only)")
	repeats := flag.Int("repeats", 3, "repeats per concurrency level")
	levelsStr := flag.String("levels", intsToStr(levelsDefault), "space-separated concurrency levels")
	opsStr := flag.String("ops", "upload,download,mkdir,list", "comma-separated ops to run: upload,download,mkdir,list")
	timeout := flag.Duration("timeout", 120*time.Second, "per-request HTTP timeout")
	flag.Parse()

	if *url == "" {
		die("-url is required")
	}
	levels := parseLevels(*levelsStr)
	ops := strings.Split(*opsStr, ",")

	d := newDavClient(*url, *timeout)
	tag := fmt.Sprintf("bench_%d", os.Getpid())

	// smoke test the server is reachable before starting
	if err := d.propfind(""); err != nil {
		die("cannot reach %s: %v", *url, err)
	}

	fmt.Printf("Target: %s | size=%dMB | repeats=%d | levels=%v | ops=%v\n\n", *url, *sizeMB, *repeats, levels, ops)

	srcData := make([]byte, *sizeMB<<20)
	rand.Read(srcData)

	for _, op := range ops {
		switch strings.TrimSpace(op) {
		case "upload":
			res := sweepTransfer(d, "upload", srcData, *sizeMB, levels, *repeats, tag)
			printSummary("UPLOAD", "Gbit/s", res, levels)
		case "download":
			res := sweepTransfer(d, "download", srcData, *sizeMB, levels, *repeats, tag)
			printSummary("DOWNLOAD", "Gbit/s", res, levels)
		case "mkdir":
			res := sweepOps(d, "mkdir", levels, *repeats, tag, d.mkcol, true)
			printSummary("MKDIR (MKCOL)", "ops/s", res, levels)
		case "list":
			// list the server root repeatedly - simulates "opening a folder"
			res := sweepOps(d, "list", levels, *repeats, tag, func(name string) error {
				return d.propfind("")
			}, false)
			printSummary("LIST (PROPFIND)", "ops/s", res, levels)
		default:
			fmt.Fprintf(os.Stderr, "skipping unknown op %q\n", op)
		}
	}
}

func intsToStr(xs []int) string {
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = strconv.Itoa(x)
	}
	return strings.Join(parts, " ")
}

func parseLevels(s string) []int {
	fields := strings.Fields(s)
	out := make([]int, 0, len(fields))
	for _, f := range fields {
		n, err := strconv.Atoi(f)
		if err != nil {
			die("invalid level %q: %v", f, err)
		}
		out = append(out, n)
	}
	sort.Ints(out)
	return out
}