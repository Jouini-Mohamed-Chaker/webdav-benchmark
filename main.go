// webdav-benchmark exercises a WebDAV server (direct or through a proxy)
// with real WebDAV operations - upload (PUT), download (GET),
// list-a-directory (PROPFIND), create-a-directory (MKCOL) - at increasing
// concurrency levels, and reports median throughput / operations-per-second.
//
// Every run also writes a plain-text report file (results_<timestamp>.txt)
// with the same tables printed to the screen, for later inspection.
//
// Usage:
//
//	webdav-benchmark -url http://192.168.95.1:8080/dav -size 200 -repeats 3
//	webdav-benchmark -url http://192.168.95.2:8080/dav -ops upload,download
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

// defaultConcurrencyLevels is the sweep used when -levels isn't given.
var defaultConcurrencyLevels = []int{2, 4, 8, 16, 32, 48, 64}

func printErrorAndExit(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "ERROR: "+format+"\n", args...)
	os.Exit(1)
}

// =======================================================================
// WebDAVClient: the small set of HTTP/DAV operations the benchmark needs.
// =======================================================================

type WebDAVClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewWebDAVClient(baseURL string, requestTimeout time.Duration) *WebDAVClient {
	return &WebDAVClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: requestTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        256,
				MaxIdleConnsPerHost: 256,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (client *WebDAVClient) urlFor(remoteName string) string {
	return client.baseURL + "/" + remoteName
}

// UploadFile PUTs data to remoteName. Used for the "upload" benchmark.
func (client *WebDAVClient) UploadFile(remoteName string, data []byte) error {
	request, err := http.NewRequest(http.MethodPut, client.urlFor(remoteName), strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	request.ContentLength = int64(len(data))

	response, err := client.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	io.Copy(io.Discard, response.Body)

	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("PUT %s: unexpected status %s", remoteName, response.Status)
	}
	return nil
}

// DownloadFile GETs remoteName and discards the body (we only care about
// how fast the bytes arrive). Used for the "download" benchmark.
func (client *WebDAVClient) DownloadFile(remoteName string) error {
	response, err := client.httpClient.Get(client.urlFor(remoteName))
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		io.Copy(io.Discard, response.Body)
		return fmt.Errorf("GET %s: unexpected status %s", remoteName, response.Status)
	}
	_, err = io.Copy(io.Discard, response.Body)
	return err
}

// DeleteFile removes remoteName. Used for benchmark cleanup; callers
// intentionally ignore the error since a failed cleanup shouldn't fail
// the benchmark itself.
func (client *WebDAVClient) DeleteFile(remoteName string) error {
	request, err := http.NewRequest(http.MethodDelete, client.urlFor(remoteName), nil)
	if err != nil {
		return err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	io.Copy(io.Discard, response.Body)
	return nil
}

// CreateDirectory issues MKCOL. Used for the "mkdir" benchmark, which
// simulates opening/creating folders. 405 (already exists) counts as
// success since it means the collection is usable either way.
func (client *WebDAVClient) CreateDirectory(remoteName string) error {
	request, err := http.NewRequest("MKCOL", client.urlFor(remoteName), nil)
	if err != nil {
		return err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	io.Copy(io.Discard, response.Body)

	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusMethodNotAllowed {
		return fmt.Errorf("MKCOL %s: unexpected status %s", remoteName, response.Status)
	}
	return nil
}

// ListDirectory issues a Depth:1 PROPFIND - what "opening a folder" does
// under the hood in a WebDAV client. Used for the "list" benchmark.
func (client *WebDAVClient) ListDirectory(remoteName string) error {
	requestBody := `<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:allprop/></D:propfind>`
	request, err := http.NewRequest("PROPFIND", client.urlFor(remoteName), strings.NewReader(requestBody))
	if err != nil {
		return err
	}
	request.Header.Set("Depth", "1")
	request.Header.Set("Content-Type", "application/xml")

	response, err := client.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	io.Copy(io.Discard, response.Body)

	const statusMultiStatus = 207
	if response.StatusCode != statusMultiStatus && response.StatusCode != http.StatusOK {
		return fmt.Errorf("PROPFIND %s: unexpected status %s", remoteName, response.Status)
	}
	return nil
}

// =======================================================================
// Concurrency sweep primitive: run an operation N times in parallel,
// starting all goroutines at (as close to) the same instant.
// =======================================================================

// ConcurrencyLevelResult holds the outcome of one concurrency level after
// all repeats: the median result (Gbit/s for transfers, ops/sec for
// mkdir/list) and how many individual operations failed along the way.
type ConcurrencyLevelResult struct {
	ConcurrentStreams int
	MedianResult      float64
	FailedOperations  int
}

// runOperationConcurrently starts `concurrentStreams` goroutines that all
// call operation(streamIndex) at the same instant, waits for all of them,
// and reports how many succeeded plus the total wall-clock time. This is
// the core "load N requests onto the wire together" primitive every
// benchmark below is built from.
func runOperationConcurrently(concurrentStreams int, operation func(streamIndex int) error) (succeeded int, elapsed time.Duration) {
	var allGoroutinesReady sync.WaitGroup
	var startSignal sync.WaitGroup
	allGoroutinesReady.Add(concurrentStreams)
	startSignal.Add(1)

	errorsByStream := make([]error, concurrentStreams)
	var allGoroutinesDone sync.WaitGroup
	allGoroutinesDone.Add(concurrentStreams)

	for i := 0; i < concurrentStreams; i++ {
		go func(streamIndex int) {
			defer allGoroutinesDone.Done()
			allGoroutinesReady.Done()
			startSignal.Wait() // released all at once, right before timing starts
			errorsByStream[streamIndex] = operation(streamIndex)
		}(i)
	}

	allGoroutinesReady.Wait()
	startTime := time.Now()
	startSignal.Done()
	allGoroutinesDone.Wait()
	elapsed = time.Since(startTime)

	for _, err := range errorsByStream {
		if err == nil {
			succeeded++
		}
	}
	return succeeded, elapsed
}

func medianOf(values []float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	count := len(sorted)
	if count == 0 {
		return 0
	}
	if count%2 == 1 {
		return sorted[count/2]
	}
	return (sorted[count/2-1] + sorted[count/2]) / 2
}

func bytesToGbitPerSecond(totalBytes int64, seconds float64) float64 {
	if seconds <= 0 {
		return 0
	}
	return float64(totalBytes*8) / seconds / 1e9
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func remoteNamesFor(runTag, operationName string, concurrentStreams, repeat int) []string {
	names := make([]string, concurrentStreams)
	for i := range names {
		names[i] = fmt.Sprintf("%s_%s_%d_%d_%d", runTag, operationName, concurrentStreams, repeat, i)
	}
	return names
}

// =======================================================================
// Benchmarks
// =======================================================================

// runTransferBenchmark sweeps upload or download throughput across
// concurrency levels. For "download" it first uploads one shared source
// file that every concurrent stream then downloads.
func runTransferBenchmark(client *WebDAVClient, out io.Writer, direction string, fileData []byte, fileSizeMB int, concurrencyLevels []int, repeatsPerLevel int, runTag string) map[int]ConcurrencyLevelResult {
	results := map[int]ConcurrencyLevelResult{}

	sharedDownloadSource := runTag + "_download_source"
	if direction == "download" {
		if err := client.UploadFile(sharedDownloadSource, fileData); err != nil {
			printErrorAndExit("could not seed download source file: %v", err)
		}
		defer client.DeleteFile(sharedDownloadSource)
	}

	for _, streams := range concurrencyLevels {
		var throughputSamples []float64
		failures := 0

		for repeat := 1; repeat <= repeatsPerLevel; repeat++ {
			fmt.Printf("\r%s: %d streams, run %d/%d...          ", direction, streams, repeat, repeatsPerLevel)
			remoteNames := remoteNamesFor(runTag, direction, streams, repeat)

			var succeeded int
			var elapsed time.Duration
			if direction == "upload" {
				succeeded, elapsed = runOperationConcurrently(streams, func(i int) error {
					return client.UploadFile(remoteNames[i], fileData)
				})
				for _, name := range remoteNames {
					client.DeleteFile(name)
				}
			} else {
				succeeded, elapsed = runOperationConcurrently(streams, func(i int) error {
					return client.DownloadFile(sharedDownloadSource)
				})
			}

			failures += streams - succeeded
			transferredBytes := int64(succeeded) * int64(fileSizeMB) * (1 << 20)
			throughputSamples = append(throughputSamples, bytesToGbitPerSecond(transferredBytes, elapsed.Seconds()))
		}

		median := medianOf(throughputSamples)
		results[streams] = ConcurrencyLevelResult{ConcurrentStreams: streams, MedianResult: median, FailedOperations: failures}
		fmt.Fprintf(out, "\r%-10s (%3d streams): %8.3f Gbit/s (median)  [%d failures across %d runs]          \n",
			capitalize(direction), streams, median, failures, repeatsPerLevel)
	}
	return results
}

// runOperationBenchmark sweeps a non-transfer operation (mkdir or list)
// across concurrency levels and reports operations-per-second. When
// cleanupAfterEachRun is set, every remote name created during a run is
// deleted afterwards (used for mkdir; list never creates anything).
func runOperationBenchmark(client *WebDAVClient, out io.Writer, operationName string, concurrencyLevels []int, repeatsPerLevel int, runTag string, operation func(remoteName string) error, cleanupAfterEachRun bool) map[int]ConcurrencyLevelResult {
	results := map[int]ConcurrencyLevelResult{}

	for _, streams := range concurrencyLevels {
		var opsPerSecondSamples []float64
		failures := 0

		for repeat := 1; repeat <= repeatsPerLevel; repeat++ {
			fmt.Printf("\r%s: %d concurrent, run %d/%d...          ", operationName, streams, repeat, repeatsPerLevel)
			remoteNames := remoteNamesFor(runTag, operationName, streams, repeat)

			succeeded, elapsed := runOperationConcurrently(streams, func(i int) error {
				return operation(remoteNames[i])
			})
			if cleanupAfterEachRun {
				for _, name := range remoteNames {
					client.DeleteFile(name)
				}
			}

			failures += streams - succeeded
			if seconds := elapsed.Seconds(); seconds > 0 {
				opsPerSecondSamples = append(opsPerSecondSamples, float64(succeeded)/seconds)
			} else {
				opsPerSecondSamples = append(opsPerSecondSamples, 0)
			}
		}

		median := medianOf(opsPerSecondSamples)
		results[streams] = ConcurrencyLevelResult{ConcurrentStreams: streams, MedianResult: median, FailedOperations: failures}
		fmt.Fprintf(out, "\r%-10s (%3d concurrent): %8.1f ops/s (median)  [%d failures across %d runs]          \n",
			operationName, streams, median, failures, repeatsPerLevel)
	}
	return results
}

func printResultsTable(out io.Writer, title string, unit string, results map[int]ConcurrencyLevelResult, concurrencyLevels []int) {
	fmt.Fprintf(out, "\n=== %s (%s, median) ===\n", title, unit)
	fmt.Fprintf(out, "%-10s%14s\n", "Streams", "Result")

	best := results[concurrencyLevels[0]]
	for _, streams := range concurrencyLevels {
		result := results[streams]
		fmt.Fprintf(out, "%-10d%14.3f\n", streams, result.MedianResult)
		if result.MedianResult > best.MedianResult {
			best = result
		}
	}
	fmt.Fprintf(out, "Best: %.3f %s at %d concurrent\n", best.MedianResult, unit, best.ConcurrentStreams)
}

// =======================================================================
// CLI entry point
// =======================================================================

func main() {
	webdavURL := flag.String("url", "", "WebDAV base URL, e.g. http://192.168.95.1:8080/dav (proxy) or http://192.168.95.2:8080/dav (direct)")
	fileSizeMB := flag.Int("size", 200, "per-stream file size in MB (upload/download only)")
	repeatsPerLevel := flag.Int("repeats", 3, "repeats per concurrency level (median is reported)")
	concurrencyLevelsFlag := flag.String("levels", intsToSpaceSeparated(defaultConcurrencyLevels), "space-separated concurrency levels to sweep")
	operationsFlag := flag.String("ops", "upload,download,mkdir,list", "comma-separated operations to run: upload,download,mkdir,list")
	requestTimeout := flag.Duration("timeout", 120*time.Second, "per-request HTTP timeout")
	reportFilePath := flag.String("report", "", "path to write the plain-text report (default: results_<timestamp>.txt)")
	flag.Parse()

	if *webdavURL == "" {
		printErrorAndExit("-url is required")
	}
	concurrencyLevels := parseConcurrencyLevels(*concurrencyLevelsFlag)
	operations := strings.Split(*operationsFlag, ",")

	if *reportFilePath == "" {
		*reportFilePath = fmt.Sprintf("results_%s.txt", time.Now().Format("20060102_150405"))
	}
	reportFile, err := os.Create(*reportFilePath)
	if err != nil {
		printErrorAndExit("could not create report file %s: %v", *reportFilePath, err)
	}
	defer reportFile.Close()
	out := io.MultiWriter(os.Stdout, reportFile) // every summary line goes to screen + file at once

	client := NewWebDAVClient(*webdavURL, *requestTimeout)
	runTag := fmt.Sprintf("bench_%d", os.Getpid())

	if err := client.ListDirectory(""); err != nil {
		printErrorAndExit("cannot reach %s: %v", *webdavURL, err)
	}

	fmt.Fprintf(out, "Target: %s | size=%dMB | repeats=%d | levels=%v | ops=%v | started=%s\n\n",
		*webdavURL, *fileSizeMB, *repeatsPerLevel, concurrencyLevels, operations, time.Now().Format(time.RFC3339))

	randomFileData := make([]byte, *fileSizeMB<<20)
	rand.Read(randomFileData)

	for _, op := range operations {
		switch strings.TrimSpace(op) {
		case "upload":
			results := runTransferBenchmark(client, out, "upload", randomFileData, *fileSizeMB, concurrencyLevels, *repeatsPerLevel, runTag)
			printResultsTable(out, "UPLOAD", "Gbit/s", results, concurrencyLevels)
		case "download":
			results := runTransferBenchmark(client, out, "download", randomFileData, *fileSizeMB, concurrencyLevels, *repeatsPerLevel, runTag)
			printResultsTable(out, "DOWNLOAD", "Gbit/s", results, concurrencyLevels)
		case "mkdir":
			results := runOperationBenchmark(client, out, "mkdir", concurrencyLevels, *repeatsPerLevel, runTag, client.CreateDirectory, true)
			printResultsTable(out, "MKDIR (MKCOL)", "ops/s", results, concurrencyLevels)
		case "list":
			results := runOperationBenchmark(client, out, "list", concurrencyLevels, *repeatsPerLevel, runTag, func(string) error {
				return client.ListDirectory("")
			}, false)
			printResultsTable(out, "LIST (PROPFIND)", "ops/s", results, concurrencyLevels)
		default:
			fmt.Fprintf(os.Stderr, "skipping unknown operation %q\n", op)
		}
	}

	fmt.Printf("\nFull report written to %s\n", *reportFilePath)
}

func intsToSpaceSeparated(values []int) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = strconv.Itoa(v)
	}
	return strings.Join(parts, " ")
}

func parseConcurrencyLevels(s string) []int {
	fields := strings.Fields(s)
	levels := make([]int, 0, len(fields))
	for _, field := range fields {
		n, err := strconv.Atoi(field)
		if err != nil {
			printErrorAndExit("invalid concurrency level %q: %v", field, err)
		}
		levels = append(levels, n)
	}
	sort.Ints(levels)
	return levels
}