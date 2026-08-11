package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

// Manifest describes the generated dataset so the "run" subcommand doesn't
// need matching flags re-typed by hand - it just reads this file.
type Manifest struct {
	MatrixDim     int       `json:"matrix_dim"`
	MatrixCount   int       `json:"matrix_count"`
	BytesPerFile  int64     `json:"bytes_per_file"`
	TotalBytes    int64     `json:"total_bytes"`
	CreatedAt     time.Time `json:"created_at"`
	Seed          int64     `json:"seed"`
}

const manifestName = "manifest.json"

// matrix file layout: [uint32 dim little-endian][dim*dim float32 little-endian]
// Storing dim in the file itself makes each matrix file self-describing,
// so a stray file or a future dim change can't silently corrupt a run.

func runGenerate(args []string) {
	fs := flag.NewFlagSet("generate", flag.ExitOnError)
	dir := fs.String("dir", "", "target directory for the dataset (required, e.g. /mnt/webdav_ram/matrixbench/dataset)")
	sizeGB := fs.Float64("size-gb", 20.0, "total dataset size in GiB")
	dim := fs.Int("dim", 4096, "matrix dimension (dim x dim square matrices)")
	seed := fs.Int64("seed", 42, "RNG seed, for reproducible datasets")
	force := fs.Bool("force", false, "wipe and regenerate if dir already has a dataset")
	fs.Parse(args)

	if *dir == "" {
		fmt.Fprintln(os.Stderr, "generate: -dir is required")
		fs.Usage()
		os.Exit(2)
	}

	bytesPerFile := int64(*dim) * int64(*dim) * 4 // float32
	totalBytes := int64(*sizeGB * (1 << 30))
	count := int(totalBytes / bytesPerFile)
	if count < 2 {
		fmt.Fprintf(os.Stderr, "generate: -size-gb %.2f too small for dim %d (need at least 2 matrices, %.2f MB each)\n",
			*sizeGB, *dim, float64(bytesPerFile)/(1<<20))
		os.Exit(2)
	}

	manifestPath := filepath.Join(*dir, manifestName)
	if _, err := os.Stat(manifestPath); err == nil && !*force {
		fmt.Fprintf(os.Stderr, "generate: dataset already exists at %s (use -force to regenerate)\n", *dir)
		os.Exit(2)
	}

	if err := os.MkdirAll(*dir, 0o775); err != nil {
		fmt.Fprintf(os.Stderr, "generate: mkdir %s: %v\n", *dir, err)
		os.Exit(1)
	}

	fmt.Printf("generating %d matrices of %dx%d float32 (%.2f MB each, %.2f GB total) into %s\n",
		count, *dim, *dim, float64(bytesPerFile)/(1<<20), float64(int64(count)*bytesPerFile)/(1<<30), *dir)

	rng := rand.New(rand.NewSource(*seed))
	start := time.Now()

	for i := 0; i < count; i++ {
		path := filepath.Join(*dir, fmt.Sprintf("matrix_%05d.bin", i))
		if err := writeRandomMatrix(path, *dim, rng); err != nil {
			fmt.Fprintf(os.Stderr, "generate: writing %s: %v\n", path, err)
			os.Exit(1)
		}
		if (i+1)%10 == 0 || i+1 == count {
			fmt.Printf("  %d/%d matrices written (%.1fs elapsed)\n", i+1, count, time.Since(start).Seconds())
		}
	}

	m := Manifest{
		MatrixDim:    *dim,
		MatrixCount:  count,
		BytesPerFile: bytesPerFile,
		TotalBytes:   int64(count) * bytesPerFile,
		CreatedAt:    time.Now(),
		Seed:         *seed,
	}
	f, err := os.Create(manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "generate: writing manifest: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		fmt.Fprintf(os.Stderr, "generate: encoding manifest: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("done in %.1fs. manifest written to %s\n", time.Since(start).Seconds(), manifestPath)
}

func writeRandomMatrix(path string, dim int, rng *rand.Rand) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 4<<20)
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(dim))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}

	buf := make([]byte, 4)
	n := dim * dim
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint32(buf, math.Float32bits(rng.Float32()))
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	return w.Flush()
}