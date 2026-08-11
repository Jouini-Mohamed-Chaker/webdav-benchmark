// matrixbench is a synthetic storage+compute stress tool.
//
// It simulates a "vector-db-like" workload: a pool of large matrices sits
// on a target filesystem (ramdisk, tmpfs, or a real disk mount). N
// concurrent sessions each repeatedly:
//
//	1. read two random matrices from the shared pool off disk
//	2. multiply them (CPU-heavy, via gonum)
//	3. write the result back to disk (to a per-session result file)
//
// This exercises sustained concurrent read pressure, CPU, and write
// pressure at the same time, unlike a plain sequential-copy benchmark.
//
// Usage:
//
//	matrixbench generate -dir /mnt/webdav_ram/matrixbench/dataset -size-gb 20 -dim 4096
//	matrixbench run -dir /mnt/webdav_ram/matrixbench/dataset -results-dir /mnt/webdav_ram/matrixbench/results \
//	    -sessions 20 -duration 30m -stats /var/log/matrixbench/stats.jsonl
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "generate":
		runGenerate(os.Args[2:])
	case "run":
		runWorkload(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `matrixbench - synthetic read->multiply->write stress test

Subcommands:
  generate   Create the shared matrix dataset (run locally on the box
             serving the data - it must land inside the WebDAV-served
             path so 'run -transport=webdav' can see it too)
  run        Run N concurrent read->multiply->write sessions, either
             directly on local disk/ramdisk (-transport=local) or over
             HTTP/WebDAV, direct or through the nginx proxy
             (-transport=webdav -webdav-url ...)

Run "matrixbench generate -h" or "matrixbench run -h" for flags.`)
}

// exitOnFlagErr is a small helper so subcommands can share flag.ErrHelp
// handling without duplicating boilerplate.
func exitOnFlagErr(fs *flag.FlagSet, err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fs.Usage()
		os.Exit(2)
	}
}