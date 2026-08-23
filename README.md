# webdav-benchmark

Two things, run back to back:

1. **iperf3** - raw TCP throughput baseline, no WebDAV/HTTP involved.
2. **webdav-benchmark** - a small Go tool that does real WebDAV operations
   (upload/PUT, download/GET, list-a-directory/PROPFIND, create-a-directory/MKCOL)
   at increasing concurrency and reports median throughput / ops-per-second,
   with overhead vs the iperf3 baseline shown inline.

Comparing the two tells you how much overhead WebDAV (and nginx, if you're
going through the proxy) adds on top of the network.

## 1. Provision the VMs

```bash
./run-ansible.sh
```
This configures nginx, httpd/mod_dav, and the disk-backed WebDAV share, and
also deploys/starts an `iperf3` systemd service on the proxy and backend
boxes (restarted cleanly on every re-run, so it never piles up duplicate
processes). No benchmarking happens here - iperf3 is just left running and
ready.

## 2. Run the combined benchmark

```bash
./bench.sh <target-host> <webdav-url> [size_mb] [repeats] [max_level]

# through the proxy
./bench.sh 192.168.95.1 http://192.168.95.1:8080/dav 200 3

# direct to backend (bypass nginx)
./bench.sh 192.168.95.2 http://192.168.95.2:8080/dav 200 3
```

`bench.sh` builds `webdav-benchmark` fresh every time (so it's always
testing your latest code), runs an iperf3 baseline with parallelism matched
to `max_level` (default `32`, matching webdav-benchmark's default
concurrency sweep), then runs the WebDAV benchmark with that baseline wired
in automatically via `-baseline-gbit`.

Stop any run early with **Ctrl+C** - it finishes whatever's currently in
flight, then exits cleanly with a partial report instead of leaving
half-written files or orphaned connections behind.

## Or run each piece manually

```bash
go build -o webdav-benchmark .

# baseline (iperf3 server is already running via Ansible/systemd)
iperf3 -c 192.168.95.1 -t 10 -P 32 -Z -J

# WebDAV only
./webdav-benchmark -url http://192.168.95.1:8080/dav -size 200 -repeats 3
./webdav-benchmark -url http://192.168.95.1:8080/dav -ops mkdir,list -repeats 5
./webdav-benchmark -url http://192.168.95.1:8080/dav -baseline-gbit 27.5
```

### Flags

| Flag             | Default                          | Meaning |
|------------------|-----------------------------------|---------|
| `-url`           | *(required)*                      | WebDAV base URL |
| `-size`          | `200`                              | Per-stream file size (MB), upload/download only |
| `-repeats`       | `3`                                 | Repeats per concurrency level (reports the median) |
| `-levels`        | `2 4 8 16 32`                       | Concurrency levels to sweep (capped at 32 by default - see note below) |
| `-ops`           | `upload,download,mkdir,list`        | Which operations to run |
| `-timeout`       | `120s`                              | Per-request HTTP timeout |
| `-baseline-gbit` | `0` (off)                           | iperf3 Gbit/s to show overhead % against on upload/download lines |
| `-report`        | `results_<timestamp>.txt`           | Path to the plain-text report file |

### Why 32 is the default concurrency cap

Levels above 32 (48/64) were observed to occasionally saturate the backend's
disk writeback and time out an *entire* run instead of degrading gracefully
- a real finding (see below), but not useful as a default since it produces
unreliable/incomplete data. Pass `-levels "2 4 8 16 32 48 64"` if you want
to reproduce that behavior deliberately.

### Reading the output

Every result line is printed once, with overhead built in - no separate
summary table to cross-reference:

```
Upload    2 streams:   5.741 Gbit/s  (79.1% overhead)
Upload    4 streams:  10.186 Gbit/s  (63.0% overhead)
Upload best: 18.726 Gbit/s at 32
```

If a level ever shows WebDAV throughput *higher* than the iperf3 baseline,
the tool prints a note explaining that this is virtually always iperf3
itself becoming CPU-bound on its own stats/accounting at high parallel
stream counts (even with `-Z`/zero-copy), not WebDAV genuinely beating the
network. Treat those specific levels as unreliable and trust the ones with
small, stable, positive overhead instead - typically the lower/mid
concurrency levels (2-8) are where iperf3's numbers hold up best; above
that, cross-check by eye before trusting the percentage.

Both screen output and the report file (`results_<timestamp>.txt` by
default) get the exact same content, written as it happens.