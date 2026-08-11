# matrixbench

Synthetic read → multiply → write stress test, targeting `webdav_backend`
(192.168.95.2)'s tmpfs at `/mnt/webdav_ram`.

## 1. Build (on a machine with internet access - NOT the RHEL VMs)

```bash
cd matrixbench
go mod tidy      # fetches gonum, generates go.sum
GOOS=linux GOARCH=amd64 go build -o matrixbench .
```

This produces a single static-ish binary, no runtime deps to install on
the RHEL boxes.

## 2. Bump the tmpfs size

The existing playbook sets `webdav_tmpfs_size: 12g`, which is too small
for a 20GB dataset. It's already been bumped to `24g` in the copy here -
apply it with your normal `run.sh` before proceeding. Confirm the backend
VM actually has enough free RAM first:

```bash
ssh root@192.168.95.2 free -h
```

24GB tmpfs + ~4GB worker working-set + OS/httpd overhead should comfortably
fit in 32GB, but check before committing.

## 3. Deploy the binary

```bash
scp matrixbench root@192.168.95.2:/usr/local/bin/matrixbench
ssh root@192.168.95.2 chmod +x /usr/local/bin/matrixbench
```

## 4. Generate the dataset (once)

```bash
ssh root@192.168.95.2 \
  /usr/local/bin/matrixbench generate \
    -dir /mnt/webdav_ram/matrixbench/dataset \
    -size-gb 20 \
    -dim 4096
```

~312 matrices of 4096x4096 float32 (~64MB each) = ~20GB. Takes a couple
minutes depending on CPU (random float generation, not I/O bound).

## 5. Run the 20-session stability test

```bash
ssh root@192.168.95.2 \
  /usr/local/bin/matrixbench run \
    -dir /mnt/webdav_ram/matrixbench/dataset \
    -results-dir /mnt/webdav_ram/matrixbench/results \
    -sessions 20 \
    -duration 30m \
    -stats /var/log/matrixbench/stats.jsonl \
    -report-interval 10s
```

Prints a live summary every 10s (cycles/sec, error count, degraded
sessions) and a full per-session breakdown at the end. Every cycle's
timing (read/compute/write ms, errors) is logged as JSON lines to
`-stats` for later analysis.

Stop early any time with Ctrl-C (or `kill -TERM`) - it shuts down
cleanly and still prints the final summary.

### Flags worth knowing

- `-duration 0` runs until Ctrl-C instead of a fixed time window
- `-cycles N` caps cycles per session instead of/in addition to duration
- `-max-consecutive-failures N` (default 5) - a session gets flagged
  `DEGRADED` in the summary after N failed cycles in a row, but **keeps
  retrying** rather than exiting. The point of this tool is to find
  where things break, not to stop at the first sign of trouble.

## 6. While it's running, also watch

```bash
ssh root@192.168.95.2 'watch -n2 free -h'          # tmpfs + RAM pressure
ssh root@192.168.95.2 'watch -n2 "df -h /mnt/webdav_ram"'
```

If you see `DEGRADED` sessions or growing error counts in matrixbench's
own output, cross-reference the timestamp against `free -h` output to
see if it's memory pressure, or check `journalctl -k` for OOM kills.

## Design notes

- **Shared dataset, not per-session copies**: all 20 sessions read from
  the same pool of ~312 matrices, picked at random each cycle. This
  mimics concurrent readers hitting a shared corpus (closer to a real
  vector-DB access pattern) rather than needing 20x20GB of ramdisk.
- **Per-session writes**: each session writes only to its own
  `results/session-NN/result.bin`, overwritten each cycle (via
  write-to-temp + atomic rename) so disk usage stays flat and writes
  never collide across sessions.
- **Self-describing matrix files**: each `.bin` file has a 4-byte
  dimension header, so a mismatched dataset/binary can't silently
  produce garbage results.
- **Failures don't kill sessions**: I/O or dimension errors are logged
  and the session keeps looping - that's the "stability" test. A
  session is only marked `DEGRADED` (not stopped) after repeated
  consecutive failures, so you can see exactly when/how the system
  starts struggling under the 20-way concurrent load.