# matrixbench

Synthetic read → multiply → write stress test. Two modes:

- **`-transport local`** — runs *on* `rhel-backend`, hits the ramdisk/disk
  directly (no network involved). Good for isolating storage+compute
  behavior from the network stack.
- **`-transport webdav`** — runs *on the Ubuntu client*, does everything
  over HTTP/WebDAV (GET/PUT/MOVE) against `rhel-backend`, either through
  the nginx proxy or hitting httpd/mod_dav directly. This is the one that
  also exercises the network + proxy path, same as your rclone benchmark
  but under sustained concurrent compute load instead of a plain copy.

Both modes share one dataset: `matrixbench generate` writes it directly
into `webdav_serve_path` (the same directory Apache mod_dav publishes at
`/dav`), so anything generated on the backend is automatically reachable
over WebDAV too — no separate upload step.

## 0. `/etc/hosts`

All commands below assume `rhel-backend` resolves to `192.168.95.2`
(you've already got this working). If you want the same for the proxy
box, add an alias and export `PROXY_HOST=that-alias` before running
`matrixbench.sh run-webdav proxy`; otherwise it defaults to the proxy's
IP, `192.168.95.1`.

## 1. tmpfs sizing - now elastic, no more manual math

`webdav_tmpfs_size` in the playbook is now `90%` instead of a fixed `24g`.
tmpfs only consumes RAM as files are actually written into it — the `90%`
is just a ceiling (percentage of *total* physical RAM), so:

- you never have to recalculate a GB number when the dataset size changes
- it won't eat RAM it isn't using
- on the 32GB backend box that's a ~28.8GB ceiling, leaving ~3.2GB for
  OS/httpd - should be comfortable for a 20GB dataset + working set

**Apply it and remount** (a live tmpfs mount doesn't pick up an `opts`
change until it's remounted):

```bash
./run.sh                                             # re-apply playbook
ssh root@rhel-backend mount -o remount,size=90% /mnt/webdav_ram
ssh root@rhel-backend free -h                        # confirm 'shared' looks right
```

Clean up the previous partial dataset before regenerating - the earlier
run died mid-write with `no space left on device`:

```bash
ssh root@rhel-backend rm -rf /mnt/webdav_ram/matrixbench
```

## 2. Build (on a machine with internet access - not the RHEL VMs)

```bash
cd matrixbench
./matrixbench.sh build     # go mod tidy (fetches gonum) + go build
```

## 3. Deploy to rhel-backend

```bash
./matrixbench.sh deploy
```

## 4. Generate the shared dataset (once, on the backend)

```bash
./matrixbench.sh generate 4096 20      # dim=4096, size=20GB
```

This always runs against `rhel-backend` directly over SSH, regardless of
which transport you'll test with later - the dataset only needs to exist
once, in the ramdisk.

## 5. Run the stress test

**Local (on-box, no network):**
```bash
./matrixbench.sh run-local 20 30m
```

**Over WebDAV, through the proxy** (this is the one that also stresses
nginx + the network path):
```bash
./matrixbench.sh run-webdav proxy 20 30m
```

**Over WebDAV, straight to the backend** (bypasses nginx, for
proxy-vs-direct comparison, same pattern as your rclone `webdav_direct`
remote):
```bash
./matrixbench.sh run-webdav direct 20 30m
```

All three print a live summary every 10s (cycles/sec, error count,
degraded-session count) and a full per-session breakdown at the end.
Per-cycle timings (read/compute/write ms + errors) land in a `.jsonl`
file for later analysis - locally for `run-local`, on the client for
`run-webdav`.

Stop early any time with Ctrl-C - it shuts down cleanly and still prints
the final summary.

### Flags worth knowing (call the binary directly for these)

- `-duration 0` runs until Ctrl-C instead of a fixed time window
- `-cycles N` caps cycles per session instead of/in addition to duration
- `-max-consecutive-failures N` (default 5) - a session gets flagged
  `DEGRADED` in the summary after N failed cycles in a row, but **keeps
  retrying** rather than exiting. The point is to find where things
  break, not stop at the first sign of trouble.
- `-http-timeout` (webdav only, default 60s) - per-request timeout,
  useful to tighten if you want failures to surface faster under load

## 6. While it's running, also watch

```bash
ssh root@rhel-backend 'watch -n2 free -h'                    # tmpfs + RAM pressure
ssh root@rhel-backend 'watch -n2 "df -h /mnt/webdav_ram"'
```

For the WebDAV runs, also worth eyeballing nginx/httpd error logs on
`rhel-backend` and the proxy box if you see errors climb in matrixbench's
own output.

## Design notes

- **Shared dataset, not per-session copies**: all sessions read from the
  same pool of ~312 matrices (4096x4096 float32, ~64MB each, ~20GB
  total), picked at random each cycle - mimics concurrent readers hitting
  a shared corpus rather than needing sessions x 20GB of ramdisk.
- **Storage abstraction**: `local` mode uses plain file I/O with
  temp-file + rename for atomic writes; `webdav` mode does the network
  equivalent - PUT to a `.tmp` name, then WebDAV `MOVE` to the final
  name, so a concurrent reader never sees a half-written result. Both
  implement the same `Storage` interface, so the worker loop code is
  identical either way - only the byte-transport differs.
- **Per-session writes never collide**: local mode keeps each session's
  result in its own subdirectory; webdav mode flattens to
  `session-NN-result.bin` filenames (avoids extra `MKCOL` calls) in a
  single `results` collection, created once via `MKCOL` at run start.
- **Failures don't kill sessions**: I/O, HTTP, or dimension errors are
  logged and the session keeps looping - that's the actual stability
  test. A session is only marked `DEGRADED` (not stopped) after repeated
  consecutive failures, so you can see when/how the system starts
  struggling under 20-way concurrent load, on-box or over the network.
- **WebDAV HTTP client is tuned to match the proxy's keepalive pool**
  (256 idle conns) so connection reuse isn't an artificial bottleneck
  compared to what nginx itself is configured for.