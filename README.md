# webdav-benchmark

Two things, run back to back:

1. **iperf3** - raw TCP throughput baseline, no WebDAV/HTTP involved.
2. **webdav-benchmark** - a small Go tool that does real WebDAV operations
   (upload/PUT, download/GET, list-a-directory/PROPFIND, create-a-directory/MKCOL)
   at increasing concurrency and reports median throughput / ops-per-second.

Comparing the two tells you how much overhead WebDAV (and nginx, if you're
going through the proxy) adds on top of the network.

## Build

```bash
cd webdav-benchmark
go build -o webdav-benchmark .
```

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
./bench.sh <target-host> <webdav-url> [size_mb] [repeats]

# through the proxy
./bench.sh 192.168.95.1 http://192.168.95.1:8080/dav 200 3

# direct to backend (bypass nginx)
./bench.sh 192.168.95.2 http://192.168.95.2:8080/dav 200 3
```

## Or run each piece manually

```bash
# baseline (iperf3 server is already running via Ansible/systemd)
iperf3 -c 192.168.95.1 -t 10 -P 4

# WebDAV only
./webdav-benchmark -url http://192.168.95.1:8080/dav -size 200 -repeats 3
./webdav-benchmark -url http://192.168.95.1:8080/dav -ops mkdir,list -repeats 5
```

### Flags

| Flag        | Default                          | Meaning |
|-------------|-----------------------------------|---------|
| `-url`      | *(required)*                      | WebDAV base URL |
| `-size`     | `200`                              | Per-stream file size (MB), upload/download only |
| `-repeats`  | `3`                                 | Repeats per concurrency level (reports the median) |
| `-levels`   | `2 4 8 16 32 48 64`                 | Concurrency levels to sweep |
| `-ops`      | `upload,download,mkdir,list`        | Which operations to run |
| `-timeout`  | `120s`                              | Per-request HTTP timeout |

Output: for each op, a table of concurrency level -> median result
(Gbit/s for upload/download, ops/s for mkdir/list), plus the best level.