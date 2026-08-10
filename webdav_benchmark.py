#!/usr/bin/env python3
"""
WebDAV Throughput Benchmark - curl or rclone transport, one sweep engine.

Usage:
  ./webdav_bench.py curl   <server-ip> [--port 8080] [--size 200] [--repeats 3]
  ./webdav_bench.py rclone <remote-name> [--size 200] [--repeats 3]

  --levels "2 4 8 16 32 64"   override parallel stream counts (default: 2 4 8 16 32 48 64 96 128)
  --budget 8192               tmpfs safety budget in MB (default 8192, leaves headroom under a 12g tmpfs)
"""
import argparse, os, shutil, statistics, subprocess, sys, tempfile, time, threading
from concurrent.futures import ThreadPoolExecutor

LEVELS_DEFAULT = [2, 4, 8, 16, 32, 48, 64, 96, 128]

def die(msg):
    sys.exit(f"ERROR: {msg}")

def run(cmd):
    """Run a command. Returns (success, stderr_text)."""
    r = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    return r.returncode == 0, r.stderr.strip()

class Curl:
    name = "curl"
    def __init__(self, server, port):
        self.base = f"http://{server}:{port}/dav"
    def check(self):
        if not shutil.which("curl"):
            die("curl not found")
    def upload(self, src_file, remote_name):
        return run(["curl", "-T", src_file, f"{self.base}/{remote_name}",
                    "--max-time", "600", "-o", "/dev/null", "-s", "-f"])
    def download(self, remote_name, dst_file):
        return run(["curl", "-o", dst_file, f"{self.base}/{remote_name}",
                    "--max-time", "600", "-s", "-f"])
    def delete(self, remote_name):
        ok, err = run(["curl", "-X", "DELETE", f"{self.base}/{remote_name}", "--max-time", "30", "-s", "-o", "/dev/null"])
        if not ok:
            print(f"\n  [Warning] Curl delete failed for {remote_name}: {err}")

class Rclone:
    name = "rclone"
    def __init__(self, remote):
        self.remote = remote
    def check(self):
        if not shutil.which("rclone"):
            die("rclone not found. Install: curl https://rclone.org/install.sh | sudo bash")
        ok, err = run(["rclone", "lsd", f"{self.remote}:"])
        if not ok:
            die(f"Can't reach remote '{self.remote}': {err}")
    def upload(self, src_file, remote_name):
        return run(["rclone", "copyto", src_file, f"{self.remote}:{remote_name}",
                    "--no-check-dest", "--low-level-retries", "1", "--retries", "1",
                    "--contimeout", "30s", "--timeout", "600s", "--stats=0"])
    def download(self, remote_name, dst_file):
        return run(["rclone", "copyto", f"{self.remote}:{remote_name}", dst_file,
                    "--low-level-retries", "1", "--retries", "1",
                    "--contimeout", "30s", "--timeout", "600s", "--stats=0"])
    def delete(self, remote_name):
        ok, err = run(["rclone", "deletefile", f"{self.remote}:{remote_name}"])
        if not ok:
            print(f"\n  [Warning] Rclone delete failed for {remote_name}: {err}")

def gbit(total_bytes, secs):
    return round((total_bytes * 8) / secs / 1_000_000_000, 3) if secs > 0 else 0.0

def sweep(transport, direction, src_file, size_mb, levels, repeats, tag):
    """direction: 'upload' or 'download'. Returns {N: median_gbit}."""
    results = {}
    for n in levels:
        run_gbits, fails = [], 0
        sample_error = [None]
        for r in range(1, repeats + 1):
            print(f"\r{direction}: {n} streams, run {r}/{repeats}...".ljust(60), end="", flush=True)
            names = [f"{tag}_{direction}_{n}_{r}_{i}" for i in range(n)]
            
            # Barrier ensures threads are fully spawned before starting the timer
            barrier = threading.Barrier(n + 1)
            
            if direction == "upload":
                def worker_up(nm):
                    barrier.wait()
                    return transport.upload(src_file, nm)
                    
                with ThreadPoolExecutor(max_workers=n) as ex:
                    futures = [ex.submit(worker_up, nm) for nm in names]
                    barrier.wait() # Main thread waits for all workers to be ready
                    start = time.time()
                    results_raw = [f.result() for f in futures]
                elapsed = time.time() - start
                
                for nm in names:
                    transport.delete(nm)
            else:
                remote_src = f"{tag}_download_src"
                dst_dir = tempfile.mkdtemp(prefix="webdav_bench_dl_")
                
                def worker_dn(nm):
                    barrier.wait()
                    return transport.download(remote_src, os.path.join(dst_dir, nm))
                    
                with ThreadPoolExecutor(max_workers=n) as ex:
                    futures = [ex.submit(worker_dn, nm) for nm in names]
                    barrier.wait() # Main thread waits for all workers to be ready
                    start = time.time()
                    results_raw = [f.result() for f in futures]
                elapsed = time.time() - start
                
                shutil.rmtree(dst_dir, ignore_errors=True)

            ok = [res[0] for res in results_raw]
            errors = [res[1] for res in results_raw if not res[0] and res[1]]
            success = sum(ok)
            fails += (n - success)
            if errors and sample_error[0] is None:
                sample_error[0] = errors[0]
            
            # Using 1,048,576 bytes because files are created with 1024 * 1024 chunks
            run_gbits.append(gbit(success * size_mb * 1_048_576, elapsed))
            
        med = statistics.median(run_gbits)
        results[n] = med
        status = f"[{fails} total failures across {repeats} runs]" if fails else "[0 failures]"
        print(f"\r{direction.capitalize():10s} ({n:3d} streams): {med:8.3f} Gbit/s (median)  {status}".ljust(70))
        if fails and sample_error[0]:
            print(f"           sample error: {sample_error[0][:200]}")
            sample_error[0] = None
    return results

def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("mode", choices=["curl", "rclone"])
    p.add_argument("target", help="server IP (curl mode) or rclone remote name (rclone mode)")
    p.add_argument("--port", type=int, default=8080, help="curl mode only")
    p.add_argument("--size", type=int, default=200, help="requested per-stream file size in MB")
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--budget", type=int, default=8192, help="tmpfs safety budget in MB")
    p.add_argument("--levels", default=" ".join(map(str, LEVELS_DEFAULT)))
    args = p.parse_args()

    levels = sorted(int(x) for x in args.levels.split())
    max_level = levels[-1]

    # tmpfs safety: +1 accounts for the single src_file that is also in the temp folder
    max_safe = args.budget // (max_level + 1)
    size_mb = min(args.size, max_safe)
    if size_mb < args.size:
        print(f"NOTE: requested {args.size}MB/stream would need {(max_level + 1) * args.size}MB at "
              f"{max_level} streams - capping to {size_mb}MB to fit {args.budget}MB tmpfs budget.")

    transport = Curl(args.target, args.port) if args.mode == "curl" else Rclone(args.target)
    transport.check()

    tmpdir = tempfile.mkdtemp(prefix="webdav_bench_")
    src_file = os.path.join(tmpdir, f"source_{size_mb}MB")
    print(f"Generating {size_mb}MB source file...")
    with open(src_file, "wb") as f:
        chunk = os.urandom(1024 * 1024)
        for _ in range(size_mb):
            f.write(chunk)

    tag = f"bench_{os.getpid()}"

    print(f"\n=== UPLOAD SWEEP ({transport.name}, median of {args.repeats} runs) ===")
    up_results = sweep(transport, "upload", src_file, size_mb, levels, args.repeats, tag)

    print(f"\n=== DOWNLOAD SWEEP ({transport.name}, median of {args.repeats} runs) ===")
    seed_ok, seed_err = transport.upload(src_file, f"{tag}_download_src")
    if not seed_ok:
        die(f"Seed upload for download sweep failed: {seed_err}")
    down_results = sweep(transport, "download", src_file, size_mb, levels, args.repeats, tag)
    transport.delete(f"{tag}_download_src")

    shutil.rmtree(tmpdir, ignore_errors=True)

    print(f"\n=== SUMMARY ({transport.name}, Gbit/s median of {args.repeats} runs) ===")
    print(f"{'Streams':<10}{'Upload':>12}{'Download':>12}")
    best_up = max(up_results.items(), key=lambda kv: kv[1])
    best_down = max(down_results.items(), key=lambda kv: kv[1])
    for n in levels:
        print(f"{n:<10}{up_results[n]:>12.3f}{down_results[n]:>12.3f}")
    print(f"\nMax Upload:   {best_up[1]:.3f} Gbit/s (at {best_up[0]} streams)")
    print(f"Max Download: {best_down[1]:.3f} Gbit/s (at {best_down[0]} streams)")

if __name__ == "__main__":
    main()