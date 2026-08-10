#!/usr/bin/env python3
"""
WebDAV Throughput Benchmark - curl or rclone transport, one sweep engine.

Usage:
  ./webdav_bench.py curl   <server-ip> [--port 8080] [--size 200] [--repeats 3]
  ./webdav_bench.py rclone <remote-name> [--size 200] [--repeats 3]

  --levels "2 4 8 16 32 64"   override parallel stream counts (default: 2 4 8 16 32 48 64 96 128)
  --budget 8192               tmpfs safety budget in MB (default 8192, leaves headroom under a 12g tmpfs)

Safety: --size is auto-capped so max_streams * size never exceeds --budget,
same guardrail regardless of transport (this is what caused 507s before).

Requires: curl mode needs `curl`; rclone mode needs `rclone` configured with
the given remote name (see rclone.conf.template) pointed at .../dav.
"""
import argparse, os, shutil, statistics, subprocess, sys, tempfile, time
from concurrent.futures import ThreadPoolExecutor

LEVELS_DEFAULT = [2, 4, 8, 16, 32, 48, 64, 96, 128]

def die(msg):
    sys.exit(f"ERROR: {msg}")

def run(cmd):
    """Run a command, return True on success (exit 0), False otherwise."""
    return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0

class Curl:
    name = "curl"
    def __init__(self, server, port):
        self.base = f"http://{server}:{port}/dav"
    def check(self):
        if not shutil.which("curl"):
            die("curl not found")
    def upload(self, src_file, remote_name):
        return run(["curl", "-T", src_file, f"{self.base}/{remote_name}",
                     "--max-time", "15", "-o", "/dev/null", "-s", "-f"])
    def download(self, remote_name, dst_file):
        return run(["curl", "-o", dst_file, f"{self.base}/{remote_name}",
                     "--max-time", "15", "-s", "-f"])
    def delete(self, remote_name):
        run(["curl", "-X", "DELETE", f"{self.base}/{remote_name}", "--max-time", "10", "-s", "-o", "/dev/null"])

class Rclone:
    name = "rclone"
    def __init__(self, remote):
        self.remote = remote
    def check(self):
        if not shutil.which("rclone"):
            die("rclone not found. Install: curl https://rclone.org/install.sh | sudo bash")
        if not run(["rclone", "lsd", f"{self.remote}:"]):
            die(f"Can't reach remote '{self.remote}'. Check ~/.config/rclone/rclone.conf and that the server is up.")
    def upload(self, src_file, remote_name):
        return run(["rclone", "copyto", src_file, f"{self.remote}:{remote_name}",
                     "--low-level-retries", "3", "--retries", "2", "--stats=0"])
    def download(self, remote_name, dst_file):
        return run(["rclone", "copyto", f"{self.remote}:{remote_name}", dst_file,
                     "--low-level-retries", "3", "--retries", "2", "--stats=0"])
    def delete(self, remote_name):
        run(["rclone", "deletefile", f"{self.remote}:{remote_name}"])

def gbit(total_bytes, secs):
    return round((total_bytes * 8) / secs / 1_000_000_000, 3) if secs > 0 else 0.0

def sweep(transport, direction, src_file, size_mb, levels, repeats, tag):
    """direction: 'upload' or 'download'. Returns {N: median_gbit}."""
    results = {}
    for n in levels:
        run_gbits, fails = [], 0
        for r in range(1, repeats + 1):
            print(f"\r{direction}: {n} streams, run {r}/{repeats}...".ljust(60), end="", flush=True)
            names = [f"{tag}_{direction}_{n}_{r}_{i}" for i in range(n)]
            if direction == "upload":
                start = time.time()
                with ThreadPoolExecutor(max_workers=n) as ex:
                    ok = list(ex.map(lambda nm: transport.upload(src_file, nm), names))
                elapsed = time.time() - start
                for nm in names:
                    transport.delete(nm)
            else:
                # single shared remote source, fetched N times concurrently
                remote_src = f"{tag}_download_src"
                dst_dir = tempfile.mkdtemp(prefix="webdav_bench_dl_")
                start = time.time()
                with ThreadPoolExecutor(max_workers=n) as ex:
                    ok = list(ex.map(lambda nm: transport.download(remote_src, os.path.join(dst_dir, nm)), names))
                elapsed = time.time() - start
                shutil.rmtree(dst_dir, ignore_errors=True)

            success = sum(ok)
            fails += (n - success)
            run_gbits.append(gbit(success * size_mb * 1_000_000, elapsed))
        med = statistics.median(run_gbits)
        results[n] = med
        status = f"[{fails} total failures across {repeats} runs]" if fails else "[0 failures]"
        print(f"\r{direction.capitalize():10s} ({n:3d} streams): {med:8.3f} Gbit/s (median)  {status}".ljust(70))
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

    # tmpfs safety: cap size so max_streams * size never exceeds the budget,
    # same guardrail for both transports.
    max_safe = args.budget // max_level
    size_mb = min(args.size, max_safe)
    if size_mb < args.size:
        print(f"NOTE: requested {args.size}MB/stream would need {max_level * args.size}MB at "
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
    transport.upload(src_file, f"{tag}_download_src")
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