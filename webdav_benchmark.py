#!/usr/bin/env python3
"""
WebDAV Throughput Benchmark - curl (N processes) or rclone (native N-way
concurrency via --transfers/--checkers in a single process) transport.

Usage:
  ./webdav_bench.py curl   <server-ip> [--port 8080] [--size 200] [--repeats 3]
  ./webdav_bench.py rclone <remote-name> [--size 200] [--repeats 3]

  --levels "2 4 8 16 32 64"   override concurrency levels (default: 2 4 8 16 32 48 64 96 128)
                              curl: N separate processes. rclone: N via --transfers/--checkers
                              in one process.
  --budget 8192               tmpfs safety budget in MB (default 8192, leaves headroom under a 12g tmpfs)
"""
import argparse, os, shutil, signal, statistics, subprocess, sys, tempfile, time, threading
from concurrent.futures import ThreadPoolExecutor

LEVELS_DEFAULT = [2, 4, 8, 16, 32, 48, 64, 96, 128]

def die(msg):
    sys.exit(f"ERROR: {msg}")

def run(cmd):
    """Run a command. Returns (success, stderr_text)."""
    r = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    return r.returncode == 0, r.stderr.strip()

def run_capture(cmd):
    """Run a command, capturing stdout. Returns (success, stdout_text, stderr_text)."""
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return r.returncode == 0, r.stdout, r.stderr.strip()

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
    """
    Native-concurrency rclone transport: at concurrency level N, this runs a
    SINGLE `rclone copy` process with --transfers N --checkers N against a
    directory of N files, instead of spawning N separate rclone processes.
    This measures rclone's own internal parallel-transfer efficiency
    (connection reuse, scheduler, etc.) - the way rclone is actually used in
    practice - rather than N independent short-lived client processes.
    """
    name = "rclone"
    def __init__(self, remote):
        self.remote = remote
    def check(self):
        if not shutil.which("rclone"):
            die("rclone not found. Install: curl https://rclone.org/install.sh | sudo bash")
        ok, err = run(["rclone", "lsd", f"{self.remote}:"])
        if not ok:
            die(f"Can't reach remote '{self.remote}': {err}")

    def copy_to_remote(self, local_dir, remote_dir, n):
        """One rclone process, N-way internal concurrency, uploads everything in local_dir."""
        return run(["rclone", "copy", local_dir, f"{self.remote}:{remote_dir}",
                    "--transfers", str(n), "--checkers", str(n),
                    "--no-check-dest", "--low-level-retries", "1", "--retries", "1",
                    "--contimeout", "30s", "--timeout", "600s", "--stats=0"])

    def copy_from_remote(self, remote_dir, local_dir, n):
        """One rclone process, N-way internal concurrency, downloads everything in remote_dir."""
        return run(["rclone", "copy", f"{self.remote}:{remote_dir}", local_dir,
                    "--transfers", str(n), "--checkers", str(n),
                    "--low-level-retries", "1", "--retries", "1",
                    "--contimeout", "30s", "--timeout", "600s", "--stats=0"])

    def count_remote(self, remote_dir):
        """How many files actually landed in remote_dir (post-transfer success count)."""
        ok, out, _ = run_capture(["rclone", "lsf", f"{self.remote}:{remote_dir}"])
        if not ok:
            return 0
        return len([l for l in out.splitlines() if l.strip()])

    def list_root_dirs(self):
        """Top-level directory names on the remote (used to find stale leftovers)."""
        ok, out, err = run_capture(["rclone", "lsf", f"{self.remote}:", "--dirs-only"])
        if not ok:
            print(f"  [Warning] Could not list remote root for cleanup check: {err}")
            return []
        return [l.strip().rstrip("/") for l in out.splitlines() if l.strip()]

    def list_root_files(self):
        """Top-level file names on the remote (used to find stale leftovers)."""
        ok, out, err = run_capture(["rclone", "lsf", f"{self.remote}:", "--files-only"])
        if not ok:
            print(f"  [Warning] Could not list remote root for cleanup check: {err}")
            return []
        return [l.strip() for l in out.splitlines() if l.strip()]

    def purge(self, remote_dir):
        ok, err = run(["rclone", "purge", f"{self.remote}:{remote_dir}"])
        if not ok and "directory not found" not in err.lower():
            print(f"\n  [Warning] Rclone purge failed for {remote_dir}: {err}")
        return ok

    def delete_file(self, remote_name):
        ok, err = run(["rclone", "deletefile", f"{self.remote}:{remote_name}"])
        if not ok and "not found" not in err.lower():
            print(f"\n  [Warning] Rclone delete failed for {remote_name}: {err}")
        return ok

def gbit(total_bytes, secs):
    return round((total_bytes * 8) / secs / 1_000_000_000, 3) if secs > 0 else 0.0

# Tracks remote paths created by THIS run so a crash/Ctrl-C can still clean up.
# (dir_paths, file_paths) - populated as we go, drained as things succeed normally.
_active_remote = {"dirs": set(), "files": set()}

def preflight_cleanup(transport, prefix="bench_"):
    """
    Remove any stale bench_* directories/files left over from a previous run
    that crashed or was killed before it could clean up after itself
    (e.g. the proxy being unreachable mid-run, Ctrl-C, OOM, etc).
    """
    if not hasattr(transport, "list_root_dirs"):
        return  # curl mode has nothing to pre-scan
    stale_dirs = [d for d in transport.list_root_dirs() if d.startswith(prefix)]
    stale_files = [f for f in transport.list_root_files() if f.startswith(prefix)]
    if not stale_dirs and not stale_files:
        return
    print(f"Found {len(stale_dirs)} stale directory(ies) and {len(stale_files)} stale file(s) "
          f"from a previous run - cleaning up before starting...")
    for d in stale_dirs:
        transport.purge(d)
    for f in stale_files:
        transport.delete_file(f)
    print("Pre-flight cleanup done.\n")

def emergency_cleanup(transport):
    """Best-effort cleanup of whatever this run has created so far, called from
    a signal handler or the top-level exception handler."""
    for d in list(_active_remote["dirs"]):
        if hasattr(transport, "purge"):
            transport.purge(d)
        _active_remote["dirs"].discard(d)
    for f in list(_active_remote["files"]):
        if hasattr(transport, "delete_file"):
            transport.delete_file(f)
        elif hasattr(transport, "delete"):
            transport.delete(f)
        _active_remote["files"].discard(f)

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
                
                # FIX: Only attempt to delete files that successfully uploaded
                for nm in names:
                    _active_remote["files"].add(nm)
                for nm, res in zip(names, results_raw):
                    if res[0]: # res[0] is the boolean success flag
                        transport.delete(nm)
                    _active_remote["files"].discard(nm)
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

def sweep_rclone_native(transport, direction, src_file, size_mb, levels, repeats, tag):
    """
    Native-concurrency sweep for rclone: one `rclone copy` process per run,
    using --transfers N --checkers N, instead of N separate processes.
    direction: 'upload' or 'download'. Returns {N: median_gbit}.
    """
    results = {}
    for n in levels:
        run_gbits, fails = [], 0
        sample_error = [None]

        # For downloads, pre-seed a remote dir with N files ONCE per level
        # (unmeasured) so each timed repeat downloads the same N-file set.
        seed_remote_dir = None
        if direction == "download":
            seed_remote_dir = f"{tag}_dlsrc_{n}"
            seed_dir = tempfile.mkdtemp(prefix="webdav_bench_seed_")
            for i in range(n):
                os.link(src_file, os.path.join(seed_dir, f"s{i}"))
            ok, err = transport.copy_to_remote(seed_dir, seed_remote_dir, n)
            shutil.rmtree(seed_dir, ignore_errors=True)
            _active_remote["dirs"].add(seed_remote_dir)
            if not ok or transport.count_remote(seed_remote_dir) < n:
                transport.purge(seed_remote_dir)
                _active_remote["dirs"].discard(seed_remote_dir)
                die(f"Seeding {n} files for download sweep failed: {err}")

        try:
            for r in range(1, repeats + 1):
                print(f"\r{direction}: {n} transfers (native), run {r}/{repeats}...".ljust(60), end="", flush=True)

                if direction == "upload":
                    batch_dir = tempfile.mkdtemp(prefix="webdav_bench_up_")
                    for i in range(n):
                        os.link(src_file, os.path.join(batch_dir, f"u{i}"))
                    remote_dir = f"{tag}_up_{n}_{r}"
                    _active_remote["dirs"].add(remote_dir)

                    start = time.time()
                    ok, err = transport.copy_to_remote(batch_dir, remote_dir, n)
                    elapsed = time.time() - start

                    success = transport.count_remote(remote_dir)
                    transport.purge(remote_dir)
                    _active_remote["dirs"].discard(remote_dir)
                    shutil.rmtree(batch_dir, ignore_errors=True)
                else:
                    dst_dir = tempfile.mkdtemp(prefix="webdav_bench_dn_")

                    start = time.time()
                    ok, err = transport.copy_from_remote(seed_remote_dir, dst_dir, n)
                    elapsed = time.time() - start

                    success = len(os.listdir(dst_dir))
                    shutil.rmtree(dst_dir, ignore_errors=True)

                fails += (n - success)
                if not ok and err and sample_error[0] is None:
                    sample_error[0] = err

                run_gbits.append(gbit(success * size_mb * 1_048_576, elapsed))
        finally:
            if seed_remote_dir:
                transport.purge(seed_remote_dir)
                _active_remote["dirs"].discard(seed_remote_dir)

        med = statistics.median(run_gbits)
        results[n] = med
        status = f"[{fails} total failures across {repeats} runs]" if fails else "[0 failures]"
        print(f"\r{direction.capitalize():10s} ({n:3d} transfers): {med:8.3f} Gbit/s (median)  {status}".ljust(70))
        if fails and sample_error[0]:
            print(f"           sample error: {sample_error[0][:200]}")
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

    preflight_cleanup(transport, prefix="bench_")

    def handle_interrupt(signum, frame):
        print("\n\nInterrupted - cleaning up remote files before exit...")
        emergency_cleanup(transport)
        sys.exit(130)
    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_interrupt)

    tmpdir = tempfile.mkdtemp(prefix="webdav_bench_")
    src_file = os.path.join(tmpdir, f"source_{size_mb}MB")
    print(f"Generating {size_mb}MB source file...")
    with open(src_file, "wb") as f:
        chunk = os.urandom(1024 * 1024)
        for _ in range(size_mb):
            f.write(chunk)

    tag = f"bench_{os.getpid()}"

    try:
        if args.mode == "rclone":
            # Native concurrency: one rclone process per run, --transfers N/--checkers N.
            print(f"\n=== UPLOAD SWEEP ({transport.name} native, median of {args.repeats} runs) ===")
            up_results = sweep_rclone_native(transport, "upload", src_file, size_mb, levels, args.repeats, tag)

            print(f"\n=== DOWNLOAD SWEEP ({transport.name} native, median of {args.repeats} runs) ===")
            down_results = sweep_rclone_native(transport, "download", src_file, size_mb, levels, args.repeats, tag)
        else:
            # curl: N separate processes (curl has no built-in multi-transfer concurrency).
            print(f"\n=== UPLOAD SWEEP ({transport.name}, median of {args.repeats} runs) ===")
            up_results = sweep(transport, "upload", src_file, size_mb, levels, args.repeats, tag)

            print(f"\n=== DOWNLOAD SWEEP ({transport.name}, median of {args.repeats} runs) ===")
            seed_name = f"{tag}_download_src"
            seed_ok, seed_err = transport.upload(src_file, seed_name)
            if not seed_ok:
                die(f"Seed upload for download sweep failed: {seed_err}")
            _active_remote["files"].add(seed_name)
            down_results = sweep(transport, "download", src_file, size_mb, levels, args.repeats, tag)
            transport.delete(seed_name)
            _active_remote["files"].discard(seed_name)
    except Exception:
        print("\n\nRun failed - cleaning up any remote files created so far...")
        emergency_cleanup(transport)
        raise
    finally:
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