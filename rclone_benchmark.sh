#!/bin/bash
#
# rclone WebDAV Throughput Benchmark
# Run from the Ubuntu client. Uses rclone instead of raw curl loops for
# stability (built-in retries, checksums, clean transfer stats).
#
# Usage:
#   ./rclone_benchmark.sh <remote_name> [file_size_mb] [repeats]
#
# Example (through the proxy, matching normal traffic path):
#   ./rclone_benchmark.sh webdav_proxy 200 3
#
# Example (straight to the backend, for the proxy-vs-direct comparison):
#   ./rclone_benchmark.sh webdav_direct 200 3
#
# <remote_name> must exist in ~/.config/rclone/rclone.conf (see rclone.conf.template)

set -uo pipefail

REMOTE="${1:-}"
FILE_SIZE_MB="${2:-200}"
REPEATS="${3:-3}"
PARALLEL_LEVELS=(${PARALLEL_LEVELS_OVERRIDE:-2 4 8 16 32 48 64 96 128})
TEST_DIR="/tmp/rclone_bench"
REMOTE_SUBDIR="rclone_bench_$$"

BOLD="\033[1m"; GREEN="\033[1;32m"; YELLOW="\033[1;33m"; CYAN="\033[1;36m"; RED="\033[1;31m"; RESET="\033[0m"

die() { echo -e "${RED}ERROR:${RESET} $1"; exit 1; }

[[ -z "$REMOTE" ]] && die "Usage: $0 <remote_name> [file_size_mb] [repeats]"
command -v rclone >/dev/null || die "rclone not found. Install: curl https://rclone.org/install.sh | sudo bash"

rclone lsd "${REMOTE}:" >/dev/null 2>&1 || die "Can't reach remote '${REMOTE}'. Check ~/.config/rclone/rclone.conf and that the server is up."

print_header() {
    echo -e "\n${BOLD}${CYAN}============================================================${RESET}"
    echo -e "${BOLD}${CYAN} $1${RESET}"
    echo -e "${BOLD}${CYAN}============================================================${RESET}"
}

median() {
    local -a vals=($(printf '%s\n' "$@" | sort -n))
    local n=${#vals[@]}
    (( n == 0 )) && { echo "0"; return; }
    if (( n % 2 == 1 )); then
        echo "${vals[$((n/2))]}"
    else
        echo "scale=3; (${vals[$((n/2-1))]} + ${vals[$((n/2))]}) / 2" | bc -l
    fi
}

bytes_to_gbit() {
    local bytes=$1 secs=$2
    (( $(echo "$secs <= 0" | bc -l) )) && { echo "0.00"; return; }
    echo "scale=3; ($bytes * 8) / $secs / 1000000000" | bc -l
}

# ---------- Prep local test files ----------
mkdir -p "$TEST_DIR"
print_header "PREPARING TEST DATA"
echo "Generating ${#PARALLEL_LEVELS[@]} test files of ${FILE_SIZE_MB}MB each in ${TEST_DIR}/upload_src ..."
mkdir -p "${TEST_DIR}/upload_src"
MAX_LEVEL=$(printf '%s\n' "${PARALLEL_LEVELS[@]}" | sort -n | tail -1)
for ((i=1; i<=MAX_LEVEL; i++)); do
    f="${TEST_DIR}/upload_src/file_${i}"
    [[ -f "$f" ]] || dd if=/dev/urandom of="$f" bs=1M count="$FILE_SIZE_MB" status=none
done

declare -A UPLOAD_RESULTS
declare -A DOWNLOAD_RESULTS

# ---------- Upload sweep ----------
print_header "UPLOAD SWEEP via rclone (median of ${REPEATS} runs per level, remote=${REMOTE})"
for N in "${PARALLEL_LEVELS[@]}"; do
    RUN_GBITS=()
    RUN_FAILS=0
    for ((r=1; r<=REPEATS; r++)); do
        echo -ne "${YELLOW}Upload: ${N} parallel streams, run ${r}/${REPEATS}...${RESET}\r"
        # stage exactly N files for this round
        rm -rf "${TEST_DIR}/upload_round"
        mkdir -p "${TEST_DIR}/upload_round"
        for ((i=1; i<=N; i++)); do
            ln "${TEST_DIR}/upload_src/file_${i}" "${TEST_DIR}/upload_round/file_${i}"
        done

        START=$(date +%s.%N)
        rclone copy "${TEST_DIR}/upload_round" "${REMOTE}:${REMOTE_SUBDIR}/up_${N}_${r}" \
            --transfers "$N" --checkers "$N" --no-check-dest \
            --stats=0 --low-level-retries 3 --retries 2 2>"${TEST_DIR}/.err_$$"
        RC=$?
        END=$(date +%s.%N)
        if (( RC != 0 )); then
            RUN_FAILS=$((RUN_FAILS + 1))
        fi

        ELAPSED=$(echo "$END - $START" | bc -l)
        TOTAL_BYTES=$(( N * FILE_SIZE_MB * 1000000 ))
        GBIT=$(bytes_to_gbit "$TOTAL_BYTES" "$ELAPSED")
        RUN_GBITS+=("$GBIT")

        rclone purge "${REMOTE}:${REMOTE_SUBDIR}/up_${N}_${r}" >/dev/null 2>&1
    done
    GBIT_MED=$(median "${RUN_GBITS[@]}")
    UPLOAD_RESULTS[$N]=$GBIT_MED
    if (( RUN_FAILS > 0 )); then
        printf "%-34s ${RED}%s Gbit/s (median)${RESET}   [${RED}%d/%d runs had errors — see %s${RESET}]\n" \
            "Upload  (${N} streams):" "$GBIT_MED" "$RUN_FAILS" "$REPEATS" "${TEST_DIR}/.err_$$"
    else
        printf "%-34s ${GREEN}%s Gbit/s (median)${RESET}   [0 failures across %d runs]\n" \
            "Upload  (${N} streams):" "$GBIT_MED" "$REPEATS"
    fi
done

# ---------- Download sweep ----------
print_header "DOWNLOAD SWEEP via rclone (median of ${REPEATS} runs per level)"
echo "Seeding ${MAX_LEVEL} source files on remote for download..."
rclone copy "${TEST_DIR}/upload_src" "${REMOTE}:${REMOTE_SUBDIR}/download_src" \
    --transfers 16 --checkers 16 --stats=0 || die "Seed upload failed — check server/proxy before continuing."

for N in "${PARALLEL_LEVELS[@]}"; do
    RUN_GBITS=()
    RUN_FAILS=0
    for ((r=1; r<=REPEATS; r++)); do
        echo -ne "${YELLOW}Download: ${N} parallel streams, run ${r}/${REPEATS}...${RESET}\r"
        rm -rf "${TEST_DIR}/download_dst"
        mkdir -p "${TEST_DIR}/download_dst"

        # build an include filter for just the first N files
        INCLUDE_FILE="${TEST_DIR}/.include_$$"
        > "$INCLUDE_FILE"
        for ((i=1; i<=N; i++)); do echo "file_${i}" >> "$INCLUDE_FILE"; done

        START=$(date +%s.%N)
        rclone copy "${REMOTE}:${REMOTE_SUBDIR}/download_src" "${TEST_DIR}/download_dst" \
            --transfers "$N" --checkers "$N" --files-from "$INCLUDE_FILE" \
            --stats=0 --low-level-retries 3 --retries 2 2>"${TEST_DIR}/.err_$$"
        RC=$?
        END=$(date +%s.%N)
        rm -f "$INCLUDE_FILE"
        if (( RC != 0 )); then
            RUN_FAILS=$((RUN_FAILS + 1))
        fi

        ELAPSED=$(echo "$END - $START" | bc -l)
        TOTAL_BYTES=$(( N * FILE_SIZE_MB * 1000000 ))
        GBIT=$(bytes_to_gbit "$TOTAL_BYTES" "$ELAPSED")
        RUN_GBITS+=("$GBIT")
    done
    GBIT_MED=$(median "${RUN_GBITS[@]}")
    DOWNLOAD_RESULTS[$N]=$GBIT_MED
    if (( RUN_FAILS > 0 )); then
        printf "%-34s ${RED}%s Gbit/s (median)${RESET}   [${RED}%d/%d runs had errors${RESET}]\n" \
            "Download(${N} streams):" "$GBIT_MED" "$RUN_FAILS" "$REPEATS"
    else
        printf "%-34s ${GREEN}%s Gbit/s (median)${RESET}   [0 failures across %d runs]\n" \
            "Download(${N} streams):" "$GBIT_MED" "$REPEATS"
    fi
done

rclone purge "${REMOTE}:${REMOTE_SUBDIR}" >/dev/null 2>&1

# ---------- Summary ----------
print_header "SUMMARY - rclone WebDAV THROUGHPUT (Gbit/s, median of ${REPEATS} runs, remote=${REMOTE})"
printf "${BOLD}%-12s %15s %15s${RESET}\n" "Streams" "Upload" "Download"
echo "------------------------------------------------"
BEST_UP=0; BEST_UP_N=1
BEST_DOWN=0; BEST_DOWN_N=1
for N in "${PARALLEL_LEVELS[@]}"; do
    UP="${UPLOAD_RESULTS[$N]:-N/A}"
    DOWN="${DOWNLOAD_RESULTS[$N]:-N/A}"
    printf "%-12s %15s %15s\n" "$N" "$UP" "$DOWN"
    if [[ "$UP" != "N/A" ]] && (( $(echo "$UP > $BEST_UP" | bc -l) )); then BEST_UP=$UP; BEST_UP_N=$N; fi
    if [[ "$DOWN" != "N/A" ]] && (( $(echo "$DOWN > $BEST_DOWN" | bc -l) )); then BEST_DOWN=$DOWN; BEST_DOWN_N=$N; fi
done
echo "------------------------------------------------"
echo -e "${BOLD}${GREEN}Max Upload:   ${BEST_UP} Gbit/s   (at ${BEST_UP_N} parallel streams)${RESET}"
echo -e "${BOLD}${GREEN}Max Download: ${BEST_DOWN} Gbit/s   (at ${BEST_DOWN_N} parallel streams)${RESET}"
echo ""
echo -e "${CYAN}This is your baseline. Re-run with remote=webdav_direct to compare vs going straight to the backend.${RESET}"