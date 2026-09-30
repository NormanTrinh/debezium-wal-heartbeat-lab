#!/usr/bin/env bash
# Benchmark: how big pg_wal gets with each connector setup. One setup runs at a time,
# so pg_wal on disk shows the effect of that setup only.
#
# Timeline of each run:  idle -> load (noise) -> wait -> after
#   no-hb / interval-only: `sim.sh fix` is applied at the start of "after"
#   hb:                    nothing is changed, we just keep watching
#
# Output: bench/results/{samples.csv,events.csv,env.txt,noise-<run>.log}
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=bench/results
RUNS=${RUNS:-"no-hb interval-only hb"}
IDLE_S=${IDLE_S:-30}
LOAD_S=${LOAD_S:-180}
WAIT_S=${WAIT_S:-60}
AFTER_S=${AFTER_S:-240}
SAMPLE_S=${SAMPLE_S:-5}
export NOISE_ROWS=${NOISE_ROWS:-20000}   # ~22 MB of WAL per batch (small enough to decode without spilling to disk)
export NOISE_SLEEP=${NOISE_SLEEP:-0}
export WAL_CAP_MB=${WAL_CAP_MB:-20000}   # safety limit only; the load stops by time

pg() { docker exec -i pgwal_postgres psql -U myuser -d mydatabase -v ON_ERROR_STOP=1 -Atq "$@"; }

mkdir -p "$OUT"
STATE=$(mktemp -d)
trap 'rm -rf "$STATE"' EXIT

echo "run,t_s,phase,pg_wal_bytes,segments,wal_written_bytes,slot_retained_bytes,slot_confirmed_lsn,slot_wal_status" > "$OUT/samples.csv"
echo "run,t_s,event" > "$OUT/events.csv"

{
  echo "date: $(date -u +%FT%TZ)"
  echo "host: $(nproc) CPU, $(free -g | awk '/Mem:/ {print $2}') GB RAM"
  echo "postgres: $(pg -c 'SHOW server_version')"
  echo "debezium: $(docker inspect pgwal_connect --format '{{.Config.Image}}')"
  pg -F': ' -c "SELECT name, setting || coalesce(unit, '') FROM pg_settings
                WHERE name IN ('wal_level','max_wal_size','min_wal_size','checkpoint_timeout',
                               'max_slot_wal_keep_size','wal_keep_size','logical_decoding_work_mem')
                ORDER BY name"
  echo "noise: NOISE_ROWS=$NOISE_ROWS NOISE_SLEEP=$NOISE_SLEEP"
  echo "timeline: IDLE_S=$IDLE_S LOAD_S=$LOAD_S WAIT_S=$WAIT_S AFTER_S=$AFTER_S SAMPLE_S=$SAMPLE_S"
  echo "heartbeat.interval.ms=10000  offset.flush.interval.ms=10000"
} > "$OUT/env.txt"

sampler() {  # sampler <run> <slot> <start_epoch> <start_lsn_bytes>
  local run=$1 slot=$2 start=$3 lsn0=$4
  while [[ -f $STATE/sampling ]]; do
    pg -F, -c "
      SELECT '$run', round(extract(epoch FROM clock_timestamp()) - $start)::int, '$(cat "$STATE/phase")',
             (SELECT sum(size) FROM pg_ls_waldir()), (SELECT count(*) FROM pg_ls_waldir()),
             (pg_current_wal_lsn() - '0/0') - $lsn0,
             coalesce((SELECT (pg_current_wal_lsn() - restart_lsn)::bigint FROM pg_replication_slots WHERE slot_name = '$slot'), -1),
             coalesce((SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = '$slot'), ''),
             coalesce((SELECT wal_status FROM pg_replication_slots WHERE slot_name = '$slot'), '')" \
      >> "$OUT/samples.csv" || true
    sleep "$SAMPLE_S"
  done
}

event() { echo "$1,$(( $(date +%s) - START )),$2" >> "$OUT/events.csv"; echo "  [$(date +%T)] $1: $2"; }
phase() { echo "$1" > "$STATE/phase"; }

for run in $RUNS; do
  slot=slot_${run//-/_}
  echo "=== run: $run"

  # Clean start: no connectors, no slots, publication back to "customers" only, small pg_wal.
  ./sim.sh drop all >/dev/null
  ./sim.sh unfix >/dev/null
  for _ in $(seq 1 30); do
    pg -c CHECKPOINT
    (( $(pg -c "SELECT (sum(size) / 1048576)::int FROM pg_ls_waldir()") <= 300 )) && break
    sleep 5
  done

  ./sim.sh register "$run" >/dev/null
  for _ in $(seq 1 60); do
    [[ $(pg -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '$slot' AND active") == 1 ]] && break
    sleep 2
  done
  sleep 10  # let the snapshot finish

  START=$(date +%s)
  phase idle
  touch "$STATE/sampling"
  sampler "$run" "$slot" "$START" "$(pg -c "SELECT pg_current_wal_lsn() - '0/0'")" &
  spid=$!
  event "$run" start
  sleep "$IDLE_S"

  phase load; event "$run" load_start
  NOISE_SECONDS=$LOAD_S ./sim.sh noise same > "$OUT/noise-$run.log" 2>&1
  event "$run" load_end

  phase wait
  sleep "$WAIT_S"

  phase after
  if [[ $run != hb ]]; then
    event "$run" fix
    ./sim.sh fix "$run" | sed 's/^/    /'
  fi
  sleep "$AFTER_S"

  event "$run" end
  rm -f "$STATE/sampling"
  wait "$spid"
done

./sim.sh drop all >/dev/null
./sim.sh unfix >/dev/null
echo "Done. Results in $OUT/"
