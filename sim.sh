#!/usr/bin/env bash
# WAL fill-up lab driver. Run `./sim.sh help` for usage.
set -euo pipefail
cd "$(dirname "$0")"

PG_CONTAINER=pgwal_postgres
KAFKA_CONTAINER=pgwal_kafka
CONNECT_URL=${CONNECT_URL:-http://localhost:8083}
NOISE_ROWS=${NOISE_ROWS:-20000}     # rows per noise batch (~1 KB each => ~20 MB WAL)
NOISE_SLEEP=${NOISE_SLEEP:-1}       # seconds between batches
WAL_CAP_MB=${WAL_CAP_MB:-3072}      # noise stops once pg_wal reaches this size
NOISE_SECONDS=${NOISE_SECONDS:-0}   # noise also stops after this many seconds (0 = no time limit)
HB_QUERY="UPDATE public.debezium_heartbeat SET ts = now() WHERE id = 1"
VARIANTS=(no-hb interval-only hb)

psql_() { docker exec -i "$PG_CONTAINER" psql -U myuser -d "${PGDB:-mydatabase}" -v ON_ERROR_STOP=1 "$@"; }
connector_of() { echo "cdc_${1//-/_}"; }   # no-hb -> cdc_no_hb
slot_of() { echo "slot_${1//-/_}"; }       # no-hb -> slot_no_hb

variants() {
  local v=${1:-all}
  if [[ $v == all ]]; then echo "${VARIANTS[@]}"; return; fi
  [[ " ${VARIANTS[*]} " == *" $v "* ]] || { echo "unknown variant '$v' (use: ${VARIANTS[*]} all)" >&2; exit 1; }
  echo "$v"
}

wait_for() {  # wait_for <description> <timeout-seconds> <command...>
  local what=$1 timeout=$2; shift 2
  printf 'Waiting for %s' "$what"
  for ((i = 0; i < timeout; i++)); do
    if "$@" >/dev/null 2>&1; then echo " ok"; return 0; fi
    printf '.'; sleep 1
  done
  echo " timed out" >&2; return 1
}

SLOTS_SQL="
SELECT slot_name, active, wal_status,
       confirmed_flush_lsn AS confirmed_lsn,
       pg_size_pretty(pg_current_wal_lsn() - restart_lsn)         AS retained_wal,
       pg_size_pretty(pg_current_wal_lsn() - confirmed_flush_lsn) AS unacked,
       pg_size_pretty(safe_wal_size)                              AS safe_wal_size
FROM pg_replication_slots ORDER BY slot_name;
SELECT pg_current_wal_lsn() AS current_lsn,
       pg_size_pretty(sum(size)) AS pg_wal_dir,
       count(*) AS segments,
       (SELECT to_char(max(ts), 'HH24:MI:SS') FROM public.debezium_heartbeat) AS heartbeat_row_ts,
       current_setting('max_slot_wal_keep_size') AS max_slot_wal_keep_size
FROM pg_ls_waldir();"

cmd_up() {
  docker compose up -d
  wait_for "postgres" 120 docker exec "$PG_CONTAINER" pg_isready -h 127.0.0.1 -U myuser -d mydatabase
  wait_for "kafka connect REST" 180 curl -sf "$CONNECT_URL/connectors"
  psql_ -c "SHOW wal_level;" -c "SELECT pubname, schemaname || '.' || tablename AS table FROM pg_publication_tables ORDER BY 1, 2;"
  echo "Kafka UI: http://localhost:8080   Connect REST: $CONNECT_URL   Postgres: localhost:5432"
}

cmd_down() { docker compose down -v; }

cmd_register() {
  for v in $(variants "${1:-all}"); do
    local name; name=$(connector_of "$v")
    printf '%-20s ' "$name"
    curl -sf -X PUT -H 'Content-Type: application/json' \
      --data @"connectors/$v.json" "$CONNECT_URL/connectors/$name/config" >/dev/null
    echo "registered (slot $(slot_of "$v"))"
  done
}

cmd_status() {
  curl -sf "$CONNECT_URL/connectors?expand=status" | jq -r '
    to_entries[] | .value.status as $s
    | "\($s.name)\tconnector=\($s.connector.state)\ttask=\(($s.tasks[0].state) // "-")"
      + (if ($s.tasks[0].trace // "") != "" then "\n    " + ($s.tasks[0].trace | split("\n")[0]) else "" end)' \
    | column -t -s $'\t'
  echo
  psql_ <<<"$SLOTS_SQL"
}

cmd_monitor() {
  local interval=${1:-5} out
  while true; do
    out=$(psql_ <<<"$SLOTS_SQL" 2>&1 || true)
    printf '\033[H\033[2J'
    echo "$(date +%T)  refresh ${interval}s  (Ctrl-C to stop)"
    echo "$out"
    sleep "$interval"
  done
}

cmd_noise() {
  local target=${1:-same} db
  case $target in
    same) db=mydatabase ;;
    busydb) db=busydb ;;
    *) echo "noise target must be 'same' or 'busydb'" >&2; exit 1 ;;
  esac
  echo "Writing ${NOISE_ROWS} rows/batch into ${db}.public.noise every ${NOISE_SLEEP}s until pg_wal >= ${WAL_CAP_MB} MB" \
    "$( (( NOISE_SECONDS > 0 )) && echo "or ${NOISE_SECONDS}s have passed ")(Ctrl-C to stop)"
  local wal_mb start=$SECONDS
  while true; do
    wal_mb=$(psql_ -Atc "SELECT (sum(size) / 1048576)::bigint FROM pg_ls_waldir()")
    printf '\r%s  pg_wal = %6s MB ' "$(date +%T)" "$wal_mb"
    if (( wal_mb >= WAL_CAP_MB )); then echo; echo "WAL cap reached (${WAL_CAP_MB} MB), stopping noise."; break; fi
    if (( NOISE_SECONDS > 0 && SECONDS - start >= NOISE_SECONDS )); then echo; echo "Time limit reached (${NOISE_SECONDS}s), stopping noise."; break; fi
    PGDB=$db psql_ -q \
      -c "INSERT INTO public.noise (payload) SELECT repeat(md5(random()::text), 32) FROM generate_series(1, ${NOISE_ROWS})" \
      -c "TRUNCATE public.noise"
    sleep "$NOISE_SLEEP"
  done
}

cmd_fix() {
  # Fix a running connector that has no heartbeat table: add the table to its publication,
  # then turn on heartbeat.interval.ms + heartbeat.action.query (the task restarts, keeps its offset).
  local v; v=$(variants "${1:?usage: fix <no-hb|interval-only>}")
  local name pub; name=$(connector_of "$v"); pub=$(jq -r '."publication.name"' "connectors/$v.json")
  psql_ -c "DO \$\$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_publication_tables
                     WHERE pubname = '$pub' AND schemaname = 'public' AND tablename = 'debezium_heartbeat') THEN
        ALTER PUBLICATION $pub ADD TABLE public.debezium_heartbeat;
      END IF;
    END \$\$" >/dev/null
  echo "$pub now has: $(psql_ -Atc "SELECT string_agg(tablename, ', ') FROM pg_publication_tables WHERE pubname = '$pub'")"
  jq --arg q "$HB_QUERY" '. + {"heartbeat.interval.ms": "10000", "heartbeat.action.query": $q}' "connectors/$v.json" \
    | curl -sf -X PUT -H 'Content-Type: application/json' --data @- "$CONNECT_URL/connectors/$name/config" >/dev/null
  echo "$name: heartbeat.interval.ms=10000 + heartbeat.action.query set"
}

cmd_unfix() {
  # Undo `fix` on the publication (connectors/*.json never contain the heartbeat settings).
  psql_ -c "DO \$\$ BEGIN
      IF EXISTS (SELECT 1 FROM pg_publication_tables
                 WHERE pubname = 'pub_no_hb' AND schemaname = 'public' AND tablename = 'debezium_heartbeat') THEN
        ALTER PUBLICATION pub_no_hb DROP TABLE public.debezium_heartbeat;
      END IF;
    END \$\$" >/dev/null
  echo "pub_no_hb now has: $(psql_ -Atc "SELECT string_agg(tablename, ', ') FROM pg_publication_tables WHERE pubname = 'pub_no_hb'")"
}

cmd_capture() {
  psql_ -c "UPDATE public.customers SET updated_at = now() WHERE id = 1 RETURNING id, name, updated_at;"
  echo "One captured change written; every connector should confirm a newer LSN within ~10-20s."
}

cmd_heartbeats() {
  # cdc_no_hb has no heartbeat topic at all; don't consume it by name (that would auto-create it).
  echo "== heartbeat topics (__debezium-heartbeat.<topic.prefix>)"
  docker exec "$KAFKA_CONTAINER" /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --include '__debezium-heartbeat\..*' --from-beginning --timeout-ms 5000 \
    --property print.key=true --property key.separator='|' 2>/dev/null \
    | jq -R -r 'split("|") | "\(.[0] | fromjson | .payload.serverName) \(.[1] | fromjson | .payload.ts_ms / 1000 | floor | todate)"' \
    | awk '{n[$1]++; last[$1] = $2} END {for (k in n) printf "%-8s %5d heartbeats, last %s\n", k, n[k], last[k]}' || true
  echo "== committed source offsets per connector (LSN Debezium acknowledges to Postgres)"
  docker exec "$KAFKA_CONTAINER" /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic pgwal_offsets --from-beginning --timeout-ms 5000 \
    --property print.key=true --property key.separator='|' 2>/dev/null \
    | awk -F'|' '{last[$1] = $2} END {for (k in last) print k " => " last[k]}' || true
}

cmd_guardrail() {
  local size=${1:?usage: guardrail <size, e.g. 1GB | off>}
  if [[ $size == off ]]; then
    psql_ -c "ALTER SYSTEM RESET max_slot_wal_keep_size"
  else
    psql_ -c "ALTER SYSTEM SET max_slot_wal_keep_size = '$size'"
  fi
  psql_ -c "SELECT pg_reload_conf()" >/dev/null
  sleep 1
  psql_ -c "CHECKPOINT" -c "SHOW max_slot_wal_keep_size"
}

cmd_drop() {
  (( $# )) || { echo "usage: drop <no-hb|interval-only|hb|all> ..." >&2; exit 1; }
  local vs=() v lost=0
  for v in "$@"; do vs+=($(variants "$v")); done

  for v in "${vs[@]}"; do
    local name slot; name=$(connector_of "$v"); slot=$(slot_of "$v")
    echo "== $name"
    [[ $(psql_ -Atc "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '$slot' AND wal_status = 'lost'") == 1 ]] && lost=1
    if curl -sf "$CONNECT_URL/connectors/$name" >/dev/null; then
      # Stop, reset offsets (so a later `register` re-snapshots cleanly), then delete.
      curl -sf -X PUT "$CONNECT_URL/connectors/$name/stop" >/dev/null
      wait_for "$name to stop" 30 bash -c "curl -sf '$CONNECT_URL/connectors/$name/status' | jq -e '.connector.state == \"STOPPED\"'"
      curl -sf -X DELETE "$CONNECT_URL/connectors/$name/offsets" | jq -r '.message' || true
      curl -sf -X DELETE "$CONNECT_URL/connectors/$name" >/dev/null && echo "connector deleted"
    fi
  done

  if (( lost )); then
    # A task whose slot was invalidated sits in Debezium's "Cannot obtain valid replication slot"
    # retry loop (~30 min) and ignores stop requests. If we just dropped the slot, that orphaned
    # task would recreate it with its old offset. Restarting the worker kills it.
    echo "A lost slot was involved: restarting Kafka Connect to kill the orphaned task"
    docker restart pgwal_connect >/dev/null
    wait_for "kafka connect REST" 180 curl -sf "$CONNECT_URL/connectors"
  fi

  for v in "${vs[@]}"; do
    local slot; slot=$(slot_of "$v")
    wait_for "$slot to go inactive" 30 bash -c \
      "[[ \$(docker exec $PG_CONTAINER psql -U myuser -d mydatabase -Atc \"SELECT count(*) FROM pg_replication_slots WHERE slot_name = '$slot' AND active\") == 0 ]]" \
      || psql_ -c "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '$slot' AND active"
    psql_ -Atc "WITH d AS (SELECT slot_name, pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '$slot') SELECT slot_name FROM d" | grep -q . \
      && echo "$slot dropped" || echo "$slot not present"
  done
  psql_ -c "CHECKPOINT" -c "SELECT pg_size_pretty(sum(size)) AS pg_wal_dir, count(*) AS segments FROM pg_ls_waldir()"
}

cmd_hb_row() {
  case ${1:-} in
    delete) psql_ -c "DELETE FROM public.debezium_heartbeat" ;;
    restore) psql_ -c "INSERT INTO public.debezium_heartbeat (id, ts) VALUES (1, now()) ON CONFLICT (id) DO NOTHING" ;;
    *) echo "usage: hb-row <delete|restore>" >&2; exit 1 ;;
  esac
}

cmd_help() {
  cat <<EOF
Usage: ./sim.sh <command> [args]

  up                         start the stack and wait until Postgres + Connect are ready
  down                       stop the stack and DELETE its volumes (full reset)
  register [variant|all]     create/update connector(s): no-hb, interval-only, hb
  status                     connector/task states + replication slot table
  monitor [seconds]          live slot / pg_wal view (default every 5s)
  noise [same|busydb]        write uncaptured load until pg_wal >= WAL_CAP_MB (${WAL_CAP_MB})
  capture                    write one change to the captured table (customers)
  fix <no-hb|interval-only>  add the heartbeat table to its publication + turn on the heartbeat
  unfix                      remove the heartbeat table from pub_no_hb again
  heartbeats                 last heartbeat messages + committed offsets (LSN) per connector
  guardrail <size|off>       set/reset max_slot_wal_keep_size at runtime, then CHECKPOINT
  drop <variant|all> ...     stop + reset offsets + delete connector(s), drop slot(s), CHECKPOINT
                             (restarts Kafka Connect if a slot was lost, to kill the orphaned task)
  hb-row <delete|restore>    remove/restore the heartbeat row (UPDATE heartbeat failure demo)

Env: NOISE_ROWS=${NOISE_ROWS} NOISE_SLEEP=${NOISE_SLEEP} WAL_CAP_MB=${WAL_CAP_MB} NOISE_SECONDS=${NOISE_SECONDS} CONNECT_URL=${CONNECT_URL}
EOF
}

cmd=${1:-help}; shift || true
case $cmd in
  up|down|register|status|monitor|noise|capture|fix|unfix|heartbeats|guardrail|drop|help) "cmd_$cmd" "$@" ;;
  hb-row) cmd_hb_row "$@" ;;
  *) cmd_help; exit 1 ;;
esac
