# Postgres WAL fill-up lab: Debezium with / without a heartbeat table

A local lab that shows a Debezium PostgreSQL connector (`pgoutput`) making `pg_wal` grow
without limit when the captured table is quiet but other tables are busy, and how a
**heartbeat table** (included in the publication and written by `heartbeat.action.query`) stops it.

Stack (`docker-compose.yml`, compose project `pgwal`, isolated from other stacks):

| Service | Container | Host port |
|---|---|---|
| Postgres 15, `wal_level=logical` | `pgwal_postgres` | 5432 (myuser / mypassword / mydatabase) |
| Kafka 3.9 (KRaft) | `pgwal_kafka` | internal only |
| Debezium Connect 2.7.4.Final | `pgwal_connect` | 8083 |
| Kafka UI | `pgwal_kafka_ui` | 8080 |

Postgres is started with `checkpoint_timeout=30s` and `max_wal_size=256MB`, so a healthy
`pg_wal` stays small and WAL removal is visible within seconds. `max_slot_wal_keep_size` is
left at `-1` (unlimited), so a stuck slot keeps WAL forever.

## What gets created (`sql/init.sql`)

| Object | Role |
|---|---|
| `debezium` role (`LOGIN REPLICATION`) | least-privilege CDC user |
| `public.customers` | captured, low-traffic table |
| `public.debezium_heartbeat (id pk, ts)`, seeded with row `id=1` | heartbeat table |
| `public.noise`, `busydb.public.noise` | busy tables in **no** publication |
| `pub_no_hb` = `{customers}` | publication **without** the heartbeat table |
| `pub_hb` = `{customers, debezium_heartbeat}` | publication **with** the heartbeat table |

Three connectors run side by side. Each has its own slot, and all use
`publication.autocreate.mode=disabled` and `table.include.list=public.customers`:

| Connector | Slot | Publication | Heartbeat config |
|---|---|---|---|
| `cdc_no_hb` | `slot_no_hb` | `pub_no_hb` | none |
| `cdc_interval_only` | `slot_interval_only` | `pub_no_hb` | `heartbeat.interval.ms=10000` |
| `cdc_hb` | `slot_hb` | `pub_hb` | `heartbeat.interval.ms=10000` + `heartbeat.action.query=UPDATE public.debezium_heartbeat SET ts = now() WHERE id = 1` |

## Run it

```bash
./sim.sh up                 # start the stack, wait for Postgres + Connect
./sim.sh register all       # create the 3 connectors
./sim.sh monitor            # in a 2nd terminal: live slot / pg_wal view
./sim.sh noise              # uncaptured load in mydatabase until pg_wal >= 3 GB
                            # (`noise busydb` = busy neighbour database instead)
```

Other commands: `status`, `capture`, `heartbeats`, `fix <no-hb|interval-only>` (add the heartbeat
table to the publication and turn on the heartbeat for a running connector), `unfix`,
`guardrail <size|off>`, `drop <variant...|all>`, `hb-row <delete|restore>`, `down` (removes the
volumes). Run `./sim.sh help` for details. You can tune the load with `NOISE_ROWS`, `NOISE_SLEEP`,
`WAL_CAP_MB` and `NOISE_SECONDS`.

**Benchmark:** [BENCHMARK.md](BENCHMARK.md) has a bigger, timed test (about 8 GB of WAL per run,
one setup at a time) with a chart and time-series tables. Run it with `bench/run.sh` and then
`python3 bench/plot.py`.

How to read `monitor`:
- `retained_wal` = `pg_current_wal_lsn() - restart_lsn`: the WAL this slot forces Postgres to keep.
- `confirmed_lsn` only moves when Debezium acknowledges an LSN.
- `pg_wal_dir` is the real disk usage, set by the **worst** slot.

## Why it happens (Postgres 15 + pgoutput + Debezium 2.7)

1. The busy transactions only touch `noise`, which is in no publication. Since PG 15,
   pgoutput **skips empty transactions**, so the slot sends nothing at all to the connector.
2. Debezium moves its offset LSN forward only when it processes a transaction's COMMIT from
   the stream. It sends that LSN to Postgres (`confirmed_flush_lsn`) when Kafka Connect commits
   offsets (`offset.flush.interval.ms`, set to 10 s here). The bundled pgjdbc driver does not
   advance the LSN on keepalives.
3. So with no captured changes, `confirmed_flush_lsn` and `restart_lsn` stay where they are.
   Every WAL segment written after them is kept, and `pg_wal` keeps growing.
4. With a heartbeat table **in the publication**, `heartbeat.action.query` makes a real change
   every 10 s. pgoutput sends that transaction, Debezium processes its COMMIT, the offset moves
   forward, and the next offset commit acknowledges it. Postgres then removes old segments at
   the next checkpoint.

An UPDATE works as well as the INSERT in the docs. Any committed change to a published table
produces a transaction the slot must send. The docs say the same: "inserting a new row or
repeatedly updating the same row". Updating one row keeps the table at one row; the docs'
`INSERT` example grows forever.

The heartbeat table does **not** need to be in `table.include.list`. Its changes reach Debezium
through the publication, are dropped as no-op events (so there is no Kafka data topic for it),
and their COMMIT still moves the LSN forward.

## Demo steps and what happened (measured run, 2026-09-29)

The noise load wrote about 16 MB/s of WAL (20k rows × 1 KB per batch).

### 1. Growth: `./sim.sh noise` (1.6 GB cap)

Sampled every 15 s (`retained_wal @ confirmed_lsn`):

| time | pg_wal | slot_no_hb | slot_interval_only | slot_hb |
|---|---|---|---|---|
| 07:12:19 | 32 MB | 272 kB @ 0/1D6A180 | 272 kB @ 0/1D6A1B8 | 272 kB @ 0/1D6A1F0 |
| 07:12:49 | 528 MB | 505 MB @ 0/1D6A180 | 505 MB @ 0/1D6A1B8 | 505 MB @ 0/9BE8420 |
| 07:13:34 | 1072 MB | 1052 MB @ 0/1D6A180 | 1052 MB @ 0/1D6A1B8 | **274 MB** @ 0/328066C0 |
| 07:14:04 | 1456 MB | 1431 MB @ 0/1D6A180 | 1431 MB @ 0/1D6A1B8 | **526 MB** @ 0/46398738 |
| 07:14:19 | 1600 MB | 1578 MB @ 0/1D6A180 | 1578 MB @ 0/1D6A1B8 | **316 MB** @ 0/56000228 |

- `slot_no_hb`: `confirmed_lsn` never moved and retained WAL tracked the load.
- `slot_interval_only`: **never moved either**, although `__debezium-heartbeat.intonly` got a
  message every 10 s. Its committed Kafka Connect offset stayed at `lsn: 30843320`
  (= `0/1D6A1B8`, the frozen slot position). Heartbeats were flowing, but they all reported the
  same old LSN.
- `slot_hb`: moved on every heartbeat. Retained WAL went up and down between about 270 and
  660 MB. That is roughly load rate × (heartbeat interval + offset flush + Postgres's own delay
  in moving `restart_lsn`), so under heavy load a healthy slot still holds a few hundred MB.
- `pg_wal` grows with the **worst** slot. In this run that was 1.6 GB, while `slot_hb` needed
  only ~300 MB.
- `./sim.sh noise busydb` (load in a neighbour database) gave the same picture: the stuck slots
  grew and `slot_hb` kept moving.

### 2. Captured changes: `./sim.sh capture`

**One captured change did not free the WAL.** It moved `confirmed_lsn` on both stuck slots, but
`restart_lsn`, and so `pg_wal`, stayed at 1578 MB / 1600 MB. `slot_interval_only` released its
WAL after the **2nd** change and `slot_no_hb` only after the **3rd**. Then `pg_wal` fell from
1600 MB to 128 MB at the next checkpoint. Two mechanisms cause this:

- **Postgres:** a logical slot's `restart_lsn` only moves when a confirm passes a *candidate*
  restart point. That candidate was recorded earlier from a running-transactions WAL record, so
  a slot that has been stuck needs one confirm to apply the old candidate and a later one to
  apply a newer candidate.
- **Debezium without heartbeats:** a change record's offset carries the commit LSN of the
  *previous* transaction. The COMMIT of the current one produces no record, so nothing
  acknowledges it and the connector confirms one transaction behind. With
  `heartbeat.interval.ms` set, the heartbeat after each COMMIT carries the current LSN, which is
  why `interval-only` needed one change fewer.

So occasional captured traffic does not reliably protect you either.

### 3. Silent UPDATE failure: `./sim.sh hb-row delete` while noise runs

`slot_hb` froze at `0/6476CFA0` and its retained WAL climbed with the others (211 → 905 MB in
60 s). All connectors stayed RUNNING, and the Connect log had **no error or warning**: the
UPDATE simply matched 0 rows. After `./sim.sh hb-row restore`, retained WAL dropped from 715 MB
to 3 kB on the next beat.

### 4. Guardrail: `./sim.sh guardrail 1GB` with the stuck slots at about 1.3 GB

- At the `CHECKPOINT` both stuck slots went straight to `wal_status = lost`. Postgres killed
  their walsenders and `pg_wal` fell from 1344 MB to 512 MB. `slot_hb` was unaffected.
- **Debezium did not fail.** It logged `Database connection failed when reading from copy`
  (EOFException), restarted the task, and then retried
  `Cannot obtain valid replication slot 'slot_no_hb' ... concurrent tx probably blocks taking snapshot`
  every 2 s, up to 900 times (about 30 min). The hint about a blocking transaction is
  misleading. Throughout, both the connector and the task showed **RUNNING**. Alert on
  `pg_replication_slots.wal_status`, not on connector state.
- That retrying task also **ignores stop and delete requests** ("Graceful stop of task failed").
  In the first attempt, dropping the slot let the orphaned task recreate it with its old
  in-memory offset. That's why `./sim.sh drop` restarts the Connect worker when a lost slot is
  involved.
- Recovery (`./sim.sh drop no-hb interval-only`, `guardrail off`, `register no-hb`,
  `register interval-only`): the new tasks logged "No previous offset found", created fresh
  slots, and completed a new snapshot. Changes made while the slots were lost are gone.
- Sizing: with the 1 GB cap and ~16 MB/s load, `slot_hb`'s `safe_wal_size` dropped as low as
  400 MB. The cap has to stay well above load rate × acknowledgement delay, or healthy slots get
  invalidated too.

### 5. Removing stuck slots without the guardrail

`./sim.sh drop no-hb interval-only` makes `pg_wal` fall back to about `max_wal_size` at the next
checkpoint.

## Trade-offs of the heartbeat table

- **Needed on PG 15+ with a filtered publication.** `heartbeat.interval.ms` alone does not help
  here, because no transactions reach the connector. The docs' "easily solved with
  heartbeat.interval.ms" applies when the connector still receives transactions: before PG 15,
  when pgoutput also sent empty transactions, or when the busy tables are in the publication and
  only filtered out by Debezium.
- **The table must be in the publication.** With `publication.autocreate.mode=filtered`, Debezium
  builds the publication from `table.include.list`, so the heartbeat table has to be in the
  include list too, and its changes are then sent to a Kafka topic
  (`<prefix>.public.debezium_heartbeat`). With `disabled` and a publication you create yourself
  (as here), it stays out of Kafka.
- **The CDC user needs write access** (`UPDATE` + `SELECT` on the heartbeat table). This does not
  work against a read-only source or a standby.
- **Extra writes on the source.** Each beat writes a little WAL and leaves one dead tuple, which
  autovacuum cleans up. The docs' `INSERT` variant makes the table grow forever unless you prune it.
- **The UPDATE heartbeat can stop without any error:**
  - If the row is deleted, the UPDATE matches 0 rows and nothing is written (seen in step 3). Use
    `INSERT … ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts` if you want it to recreate the row.
  - The table needs a primary key or `REPLICA IDENTITY`; without one the UPDATE fails on a
    published table.
  - The publication must publish `update`. That is the default; `WITH (publish = 'insert')`
    would ignore UPDATE heartbeats.
- **Extra Kafka traffic:** a `__debezium-heartbeat.<topic.prefix>` topic with one message per interval.
- **Acknowledgement delay** is about `heartbeat.interval.ms` + `offset.flush.interval.ms`, plus
  Postgres's own delay in moving `restart_lsn`. This bounds, but does not remove, the WAL a
  healthy slot retains: 270–660 MB here at ~16 MB/s with 10 s / 10 s settings. Make both
  intervals short, and keep `max_slot_wal_keep_size` well above this figure.
- **It does nothing while the connector is down.** An inactive slot still retains WAL. You still
  need slot monitoring (`pg_replication_slots`) and ideally `max_slot_wal_keep_size`, which
  protects the disk at the cost of a lost slot, a new snapshot and possibly lost changes (step 4).
- **Don't rely on connector state for alerting.** A connector whose slot is stuck (no heartbeat
  table, a deleted heartbeat row) or even lost (guardrail) still reports RUNNING. Alert on
  `pg_current_wal_lsn() - restart_lsn` and `wal_status` in `pg_replication_slots`.
- **Neighbour databases** (`./sim.sh noise busydb`) cause the same problem, because WAL is
  shared by the whole cluster. This is the case the docs describe for `heartbeat.action.query`.
