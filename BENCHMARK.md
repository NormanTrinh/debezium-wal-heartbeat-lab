# Benchmark: Postgres WAL size with and without a Debezium heartbeat table

Date: 2026-09-29. Lab: this repo (`docker-compose.yml`, `sim.sh`). Raw data: [bench/results/](bench/results/).

Short on time? Read the [Conclusion](#conclusion) and
[When do you need a heartbeat table?](#when-do-you-need-a-heartbeat-table)

## Summary

- **Without a heartbeat table, Postgres keeps all the WAL.** About 7.7 GB was written in
  3 minutes. `pg_wal` grew to 7.8 GB and stayed there until we changed something.
- **Heartbeat messages alone (`heartbeat.interval.ms`) do not help.** The result is the same
  as with no heartbeat: 7.7 GB kept.
- **With a heartbeat table in the publication, the WAL goes down again and again.** Under the
  same load, `pg_wal` never went above 2.9 GB, and it went down many times (as low as 1.1 GB).
  It went back to 256 MB **33 s** after the load stopped, without any change.
- **Adding the heartbeat table later also works.** For the first two setups, we added the
  heartbeat table to the running connector at 4:30. Then `pg_wal` went from 7.8 GB to 224 MB in
  95 s (no heartbeat), and from 7.7 GB to 256 MB in 61 s (heartbeat messages only).

![pg_wal size over time for each setup](bench/results/pg_wal.svg)

*Each panel is one run. The x axis is the time since the run started (minutes). The y axis is
a size in GB. The colored line is the size of `pg_wal` on disk. The gray line is all
WAL written since the run started. When the two lines are the same, Postgres deleted nothing.
The vertical line is the moment we added the heartbeat table to that connector.*

## What we tested

Debezium reads changes from a replication slot. Postgres must keep every WAL file that the slot
has not confirmed yet. We capture only one quiet table (`customers`). A busy table (`noise`)
is in no publication, so Debezium never gets an event for it. The question: **does the slot
keep moving forward, so Postgres can delete old WAL?**

| Setup | Connector | Publication | Heartbeat settings |
|---|---|---|---|
| No heartbeat | `cdc_no_hb` | `pub_no_hb` = {customers} | none |
| Heartbeat messages only | `cdc_interval_only` | `pub_no_hb` = {customers} | `heartbeat.interval.ms=10000` |
| Heartbeat table | `cdc_hb` | `pub_hb` = {customers, debezium_heartbeat} | `heartbeat.interval.ms=10000` + `heartbeat.action.query=UPDATE public.debezium_heartbeat SET ts = now() WHERE id = 1` |

## How we tested

**Environment**

| Item | Value |
|---|---|
| Host | 8 CPU, 23 GB RAM, Docker |
| Postgres | 15.14, `wal_level=logical`, `max_wal_size=256MB`, `min_wal_size=80MB`, `checkpoint_timeout=30s`, `max_slot_wal_keep_size=-1` (no limit) |
| Debezium | `quay.io/debezium/connect:2.7.4.Final`, `plugin.name=pgoutput`, `publication.autocreate.mode=disabled` |
| Kafka Connect | `offset.flush.interval.ms=10000` |

**Load.** Every transaction inserts 20,000 rows of about 1 KB into `public.noise`, and then
`TRUNCATE`s it. There is no pause between transactions. The load runs for 180 s and writes
about 7.7-8.1 GB of WAL, which is about 44 MB/s on average (faster at the start).

**Timeline of each run** (the same for all three):

| Time | Phase | What happens |
|---|---|---|
| 0:00 - 0:30 | idle | no load |
| 0:30 - 3:30 | load | heavy load on `noise` |
| 3:30 - 4:30 | wait | no load, nothing changes |
| 4:30 - 8:30 | after | **No heartbeat** and **Heartbeat messages only**: at 4:30 we add the heartbeat table to the running connector (`./sim.sh fix`). **Heartbeat table**: it already has one, so we just watch. |

Adding the heartbeat table (`./sim.sh fix`) does what you would do in production:

```sql
ALTER PUBLICATION pub_no_hb ADD TABLE public.debezium_heartbeat;
```

Then it updates the connector config with `heartbeat.interval.ms=10000` and
`heartbeat.action.query=UPDATE public.debezium_heartbeat SET ts = now() WHERE id = 1`.
The connector restarts and keeps its offset.

**Fair start.** Only one connector runs at a time, so `pg_wal` shows the effect of that one
setup. Before each run, all connectors and slots are removed, the publication is reset, and
`pg_wal` is back to about 256 MB. Each run then creates a new connector and slot.

**What we measured** (every 5 s):

- `pg_wal` size on disk: `SELECT sum(size) FROM pg_ls_waldir()`
- WAL written since the start: `pg_current_wal_lsn()` minus the start LSN
- WAL kept by the slot: `pg_current_wal_lsn() - restart_lsn` from `pg_replication_slots`

Sizes use 1 GB = 1024³ bytes.

## Results

### Summary per setup

| Setup | WAL written | pg_wal peak | pg_wal at load end | pg_wal before adding heartbeat table | pg_wal at end | Back under 0.5 GB |
|---|---|---|---|---|---|---|
| No heartbeat | 7.7 GB | 7.8 GB | 7.6 GB | 7.8 GB | 224 MB | 95 s after heartbeat table added |
| Heartbeat messages only | 7.7 GB | 7.7 GB | 7.6 GB | 7.7 GB | 256 MB | 61 s after heartbeat table added |
| Heartbeat table | 8.1 GB | 2.9 GB | 1.8 GB | - (has it from the start) | 256 MB | 33 s after load end |

### Time series (pg_wal size on disk, every 30 s)

| Time | Phase | No heartbeat | Heartbeat messages only | Heartbeat table |
|---|---|---|---|---|
| 0:00 | idle | 256 MB | 224 MB | 256 MB |
| 0:30 | load | 256 MB | 224 MB | 256 MB |
| 1:00 | load | 2.4 GB | 2.4 GB | 2.4 GB |
| 1:30 | load | 3.2 GB | 3.2 GB | 2.5 GB |
| 2:00 | load | 4.1 GB | 3.9 GB | 1.5 GB |
| 2:30 | load | 5.2 GB | 5.0 GB | 2.0 GB |
| 3:00 | load | 6.3 GB | 6.4 GB | 1.5 GB |
| 3:30 | load | 7.6 GB | 7.6 GB | 1.8 GB |
| 4:00 | wait | 7.7 GB | 7.7 GB | 1.8 GB |
| 4:30 | after (heartbeat table added) | 7.8 GB | 7.7 GB | 256 MB |
| 5:00 | after | 7.8 GB | 7.7 GB | 256 MB |
| 5:30 | after | 7.8 GB | 256 MB | 256 MB |
| 6:00 | after | 7.8 GB | 256 MB | 256 MB |
| 6:30 | after | 224 MB | 256 MB | 256 MB |
| 7:00 | after | 224 MB | 256 MB | 256 MB |
| 7:30 | after | 224 MB | 256 MB | 256 MB |
| 8:00 | after | 224 MB | 256 MB | 256 MB |

## What the results mean

**No heartbeat.** The `noise` table is not in the publication. Since Postgres 15, pgoutput
does not send empty transactions, so Debezium gets nothing at all. It never confirms a new
position, and the slot stays at the start. Postgres must keep every WAL file: `pg_wal` is the
same as "WAL written" (the blue and gray lines are on top of each other). Waiting does not help:
from 3:30 to 4:30 nothing changes.

**Heartbeat messages only.** Debezium sends a heartbeat message to Kafka every 10 s, so the
connector looks healthy. But the message has the same old position, because Debezium still
receives nothing from the slot. The result is the same as with no heartbeat.

**Heartbeat table.** Every 10 s, Debezium runs the `UPDATE`. The heartbeat table is in the
publication, so this change goes through the slot. Debezium confirms its position (at the next
offset flush), and at the next checkpoint Postgres deletes the old WAL files. This is the
up-and-down line in the third panel. It is not flat: during heavy load it still keeps about
1-3 GB. That amount is roughly *write speed × time to confirm*. Here the time to confirm is
about 20-50 s:
- up to 10 s until the next heartbeat;
- up to 10 s until the next offset flush;
- the time Postgres needs to move `restart_lsn`;
- up to 30 s until the next checkpoint deletes the files.

At 44-80 MB/s this adds up to GBs. When the load stops, `pg_wal` goes back to 256 MB within
about 30 s.

**After adding the heartbeat table.** The WAL went down, but not right away:
- **Heartbeat messages only:** it took 61 s.
- **No heartbeat:** it took 95 s.

Why it takes this long:
1. The connector restarts with the new config.
2. Postgres must read (decode) the 7.7 GB of old WAL again, from the slot's `restart_lsn`,
   before it reaches the new heartbeat changes.
3. The next heartbeat and offset flush confirm the new position.
4. The next checkpoint deletes the files.

## Conclusion

1. **Without a heartbeat table, the WAL has no upper limit.** When the tables in the
   publication are quiet, Postgres keeps every WAL file written anywhere on the server. It keeps
   them until a captured table changes, or until the disk is full. In this test that was 7.7 GB
   in 3 minutes.
2. **`heartbeat.interval.ms` alone does not fix it** (Postgres 15 and newer). The connector
   looks healthy, because heartbeat messages arrive in Kafka, but the WAL still grows.
3. **A heartbeat table in the publication fixes it.**
   - During heavy load, the WAL stayed under 2.9 GB.
   - It went back to normal about 30 s after the load stopped.
   - It also works when added to a connector that is already running: the WAL went down
     within 1-2 minutes.
4. **The cost is very small.**
   - One table with one row, 56 kB on disk.
   - One UPDATE every 10 s is 8,640 per day. Each one writes about 139 bytes of WAL (about
     2 KB for the first one after a checkpoint), so together about **1-2 MB of WAL per day**.
     Compare that with the 7.7 GB in 3 minutes that can pile up without it.
5. **It is not the only protection.** If the connector is stopped or broken, the slot still
   keeps WAL, and the heartbeat cannot help. Keep an alert on the WAL kept by each slot, and
   think about `max_slot_wal_keep_size`. See the README for the trade-offs.

**Recommendation.** For every Debezium Postgres connector:
1. Create a heartbeat table in the connector's database.
2. Give the Debezium user `SELECT, UPDATE` on it.
3. Add it to the connector's publication.
4. Set `heartbeat.interval.ms` and `heartbeat.action.query` in the connector config.

**One heartbeat table per CDC database.** A connector, its replication slot and its
publication all belong to one database. A slot only moves when a change comes through its own
publication, and a publication can only list tables from its own database. So if you capture
tables from `sales` and `billing`, you need a heartbeat table in `sales` and another one in
`billing`. A database without CDC does not need one. Several connectors in the same database can
share one heartbeat table if each connector's publication includes it. (The benchmark used one
CDC database; this rule follows from how slots and publications work.)

**Access.** `SELECT, UPDATE` on the heartbeat table is enough for the heartbeat
(`UPDATE ... WHERE` also needs `SELECT`). Everything else is what CDC already needs:
- `LOGIN` and `REPLICATION` on the role;
- `CONNECT` on the database and `USAGE` on the schema (both are defaults for `public`);
- `SELECT` on the captured tables.

The whole lab ran with exactly this user: not a superuser, and no `CREATE` on the database,
because the publications are created by the DBA (`publication.autocreate.mode=disabled`).
`INSERT` is only needed if you use `INSERT ... ON CONFLICT` to recreate a deleted row.

## When do you need a heartbeat table?

You need it when **the server writes WAL, but the tables in your publication do not change for
some time.** WAL is shared by all tables and all databases on the server. A slot only moves
forward when a change from its publication arrives.

| Case | Example | Tested here |
|---|---|---|
| Captured tables change slowly, other tables in the same database are busy | We capture `customers` (a few changes per day), but `orders_log` gets 1,000 inserts per second | Yes, this benchmark |
| Another database on the same server is busy | We capture database `app_a`, but database `app_b` on the same server is busy | Yes, `./sim.sh noise busydb` (README) |
| Quiet times | Nights, weekends, holidays: no business changes, but jobs still write WAL (VACUUM, index rebuilds, batch jobs, partition maintenance, `pg_cron`) | Same cause as above |
| Managed Postgres | AWS RDS writes to its own system tables about every 5 minutes, even when the app is idle (Debezium docs) | No |
| New connector on a table with no traffic yet | A new table, or a table that only changes at month end | Same cause as above |

All of this is about **Postgres 15 and newer**, `plugin.name=pgoutput`, and a publication that
lists only some tables (`FOR TABLE ...`).

You may not need it when:
- The captured tables change all the time (every few seconds, day and night), **and** there is
  no big WAL writer outside the publication. Even then it is cheap insurance. After a quiet
  time, one change may not be enough to free the WAL: in the README test, it took 2-3 changes.
- The publication is `FOR ALL TABLES`. The busy tables are then in the publication, so the
  connector receives their changes and can move forward. The Debezium docs say
  `heartbeat.interval.ms` is enough here, but Debezium then receives every change from every
  table, which costs network and CPU. Not tested here.

**How much WAL can pile up?** A rough rule:

> WAL kept = WAL write speed of the whole server × longest quiet time of the captured tables

For example, 1 MB/s × a 60-hour weekend ≈ 211 GB. To measure the write speed, run
`SELECT pg_current_wal_lsn();` twice, 60 s apart, and subtract the two values with
`SELECT pg_wal_lsn_diff('<second>', '<first>');`.

**How to check a server that already runs Debezium.** Run this a few times during a quiet
period. If `wal_kept_by_slot` keeps growing while the connector is RUNNING, you need the
heartbeat table.

```sql
SELECT slot_name, active,
       pg_size_pretty(pg_current_wal_lsn() - restart_lsn)         AS wal_kept_by_slot,
       pg_size_pretty(pg_current_wal_lsn() - confirmed_flush_lsn) AS not_confirmed
FROM pg_replication_slots
WHERE slot_type = 'logical';
```

## Limits of this test

- One machine, Docker, local disk. The numbers depend on the hardware; the shape of the lines
  should be the same anywhere.
- The load speed is not constant (faster at the start, slower during checkpoints).
- Each setup ran once.
- This test uses Postgres 15. Before Postgres 15, pgoutput also sent empty transactions, so
  "heartbeat messages only" may work better there.
- We used 10 s for both `heartbeat.interval.ms` and `offset.flush.interval.ms`. Shorter values
  should make the heartbeat-table line lower during load.

## How to run it again

```bash
./sim.sh down && ./sim.sh up     # fresh lab (deletes all data)
bench/run.sh                     # about 27 minutes, writes bench/results/*.csv
python3 bench/plot.py            # makes bench/results/pg_wal.svg and tables.md
```

Settings are environment variables, for example:

```bash
LOAD_S=300 NOISE_ROWS=20000 RUNS="no-hb hb" bench/run.sh
```

| Variable | Default | Meaning |
|---|---|---|
| `RUNS` | `no-hb interval-only hb` | setups to run, in order |
| `IDLE_S` / `LOAD_S` / `WAIT_S` / `AFTER_S` | 30 / 180 / 60 / 240 | length of each phase (seconds) |
| `NOISE_ROWS` / `NOISE_SLEEP` | 20000 / 0 | rows per load transaction / pause between transactions |
| `SAMPLE_S` | 5 | seconds between samples |
