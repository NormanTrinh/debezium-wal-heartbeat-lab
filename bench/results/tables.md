| Setup | WAL written | pg_wal peak | pg_wal at load end | pg_wal before adding heartbeat table | pg_wal at end | Back under 0.5 GB |
|---|---|---|---|---|---|---|
| No heartbeat | 7.7 GB | 7.8 GB | 7.6 GB | 7.8 GB | 224 MB | 95 s after heartbeat table added |
| Heartbeat messages only | 7.7 GB | 7.7 GB | 7.6 GB | 7.7 GB | 256 MB | 61 s after heartbeat table added |
| Heartbeat table | 8.1 GB | 2.9 GB | 1.8 GB | - (has it from the start) | 256 MB | 33 s after load end |

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
