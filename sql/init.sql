-- Runs once, on a fresh volume, as myuser in mydatabase (docker-entrypoint-initdb.d).

-- Least-privilege Debezium user: REPLICATION + LOGIN, not superuser.
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'dbz';

-- Captured, low-traffic table.
CREATE TABLE public.customers (
    id         serial PRIMARY KEY,
    name       text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO public.customers (name) VALUES ('alice'), ('bob'), ('carol');

-- Heartbeat table. The heartbeat is a plain UPDATE of row id=1, so the row must exist:
-- an UPDATE matching 0 rows writes no WAL and the slot would silently stop advancing.
-- The primary key gives it a replica identity, which Postgres requires for UPDATEs
-- on a table that belongs to a publication publishing updates.
CREATE TABLE public.debezium_heartbeat (
    id int PRIMARY KEY,
    ts timestamptz NOT NULL
);
INSERT INTO public.debezium_heartbeat (id, ts) VALUES (1, now());

-- Busy, uncaptured table (in no publication).
CREATE TABLE public.noise (
    id      bigserial,
    payload text
);

GRANT SELECT ON public.customers TO debezium;
GRANT SELECT, UPDATE ON public.debezium_heartbeat TO debezium;

-- Publication WITHOUT the heartbeat table.
CREATE PUBLICATION pub_no_hb FOR TABLE public.customers;
-- Publication WITH the heartbeat table (required for pgoutput to ship heartbeat changes).
CREATE PUBLICATION pub_hb FOR TABLE public.customers, public.debezium_heartbeat;

-- A high-traffic neighbour database on the same server (WAL is shared cluster-wide).
SELECT 'CREATE DATABASE busydb'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'busydb')\gexec

\c busydb
CREATE TABLE public.noise (
    id      bigserial,
    payload text
);
