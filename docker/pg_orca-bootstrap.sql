-- Bootstrap pg_orca on first start of the image.
-- Runs against the default POSTGRES_DB (set via env, defaults to "postgres").
-- Idempotent: safe to re-run, IF NOT EXISTS guards both CREATE EXTENSION calls.

\connect template1
CREATE EXTENSION IF NOT EXISTS pg_orca;

\connect postgres
CREATE EXTENSION IF NOT EXISTS pg_orca;

-- Cluster-wide arm: every new connection (in any DB) auto-loads the .so.
-- A connection then needs `CREATE EXTENSION pg_orca` once to register
-- the GUCs / planner_hook in that database's catalog.  Because we already
-- did that on template1 above, any future `CREATE DATABASE` clones it.
ALTER SYSTEM SET session_preload_libraries = 'pg_orca';
ALTER SYSTEM SET pg_orca.enable_orca = on;
SELECT pg_reload_conf();
