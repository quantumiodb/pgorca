/* pg_orca extension SQL.
 *
 * Loads the shared library so _PG_init runs and registers the
 * planner_hook + pg_orca.* GUCs in the current session.
 *
 * To make every new connection to a database auto-load pg_orca
 * (no per-session LOAD, no server restart), run AFTER CREATE EXTENSION
 * as a top-level command:
 *
 *     ALTER DATABASE mydb SET session_preload_libraries = 'pg_orca';
 *
 * Alternative scopes:
 *     ALTER SYSTEM SET session_preload_libraries = 'pg_orca';   -- cluster-wide
 *     SELECT pg_reload_conf();
 *
 *     ALTER ROLE bench SET session_preload_libraries = 'pg_orca'; -- single role
 *
 * Roll back:
 *     ALTER DATABASE mydb RESET session_preload_libraries;
 *     DROP EXTENSION pg_orca;
 */
LOAD 'MODULE_PATHNAME';
