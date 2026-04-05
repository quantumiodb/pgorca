//! Self-contained integration tests for pg_orca_rs.
//!
//! Automatically creates a temporary PostgreSQL instance with pg_bridge loaded,
//! runs all tests, then tears everything down.
//!
//! Prerequisites: `cargo pgrx install` must have been run first to install
//! pg_bridge.so into the PG installation.
//!
//! Run:  cargo test -p pg_bridge --test integration -- --test-threads=1

use postgres::{Client, NoTls};
use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

// ── Ephemeral PG instance ───────────────────────────────

struct PgInstance {
    pgdata: PathBuf,
    port: u16,
    pg_bin: PathBuf,
}

impl PgInstance {
    fn start() -> Self {
        let pg_config = std::env::var("PGRX_PG_CONFIG_PATH")
            .unwrap_or_else(|_| "pg_config".into());

        // Get bin dir and lib dir from pg_config
        let bin_dir = cmd_output(&pg_config, &["--bindir"]);
        let lib_dir = cmd_output(&pg_config, &["--pkglibdir"]);
        let pg_bin = PathBuf::from(&bin_dir);

        // Verify pg_bridge shared library is installed (.so on Linux, .dylib on macOS)
        let so_path = if cfg!(target_os = "macos") {
            PathBuf::from(&lib_dir).join("pg_bridge.dylib")
        } else {
            PathBuf::from(&lib_dir).join("pg_bridge.so")
        };
        assert!(
            so_path.exists(),
            "pg_bridge shared library not found at {:?}. Run `cargo pgrx install` first.",
            so_path,
        );

        // Pick a random port in 10000-60000 range
        let port = 10000 + (std::process::id() % 50000) as u16;

        // Create temp data directory
        let pgdata = std::env::temp_dir().join(format!("pg_orca_test_{}", port));
        if pgdata.exists() {
            std::fs::remove_dir_all(&pgdata).ok();
        }

        // initdb
        let status = Command::new(pg_bin.join("initdb"))
            .args(["-D", pgdata.to_str().unwrap(), "--no-locale", "-E", "UTF8"])
            .env("LC_ALL", "C")
            .output()
            .expect("failed to run initdb");
        assert!(status.status.success(), "initdb failed: {}", String::from_utf8_lossy(&status.stderr));

        // Configure postgresql.conf
        let conf = pgdata.join("postgresql.conf");
        let extra = format!(
            "\n\
             listen_addresses = '127.0.0.1'\n\
             port = {}\n\
             shared_preload_libraries = 'pg_bridge'\n\
             log_min_messages = 'warning'\n\
             logging_collector = off\n",
            port,
        );
        std::fs::write(&conf, {
            let mut s = std::fs::read_to_string(&conf).unwrap();
            s.push_str(&extra);
            s
        }).unwrap();

        // pg_ctl start
        let status = Command::new(pg_bin.join("pg_ctl"))
            .args(["start", "-D", pgdata.to_str().unwrap(), "-w", "-l", pgdata.join("log").to_str().unwrap()])
            .output()
            .expect("failed to start postgres");
        assert!(
            status.status.success(),
            "pg_ctl start failed: {}\nlog: {}",
            String::from_utf8_lossy(&status.stderr),
            std::fs::read_to_string(pgdata.join("log")).unwrap_or_default(),
        );

        eprintln!("  pg_orca test instance started on port {}", port);

        PgInstance { pgdata, port, pg_bin }
    }

    fn connect(&self) -> Client {
        let connstr = format!(
            "host=127.0.0.1 port={} user={} dbname=postgres",
            self.port,
            whoami(),
        );
        Client::connect(&connstr, NoTls)
            .unwrap_or_else(|e| panic!("connect to port {} failed: {}", self.port, e))
    }
}

impl Drop for PgInstance {
    fn drop(&mut self) {
        // pg_ctl stop
        let _ = Command::new(self.pg_bin.join("pg_ctl"))
            .args(["stop", "-D", self.pgdata.to_str().unwrap(), "-m", "immediate"])
            .output();
        // Clean up data directory
        let _ = std::fs::remove_dir_all(&self.pgdata);
        eprintln!("  pg_orca test instance stopped and cleaned up");
    }
}

fn cmd_output(program: &str, args: &[&str]) -> String {
    let out = Command::new(program)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("failed to run {}: {}", program, e));
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

fn whoami() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("LOGNAME"))
        .unwrap_or_else(|_| cmd_output("whoami", &[]))
}

// Singleton: one PG instance for all tests (requires --test-threads=1)
static PG: OnceLock<PgInstance> = OnceLock::new();

fn pg() -> &'static PgInstance {
    PG.get_or_init(|| {
        // Clean up any leftover instances from previous runs
        if let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                if name.to_string_lossy().starts_with("pg_orca_test_") {
                    let p = entry.path();
                    if p.join("postmaster.pid").exists() {
                        let bin = PathBuf::from(cmd_output(
                            &std::env::var("PGRX_PG_CONFIG_PATH").unwrap_or("pg_config".into()),
                            &["--bindir"],
                        ));
                        let _ = Command::new(bin.join("pg_ctl"))
                            .args(["stop", "-D", p.to_str().unwrap(), "-m", "immediate"])
                            .output();
                    }
                    let _ = std::fs::remove_dir_all(&p);
                }
            }
        }

        let inst = PgInstance::start();

        // Register atexit cleanup (static OnceLock values are never dropped)
        extern "C" fn cleanup() {
            if let Some(inst) = PG.get() {
                let _ = Command::new(inst.pg_bin.join("pg_ctl"))
                    .args(["stop", "-D", inst.pgdata.to_str().unwrap(), "-m", "immediate"])
                    .output();
                let _ = std::fs::remove_dir_all(&inst.pgdata);
                eprintln!("  pg_orca test instance cleaned up via atexit");
            }
        }
        unsafe { libc::atexit(cleanup); }

        inst
    })
}

// ── Test helpers ────────────────────────────────────────

fn connect() -> Client {
    pg().connect()
}

fn setup(client: &mut Client) {
    client.batch_execute("SET orca.enabled = on;").unwrap();
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS _test_t (a int, b text);
             TRUNCATE _test_t;
             INSERT INTO _test_t VALUES (1, 'hello'), (2, 'world');
             ANALYZE _test_t;

             CREATE TABLE IF NOT EXISTS _test_orders (id int, amount numeric, customer_id int);
             TRUNCATE _test_orders;
             INSERT INTO _test_orders SELECT i, (i*1.5)::numeric, (i%100)
               FROM generate_series(1,1000) i;
             CREATE INDEX IF NOT EXISTS _test_orders_id_idx ON _test_orders(id);
             ANALYZE _test_orders;

             CREATE TABLE IF NOT EXISTS _test_cust (id int PRIMARY KEY, name text);
             TRUNCATE _test_cust;
             INSERT INTO _test_cust VALUES (1,'Alice'),(2,'Bob');
             ANALYZE _test_cust;",
        )
        .unwrap();
}

fn explain(client: &mut Client, sql: &str) -> Vec<String> {
    let q = format!("EXPLAIN (COSTS OFF) {}", sql);
    client
        .query(&q, &[])
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect()
}

fn query_strings(client: &mut Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .query(sql, &[])
        .unwrap()
        .iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get::<_, String>(i).ok())
                .collect()
        })
        .collect()
}

// ── M1: Simple scan ─────────────────────────────────────

#[test]
fn m1_simple_scan() {
    let mut c = connect();
    setup(&mut c);
    let plan = explain(&mut c, "SELECT * FROM _test_t;");
    assert!(plan.iter().any(|l| l.contains("Seq Scan")), "plan: {:?}", plan);
    assert!(plan.iter().any(|l| l.contains("Optimizer: pg_orca")), "plan: {:?}", plan);
}

#[test]
fn test_extended_types() {
    let mut c = connect();
    setup(&mut c);
    c.batch_execute("
        DROP TABLE IF EXISTS _test_types;
        CREATE TABLE _test_types (
            id int,
            val_numeric numeric,
            val_date date,
            val_ts timestamp,
            val_tstz timestamptz,
            val_text text
        );
        INSERT INTO _test_types VALUES (
            1, 
            123.456, 
            '2024-01-01', 
            '2024-01-01 12:00:00', 
            '2024-01-01 12:00:00+00',
            'hello world'
        );
    ").unwrap();

    // 1. Numeric test
    let plan = explain(&mut c, "SELECT * FROM _test_types WHERE val_numeric = 123.456;");
    assert!(plan.iter().any(|l| l.contains("Optimizer: pg_orca")), "Numeric failed orca: {:?}", plan);
    let rows = c.query("SELECT id FROM _test_types WHERE val_numeric = 123.456;", &[]).unwrap();
    assert_eq!(rows.len(), 1);

    // 2. Date test
    let plan = explain(&mut c, "SELECT * FROM _test_types WHERE val_date = '2024-01-01'::date;");
    assert!(plan.iter().any(|l| l.contains("Optimizer: pg_orca")), "Date failed orca: {:?}", plan);
    let rows = c.query("SELECT id FROM _test_types WHERE val_date = '2024-01-01'::date;", &[]).unwrap();
    assert_eq!(rows.len(), 1);

    // 3. Timestamp test
    let plan = explain(&mut c, "SELECT * FROM _test_types WHERE val_ts = '2024-01-01 12:00:00'::timestamp;");
    assert!(plan.iter().any(|l| l.contains("Optimizer: pg_orca")), "Timestamp failed orca: {:?}", plan);
    let rows = c.query("SELECT id FROM _test_types WHERE val_ts = '2024-01-01 12:00:00'::timestamp;", &[]).unwrap();
    assert_eq!(rows.len(), 1);

    // 4. TimestampTz test
    let plan = explain(&mut c, "SELECT * FROM _test_types WHERE val_tstz = '2024-01-01 12:00:00+00'::timestamptz;");
    assert!(plan.iter().any(|l| l.contains("Optimizer: pg_orca")), "TimestampTz failed orca: {:?}", plan);
    let rows = c.query("SELECT id FROM _test_types WHERE val_tstz = '2024-01-01 12:00:00+00'::timestamptz;", &[]).unwrap();
    assert_eq!(rows.len(), 1);

    // 5. Money test
    c.batch_execute("ALTER TABLE _test_types ADD COLUMN val_money money;").unwrap();
    c.execute("UPDATE _test_types SET val_money = '123.45'::money WHERE id = 1;", &[]).unwrap();
    let plan = explain(&mut c, "SELECT * FROM _test_types WHERE val_money = '123.45'::money;");
    assert!(plan.iter().any(|l| l.contains("Optimizer: pg_orca")), "Money failed orca: {:?}", plan);
    let rows = c.query("SELECT id FROM _test_types WHERE val_money = '123.45'::money;", &[]).unwrap();
    assert_eq!(rows.len(), 1);

    // 6. Large text (TOAST test)
    let large_text = "A".repeat(100000); // 100KB text should be enough to trigger TOAST
    c.execute("INSERT INTO _test_types (id, val_text) VALUES (2, $1);", &[&large_text]).unwrap();

    let plan = explain(&mut c, "SELECT val_text FROM _test_types WHERE id = 2;");
    assert!(plan.iter().any(|l| l.contains("Optimizer: pg_orca")), "Large text failed orca: {:?}", plan);
    let rows = c.query("SELECT length(val_text) FROM _test_types WHERE id = 2;", &[]).unwrap();
    let len: i32 = rows[0].get(0);
    assert_eq!(len, 100000);
}

// ── M3: WHERE clause filter ─────────────────────────────

#[test]
fn m3_where_filter() {
    let mut c = connect();
    setup(&mut c);
    let plan = explain(&mut c, "SELECT * FROM _test_t WHERE a > 1;");
    assert!(plan.iter().any(|l| l.contains("Filter")), "plan: {:?}", plan);

    let rows = query_strings(&mut c, "SELECT * FROM _test_t WHERE a > 1;");
    assert_eq!(rows.len(), 1);
}

#[test]
fn m3_where_indexed() {
    let mut c = connect();
    setup(&mut c);
    let rows: Vec<_> = c.query("SELECT count(*)::int FROM _test_orders WHERE id > 900;", &[]).unwrap();
    let cnt: i32 = rows[0].get(0);
    assert_eq!(cnt, 100);
}

// ── M4: JOIN ────────────────────────────────────────────

#[test]
fn m4_join() {
    let mut c = connect();
    setup(&mut c);
    let plan = explain(
        &mut c,
        "SELECT * FROM _test_t JOIN _test_cust ON _test_t.a = _test_cust.id;",
    );
    assert!(plan.iter().any(|l| l.contains("Optimizer: pg_orca")), "plan: {:?}", plan);

    // Use row_to_json to verify data (avoids binary protocol column issues)
    let rows: Vec<_> = c
        .query(
            "SELECT row_to_json(sub)::text FROM ( \
               SELECT * FROM _test_t JOIN _test_cust ON _test_t.a = _test_cust.id ORDER BY _test_t.a \
             ) sub;",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 2, "expected 2 join rows");
    let r0: String = rows[0].get(0);
    let r1: String = rows[1].get(0);
    assert!(r0.contains("Alice"), "row0: {}", r0);
    assert!(r1.contains("Bob"), "row1: {}", r1);
}

// ── M4b: LEFT/RIGHT JOIN ───────────────────────────────

#[test]
fn m4_left_join() {
    let mut c = connect();
    setup(&mut c);

    // _test_t has a=1,2; _test_cust has id=1,2. Add a=3 to _test_t for unmatched row.
    c.batch_execute("INSERT INTO _test_t VALUES (3, 'extra');").unwrap();

    let plan = explain(
        &mut c,
        "SELECT _test_t.a, _test_cust.name FROM _test_t LEFT JOIN _test_cust ON _test_t.a = _test_cust.id;",
    );
    eprintln!("left join plan: {:?}", plan);
    assert!(plan.iter().any(|l| l.contains("Optimizer: pg_orca")), "plan: {:?}", plan);

    // First: test SELECT * to see all columns
    let rows_all: Vec<_> = c.query(
        "SELECT * FROM _test_t LEFT JOIN _test_cust ON _test_t.a = _test_cust.id ORDER BY _test_t.a;",
        &[],
    ).unwrap();
    eprintln!("LEFT JOIN SELECT * columns: {}", rows_all[0].len());
    for (i, row) in rows_all.iter().enumerate() {
        let vals: Vec<String> = (0..row.len()).map(|j| {
            format!("{:?}", row.try_get::<_, String>(j).ok())
        }).collect();
        eprintln!("  row {}: {:?}", i, vals);
    }

    // Test specific columns
    let rows: Vec<_> = c.query(
        "SELECT _test_t.a, _test_cust.name FROM _test_t LEFT JOIN _test_cust ON _test_t.a = _test_cust.id ORDER BY _test_t.a;",
        &[],
    ).unwrap();
    eprintln!("LEFT JOIN SELECT a, name columns: {}", rows[0].len());
    for (i, row) in rows.iter().enumerate() {
        let vals: Vec<String> = (0..row.len()).map(|j| {
            format!("{:?}", row.try_get::<_, String>(j).ok())
        }).collect();
        eprintln!("  row {}: {:?}", i, vals);
    }
    assert_eq!(rows.len(), 3, "expected 3 rows from LEFT JOIN");
    // a=3 should have NULL name
    let name: Option<String> = rows[2].get(1);
    assert_eq!(name, None, "expected NULL for unmatched row, got {:?}", name);

    c.batch_execute("DELETE FROM _test_t WHERE a = 3;").unwrap();
}

#[test]
fn m4_right_join() {
    let mut c = connect();
    setup(&mut c);

    // _test_cust has id=1,2; _test_t has a=1,2.
    // Add id=3 to _test_cust for unmatched row.
    c.batch_execute("INSERT INTO _test_cust VALUES (3, 'Carol');").unwrap();

    let plan = explain(
        &mut c,
        "SELECT _test_t.a, _test_cust.name FROM _test_t RIGHT JOIN _test_cust ON _test_t.a = _test_cust.id;",
    );
    eprintln!("right join plan: {:?}", plan);
    assert!(plan.iter().any(|l| l.contains("Optimizer: pg_orca")), "plan: {:?}", plan);

    let rows: Vec<_> = c.query(
        "SELECT _test_t.a, _test_cust.name FROM _test_t RIGHT JOIN _test_cust ON _test_t.a = _test_cust.id ORDER BY _test_cust.id;",
        &[],
    ).unwrap();
    assert_eq!(rows.len(), 3, "expected 3 rows from RIGHT JOIN");
    // id=3 (Carol) should have NULL a
    let a_val: Option<i32> = rows[2].get(0);
    assert_eq!(a_val, None, "expected NULL for unmatched row, got {:?}", a_val);

    c.batch_execute("DELETE FROM _test_cust WHERE id = 3;").unwrap();
}

// ── M6: Aggregation ─────────────────────────────────────

#[test]
fn m6_count_star() {
    let mut c = connect();
    setup(&mut c);
    let rows: Vec<_> = c.query("SELECT count(*)::int FROM _test_orders WHERE id > 900;", &[]).unwrap();
    let cnt: i32 = rows[0].get(0);
    assert_eq!(cnt, 100);
}

#[test]
fn m6_group_by() {
    let mut c = connect();
    setup(&mut c);
    let plan = explain(&mut c, "SELECT a, count(*) FROM _test_t GROUP BY a;");
    assert!(
        plan.iter().any(|l| l.contains("HashAggregate") || l.contains("GroupAggregate")),
        "plan: {:?}", plan,
    );

    let rows: Vec<_> = c
        .query("SELECT a, count(*) FROM _test_t GROUP BY a ORDER BY a;", &[])
        .unwrap();
    assert_eq!(rows.len(), 2, "expected 2 groups");
}

// ── M7: Sort / Limit / Distinct ─────────────────────────

#[test]
fn m7_order_by_limit() {
    let mut c = connect();
    setup(&mut c);
    let plan = explain(&mut c, "SELECT * FROM _test_t ORDER BY a LIMIT 1;");
    assert!(plan.iter().any(|l| l.contains("Limit")), "plan: {:?}", plan);

    let rows = query_strings(&mut c, "SELECT * FROM _test_t ORDER BY a LIMIT 1;");
    assert_eq!(rows.len(), 1);
}

#[test]
fn m7_distinct() {
    let mut c = connect();
    setup(&mut c);
    let plan = explain(&mut c, "SELECT DISTINCT a FROM _test_t;");
    assert!(plan.iter().any(|l| l.contains("Unique")), "plan: {:?}", plan);

    let rows: Vec<_> = c.query("SELECT DISTINCT a FROM _test_t ORDER BY a;", &[]).unwrap();
    assert_eq!(rows.len(), 2);
}

// ── Partitioned tables ──────────────────────────────────

#[test]
fn partitioned_table() {
    let mut c = connect();
    setup(&mut c);

    // Create a range-partitioned table
    c.batch_execute(
        "DROP TABLE IF EXISTS _test_part CASCADE;
         CREATE TABLE _test_part (id int, val text, created_at date)
             PARTITION BY RANGE (created_at);
         CREATE TABLE _test_part_2024 PARTITION OF _test_part
             FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
         CREATE TABLE _test_part_2025 PARTITION OF _test_part
             FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
         INSERT INTO _test_part VALUES
             (1, 'a', '2024-06-15'),
             (2, 'b', '2024-12-01'),
             (3, 'c', '2025-03-20'),
             (4, 'd', '2025-07-04');
         ANALYZE _test_part;",
    )
    .unwrap();

    // Query should succeed via pg_orca optimizer and return correct results
    let rows = query_strings(&mut c, "SELECT * FROM _test_part ORDER BY id;");
    assert_eq!(rows.len(), 4, "expected 4 rows from partitioned table");

    // Should use pg_orca with Append plan
    let plan = explain(&mut c, "SELECT * FROM _test_part;");
    assert!(
        plan.iter().any(|l| l.contains("Optimizer: pg_orca")),
        "expected pg_orca for partitioned table, got: {:?}",
        plan,
    );
    assert!(
        plan.iter().any(|l| l.contains("Append")),
        "expected Append in plan, got: {:?}",
        plan,
    );

    // Simple scan on partitioned table
    let rows = query_strings(&mut c, "SELECT * FROM _test_part;");
    assert_eq!(rows.len(), 4, "expected 4 rows");

    // Query on a specific partition directly should also use pg_orca
    let plan = explain(&mut c, "SELECT * FROM _test_part_2024;");
    assert!(
        plan.iter().any(|l| l.contains("Optimizer: pg_orca")),
        "expected pg_orca for direct partition scan, got: {:?}",
        plan,
    );

    c.batch_execute("DROP TABLE IF EXISTS _test_part CASCADE;").unwrap();
}

// ── UNION / UNION ALL ──────────────────────────────────

#[test]
fn union_all() {
    let mut c = connect();
    setup(&mut c);

    // UNION ALL should use pg_orca with Append
    let plan = explain(&mut c, "SELECT a, b FROM _test_t WHERE a = 1 UNION ALL SELECT a, b FROM _test_t WHERE a = 2;");
    assert!(
        plan.iter().any(|l| l.contains("Optimizer: pg_orca")),
        "expected pg_orca for UNION ALL, got: {:?}", plan,
    );
    assert!(
        plan.iter().any(|l| l.contains("Append")),
        "expected Append in UNION ALL plan, got: {:?}", plan,
    );

    // Verify correct results
    let rows = query_strings(&mut c, "SELECT a, b FROM _test_t WHERE a = 1 UNION ALL SELECT a, b FROM _test_t WHERE a = 2;");
    assert_eq!(rows.len(), 2, "expected 2 rows from UNION ALL");
}

#[test]
fn union_distinct() {
    let mut c = connect();
    setup(&mut c);

    // UNION (distinct) should use pg_orca with Append + Unique
    let plan = explain(&mut c, "SELECT a FROM _test_t UNION SELECT a FROM _test_t;");
    assert!(
        plan.iter().any(|l| l.contains("Optimizer: pg_orca")),
        "expected pg_orca for UNION, got: {:?}", plan,
    );

    // Verify correct results: UNION deduplicates, so 2 distinct values
    let rows: Vec<_> = c.query("SELECT a FROM _test_t UNION SELECT a FROM _test_t ORDER BY a;", &[]).unwrap();
    assert_eq!(rows.len(), 2, "expected 2 distinct rows from UNION");
}

#[test]
fn union_all_three_way() {
    let mut c = connect();
    setup(&mut c);

    // Three-way UNION ALL
    let sql = "SELECT a, b FROM _test_t UNION ALL SELECT a, b FROM _test_t UNION ALL SELECT a, b FROM _test_t;";
    let plan = explain(&mut c, sql);
    assert!(
        plan.iter().any(|l| l.contains("Optimizer: pg_orca")),
        "expected pg_orca for 3-way UNION ALL, got: {:?}", plan,
    );

    let rows = query_strings(&mut c, sql);
    assert_eq!(rows.len(), 6, "expected 6 rows from 3-way UNION ALL");
}

#[test]
fn union_mixed() {
    let mut c = connect();
    setup(&mut c);

    // Mixed: UNION ALL + UNION (distinct)
    // PG parses as: (SELECT a FROM _test_t UNION ALL SELECT a FROM _test_t) UNION SELECT a FROM _test_t
    let sql = "SELECT a FROM _test_t UNION ALL SELECT a FROM _test_t UNION SELECT a FROM _test_t;";
    let plan = explain(&mut c, sql);
    eprintln!("mixed union plan: {:?}", plan);

    let rows: Vec<_> = c.query(
        "SELECT a FROM _test_t UNION ALL SELECT a FROM _test_t UNION SELECT a FROM _test_t ORDER BY a;",
        &[],
    ).unwrap();
    eprintln!("mixed union rows: {}", rows.len());

    assert!(
        plan.iter().any(|l| l.contains("Optimizer: pg_orca")),
        "expected pg_orca for mixed UNION, got: {:?}", plan,
    );
    // UNION dedup at the top: should get 2 distinct values
    assert_eq!(rows.len(), 2, "expected 2 distinct rows from mixed UNION");
}

// ── Fallback: unsupported queries use PG planner ────────

#[test]
fn fallback_subquery() {
    let mut c = connect();
    setup(&mut c);
    let rows = query_strings(
        &mut c,
        "SELECT * FROM _test_t WHERE a IN (SELECT id FROM _test_cust);",
    );
    assert_eq!(rows.len(), 2);
}

#[test]
fn fallback_cte() {
    let mut c = connect();
    setup(&mut c);
    let rows = query_strings(&mut c, "WITH cte AS (SELECT * FROM _test_t) SELECT * FROM cte;");
    assert_eq!(rows.len(), 2);
}

// ── Cost model: MCV / histogram / damping integration tests ───────

/// Helper: EXPLAIN with costs to check cost estimates
fn explain_costs(client: &mut Client, sql: &str) -> Vec<String> {
    let q = format!("EXPLAIN (COSTS ON) {}", sql);
    client
        .query(&q, &[])
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect()
}

/// Enable pg_orca on a connection and show fallback notices.
fn enable_orca(client: &mut Client) {
    client.batch_execute(
        "SET orca.enabled = on;
         SET orca.log_failure = on;
         SET client_min_messages = 'notice';"
    ).unwrap();
}

/// Create a skewed table where column `val` has 80% value=1, 20% other values.
/// This produces clear MCV entries after ANALYZE.
fn setup_skewed_table(client: &mut Client) {
    client
        .batch_execute(
            "DROP TABLE IF EXISTS _test_skew CASCADE;
             CREATE TABLE _test_skew (val int, rng int);
             -- 80% of rows have val=1
             INSERT INTO _test_skew (val, rng)
               SELECT 1, i FROM generate_series(1, 8000) i;
             -- 20% spread across val=2..100
             INSERT INTO _test_skew (val, rng)
               SELECT 2 + (i % 99), i FROM generate_series(1, 2000) i;
             ANALYZE _test_skew;",
        )
        .unwrap();
}

#[test]
fn cost_model_mcv_equality() {
    let mut c = connect();
    enable_orca(&mut c);
    setup_skewed_table(&mut c);

    // val=1 has ~80% of rows → MCV should have high frequency.
    // val=999 is rare/absent → much lower selectivity.
    // If MCV is being used, the cost for val=1 should be HIGHER than val=999
    // (more rows to process).
    let plan_common = explain_costs(&mut c, "SELECT * FROM _test_skew WHERE val = 1;");
    let plan_rare = explain_costs(&mut c, "SELECT * FROM _test_skew WHERE val = 999;");

    eprintln!("MCV common (val=1): {:?}", plan_common);
    eprintln!("MCV rare (val=999): {:?}", plan_rare);

    assert!(
        plan_common.iter().any(|l| l.contains("Optimizer: pg_orca")),
        "plan_common: {:?}", plan_common,
    );
    assert!(
        plan_rare.iter().any(|l| l.contains("Optimizer: pg_orca")),
        "plan_rare: {:?}", plan_rare,
    );

    // Extract row estimates from EXPLAIN output (format: "rows=NNN")
    let rows_common = extract_rows_estimate(&plan_common);
    let rows_rare = extract_rows_estimate(&plan_rare);

    eprintln!("  rows estimate common: {:?}, rare: {:?}", rows_common, rows_rare);

    // val=1 should have significantly more estimated rows than val=999
    if let (Some(rc), Some(rr)) = (rows_common, rows_rare) {
        assert!(
            rc > rr * 2.0,
            "MCV not effective: common rows={} should be >> rare rows={}",
            rc, rr,
        );
    }

    // Verify correctness
    let cnt: i64 = c
        .query("SELECT count(*) FROM _test_skew WHERE val = 1;", &[])
        .unwrap()[0]
        .get(0);
    assert_eq!(cnt, 8000);

    c.batch_execute("DROP TABLE IF EXISTS _test_skew CASCADE;").unwrap();
}

#[test]
fn cost_model_histogram_range() {
    let mut c = connect();
    enable_orca(&mut c);

    // Create a table with uniformly distributed values 1..10000
    c.batch_execute(
        "DROP TABLE IF EXISTS _test_hist CASCADE;
         CREATE TABLE _test_hist (id int);
         INSERT INTO _test_hist SELECT i FROM generate_series(1, 10000) i;
         ANALYZE _test_hist;",
    ).unwrap();

    // id < 1000 should select ~10% of rows
    // id < 5000 should select ~50% of rows
    // If histogram is used, the cost for id < 5000 should be higher
    let plan_10pct = explain_costs(&mut c, "SELECT * FROM _test_hist WHERE id < 1000;");
    let plan_50pct = explain_costs(&mut c, "SELECT * FROM _test_hist WHERE id < 5000;");

    eprintln!("histogram 10%: {:?}", plan_10pct);
    eprintln!("histogram 50%: {:?}", plan_50pct);

    assert!(
        plan_10pct.iter().any(|l| l.contains("Optimizer: pg_orca")),
        "plan_10pct: {:?}", plan_10pct,
    );
    assert!(
        plan_50pct.iter().any(|l| l.contains("Optimizer: pg_orca")),
        "plan_50pct: {:?}", plan_50pct,
    );

    let rows_10 = extract_rows_estimate(&plan_10pct);
    let rows_50 = extract_rows_estimate(&plan_50pct);

    eprintln!("  rows estimate 10%: {:?}, 50%: {:?}", rows_10, rows_50);

    if let (Some(r10), Some(r50)) = (rows_10, rows_50) {
        assert!(
            r50 > r10 * 2.0,
            "histogram not effective: 50%={} should be >> 10%={}",
            r50, r10,
        );
    }

    // Verify correctness
    let cnt: i64 = c
        .query("SELECT count(*) FROM _test_hist WHERE id < 5000;", &[])
        .unwrap()[0]
        .get(0);
    assert_eq!(cnt, 4999);

    c.batch_execute("DROP TABLE IF EXISTS _test_hist CASCADE;").unwrap();
}

#[test]
fn cost_model_damping_guc() {
    let mut c = connect();
    enable_orca(&mut c);

    // Create table with two independent columns
    c.batch_execute(
        "DROP TABLE IF EXISTS _test_damp CASCADE;
         CREATE TABLE _test_damp (a int, b int);
         INSERT INTO _test_damp SELECT i % 100, i % 50 FROM generate_series(1, 10000) i;
         ANALYZE _test_damp;",
    ).unwrap();

    let query = "SELECT * FROM _test_damp WHERE a < 10 AND b < 10;";

    // With default damping (0.75) — selectivity is loosened
    c.batch_execute("SET orca.damping_factor_filter = 0.75;").unwrap();
    let plan_damped = explain_costs(&mut c, query);
    let rows_damped = extract_rows_estimate(&plan_damped);

    // With damping = 1.0 — no damping, naive product of selectivities
    c.batch_execute("SET orca.damping_factor_filter = 1.0;").unwrap();
    let plan_naive = explain_costs(&mut c, query);
    let rows_naive = extract_rows_estimate(&plan_naive);

    eprintln!("damped (0.75): {:?} -> rows {:?}", plan_damped, rows_damped);
    eprintln!("naive  (1.0):  {:?} -> rows {:?}", plan_naive, rows_naive);

    assert!(
        plan_damped.iter().any(|l| l.contains("Optimizer: pg_orca")),
        "plan_damped: {:?}", plan_damped,
    );
    assert!(
        plan_naive.iter().any(|l| l.contains("Optimizer: pg_orca")),
        "plan_naive: {:?}", plan_naive,
    );

    // Damped should estimate more rows than naive (since damping loosens the selectivity)
    if let (Some(rd), Some(rn)) = (rows_damped, rows_naive) {
        assert!(
            rd >= rn,
            "damping not effective: damped rows={} should be >= naive rows={}",
            rd, rn,
        );
    }

    // Reset GUC
    c.batch_execute("SET orca.damping_factor_filter = 0.75;").unwrap();
    c.batch_execute("DROP TABLE IF EXISTS _test_damp CASCADE;").unwrap();
}

/// Extract the first "rows=NNN" estimate from EXPLAIN output lines.
fn extract_rows_estimate(plan: &[String]) -> Option<f64> {
    for line in plan {
        // EXPLAIN format: "... rows=1234 ..."
        if let Some(pos) = line.find("rows=") {
            let after = &line[pos + 5..];
            let num_str: String = after.chars().take_while(|c| c.is_ascii_digit() || *c == '.').collect();
            if let Ok(v) = num_str.parse::<f64>() {
                return Some(v);
            }
        }
    }
    None
}

// ── pg_regress: run official PG tests against our instance ───────

/// Run a set of PG regression tests via `pg_regress --use-existing`.
///
/// `test_names` – names matching files in `<inputdir>/sql/*.sql`.
/// `inputdir`   – directory containing `sql/` and `expected/` (e.g. PG source tree).
/// Results land in `pg_bridge/test/results/pg_regress/`.
fn run_pg_regress(test_names: &[&str], inputdir: &str) {
    let inst = pg();

    // Locate pg_regress binary: prefer installed pgxs copy, fall back to PATH.
    let pg_config = std::env::var("PGRX_PG_CONFIG_PATH").unwrap_or("pg_config".into());
    let pkglibdir = cmd_output(&pg_config, &["--pkglibdir"]);
    // pkglibdir is e.g. /foo/lib/postgresql; pgxs regress is at
    // /foo/lib/postgresql/pgxs/src/test/regress/pg_regress
    let pg_regress_path = PathBuf::from(&pkglibdir)
        .join("pgxs/src/test/regress/pg_regress");
    let pg_regress = if pg_regress_path.exists() {
        pg_regress_path
    } else {
        PathBuf::from("pg_regress")  // hope it is on PATH
    };

    let bindir = cmd_output(&pg_config, &["--bindir"]);
    let outputdir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test/results/pg_regress");
    std::fs::create_dir_all(&outputdir).unwrap();

    // Ensure the "regression" database exists (pg_regress --use-existing won't create it).
    Command::new(inst.pg_bin.join("createdb"))
        .args(["-h", "127.0.0.1", "-p", &inst.port.to_string(), "-U", &whoami(), "regression"])
        .output()
        .ok(); // ignore error if it already exists

    // Run test_setup first so that shared fixtures (INT4_TBL, etc.) are in place.
    // Pass required env vars that test_setup.sql reads via \getenv.
    let libdir = cmd_output(&pg_config, &["--pkglibdir"]);
    let dlsuffix = if cfg!(target_os = "macos") { ".dylib" } else { ".so" };
    Command::new(inst.pg_bin.join("psql"))
        .args([
            "-h", "127.0.0.1",
            "-p", &inst.port.to_string(),
            "-U", &whoami(),
            "-d", "regression",
            "-f", &format!("{}/sql/test_setup.sql", inputdir),
        ])
        .env("PGOPTIONS", "-c orca.enabled=on -c orca.log_failure=off")
        .env("PG_ABS_SRCDIR", inputdir)
        .env("PG_LIBDIR", &libdir)
        .env("PG_DLSUFFIX", dlsuffix)
        .output()
        .expect("failed to run test_setup.sql");

    let mut cmd = Command::new(&pg_regress);
    cmd.args([
        "--use-existing",
        "--host=127.0.0.1",
        &format!("--port={}", inst.port),
        &format!("--user={}", whoami()),
        "--dbname=regression",
        &format!("--bindir={}", bindir),
        &format!("--inputdir={}", inputdir),
        &format!("--expecteddir={}", inputdir),
        &format!("--outputdir={}", outputdir.display()),
    ]);
    cmd.args(test_names);
    // PGOPTIONS is forwarded by libpq to the server on every connection,
    // equivalent to issuing SET for each option before the session begins.
    cmd.env("PGOPTIONS", "-c orca.enabled=on -c orca.log_failure=off");

    let output = cmd.output().expect("failed to run pg_regress");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // pg_regress writes a regression.diffs file when tests fail
    let diffs_file = outputdir.join("regression.diffs");
    let diffs = if diffs_file.exists() {
        std::fs::read_to_string(&diffs_file).unwrap_or_default()
    } else {
        String::new()
    };

    assert!(
        output.status.success(),
        "pg_regress failed (exit {}).\nstdout:\n{}\nstderr:\n{}\ndiffs:\n{}",
        output.status,
        stdout,
        stderr,
        diffs,
    );
}

#[test]
fn pg_regress_int4() {
    let inputdir = std::env::var("PG_REGRESS_INPUTDIR")
        .unwrap_or_else(|_| "/Users/jianghua/code/postgresql/src/test/regress".into());
    run_pg_regress(&["int4"], &inputdir);
}

// ── SQL regression tests (test/sql/*.sql vs test/expected/*.out) ─

#[test]
fn sql_regress_base() {
    let inst = pg();
    let test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test");
    let sql_file = test_dir.join("sql/base.sql");
    assert!(sql_file.exists(), "missing {}", sql_file.display());

    // Run SQL via psql (set LD_LIBRARY_PATH to PG's own libdir to avoid libpq conflicts)
    let lib_dir = cmd_output(
        &std::env::var("PGRX_PG_CONFIG_PATH").unwrap_or("pg_config".into()),
        &["--libdir"],
    );
    let output = Command::new(inst.pg_bin.join("psql"))
        .args([
            "-h", "127.0.0.1",
            "-p", &inst.port.to_string(),
            "-U", &whoami(),
            "-d", "postgres",
            "-f", sql_file.to_str().unwrap(),
        ])
        .env("LD_LIBRARY_PATH", &lib_dir)
        .output()
        .expect("failed to run psql");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Save actual output for inspection
    let actual_file = test_dir.join("results/base.out");
    std::fs::create_dir_all(test_dir.join("results")).ok();
    std::fs::write(&actual_file, stdout.as_bytes()).ok();

    // Check no fatal errors (connection lost = crash)
    assert!(
        !stderr.contains("server closed the connection unexpectedly"),
        "SQL regression test crashed!\nstderr: {}\nSee: {}",
        stderr,
        actual_file.display(),
    );
    assert!(
        !stderr.contains("FATAL"),
        "SQL regression test hit FATAL error!\nstderr: {}",
        stderr,
    );
    // psql exit code 0 means all queries ran (even if some had ERRORs from fallback)
    assert!(
        output.status.success(),
        "psql exited with {}.\nstderr: {}",
        output.status,
        stderr,
    );

    // Verify output is non-empty
    assert!(!stdout.is_empty(), "psql produced no output");

    // Compare against expected output (test/expected/base.out)
    let expected_file = test_dir.join("expected/base.out");
    if expected_file.exists() {
        let expected = std::fs::read_to_string(&expected_file).unwrap();
        if stdout != expected {
            panic!(
                "SQL regression output differs.\n\
                 To update expected: cp {} {}\n\
                 To review:  diff -u {} {}",
                actual_file.display(), expected_file.display(),
                expected_file.display(), actual_file.display(),
            );
        }
    } else {
        // First run: save as expected baseline
        std::fs::create_dir_all(test_dir.join("expected")).ok();
        std::fs::write(&expected_file, stdout.as_bytes()).unwrap();
        eprintln!(
            "  Saved initial expected output to {}\n  \
             Commit this file to version control.",
            expected_file.display(),
        );
    }
}
