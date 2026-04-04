//! Integration tests for pg_orca_rs.
//!
//! Requires a running PostgreSQL 17 instance with pg_bridge loaded via
//! shared_preload_libraries. Set PG_TEST_PORT env var (default: 28817).
//!
//! Run:  PG_TEST_PORT=28817 cargo test -p pg_bridge --test integration

use postgres::{Client, NoTls};

fn connect() -> Client {
    let port = std::env::var("PG_TEST_PORT").unwrap_or_else(|_| "28817".into());
    let connstr = format!("host=127.0.0.1 port={} user=administrator dbname=postgres", port);
    Client::connect(&connstr, NoTls).expect("failed to connect to PostgreSQL")
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
                .map(|i| {
                    // Try text representation for any type
                    row.try_get::<_, String>(i).ok()
                })
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

    let rows = query_strings(&mut c, "SELECT * FROM _test_t;");
    assert_eq!(rows.len(), 2);
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

    // Verify join produces correct results
    // Note: orca currently returns all columns (no projection pruning), so use SELECT *
    let rows: Vec<_> = c
        .query(
            "SELECT * FROM _test_t JOIN _test_cust ON _test_t.a = _test_cust.id ORDER BY _test_t.a;",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 2, "expected 2 join rows");
    // Use text-mode query to avoid binary protocol issues
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
