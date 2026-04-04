use pgrx::prelude::*;
use pgrx::{GucRegistry, GucSetting, GucContext, GucFlags};
use std::cell::Cell;
use std::ffi::c_char;
use std::panic::AssertUnwindSafe;

pgrx::pg_module_magic!();

pub mod inbound;
pub mod catalog;
pub mod outbound;
pub mod utils;

// ── GUC settings ────────────────────────────────────────

static ORCA_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(false);
static ORCA_LOG_FAILURE: GucSetting<bool> = GucSetting::<bool>::new(true);
static ORCA_LOG_PLAN: GucSetting<bool> = GucSetting::<bool>::new(false);

// ── Previous hooks ──────────────────────────────────────

static mut PREV_PLANNER_HOOK: pg_sys::planner_hook_type = None;
static mut PREV_EXPLAIN_HOOK: pg_sys::ExplainOneQuery_hook_type = None;

// ── Thread-local flag: did orca handle the last plan? ───

thread_local! {
    static ORCA_PLANNED: Cell<bool> = const { Cell::new(false) };
}

// ── Unified error type ──────────────────────────────────

#[derive(Debug)]
enum OrcaError {
    Inbound(inbound::InboundError),
    Optimizer(optimizer_core::OptimizerError),
    Outbound(outbound::OutboundError),
}

impl std::fmt::Display for OrcaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inbound(e) => write!(f, "inbound: {}", e),
            Self::Optimizer(e) => write!(f, "optimizer: {}", e),
            Self::Outbound(e) => write!(f, "outbound: {}", e),
        }
    }
}

// ── Planner hook implementation ─────────────────────────

#[pg_guard]
unsafe extern "C-unwind" fn orca_planner(
    parse: *mut pg_sys::Query,
    query_string: *const c_char,
    cursor_options: i32,
    bound_params: pg_sys::ParamListInfo,
) -> *mut pg_sys::PlannedStmt {
    // Check GUC
    if !ORCA_ENABLED.get() {
        return call_prev_planner(parse, query_string, cursor_options, bound_params);
    }

    // Try our optimizer with panic catch
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        try_optimize(parse)
    }));

    match result {
        Ok(Ok(stmt)) => {
            ORCA_PLANNED.with(|f| f.set(true));
            if ORCA_LOG_PLAN.get() {
                pgrx::notice!("pg_orca_rs: plan generated successfully");
            }
            stmt
        }
        Ok(Err(e)) => {
            ORCA_PLANNED.with(|f| f.set(false));
            if ORCA_LOG_FAILURE.get() {
                pgrx::notice!("pg_orca_rs fallback: {}", e);
            }
            call_prev_planner(parse, query_string, cursor_options, bound_params)
        }
        Err(_panic) => {
            ORCA_PLANNED.with(|f| f.set(false));
            pgrx::warning!("pg_orca_rs: panic caught, falling back to standard planner");
            call_prev_planner(parse, query_string, cursor_options, bound_params)
        }
    }
}

unsafe fn try_optimize(parse: *mut pg_sys::Query) -> Result<*mut pg_sys::PlannedStmt, OrcaError> {
    let query = &*parse;

    // Phase 1: Query → LogicalExpr
    let convert_result = inbound::convert_query(query)
        .map_err(OrcaError::Inbound)?;

    // Phase 2: Cascades optimize
    let phys_plan = optimizer_core::optimize(convert_result.logical_expr, &convert_result.catalog)
        .map_err(OrcaError::Optimizer)?;

    // Phase 3: PhysicalPlan → PlannedStmt
    let stmt = outbound::generate_planned_stmt(&phys_plan, query, &convert_result.col_map)
        .map_err(OrcaError::Outbound)?;

    Ok(stmt)
}

unsafe fn call_prev_planner(
    parse: *mut pg_sys::Query,
    query_string: *const c_char,
    cursor_options: i32,
    bound_params: pg_sys::ParamListInfo,
) -> *mut pg_sys::PlannedStmt {
    match PREV_PLANNER_HOOK {
        Some(hook) => hook(parse, query_string, cursor_options, bound_params),
        None => pg_sys::standard_planner(parse, query_string, cursor_options, bound_params),
    }
}

// ── ExplainOneQuery hook ────────────────────────────────

#[pg_guard]
unsafe extern "C-unwind" fn orca_explain_one_query(
    query: *mut pg_sys::Query,
    cursor_options: std::ffi::c_int,
    into: *mut pg_sys::IntoClause,
    es: *mut pg_sys::ExplainState,
    query_string: *const c_char,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
) {
    // Call the previous hook (or standard_ExplainOneQuery)
    match PREV_EXPLAIN_HOOK {
        Some(hook) => hook(query, cursor_options, into, es, query_string, params, query_env),
        None => pg_sys::standard_ExplainOneQuery(
            query, cursor_options, into, es, query_string, params, query_env,
        ),
    }

    // Append "Optimizer" property to EXPLAIN output
    if ORCA_ENABLED.get() {
        let optimizer_name = if ORCA_PLANNED.with(|f| f.get()) {
            c"pg_orca"
        } else {
            c"Postgres"
        };
        pg_sys::ExplainPropertyText(c"Optimizer".as_ptr(), optimizer_name.as_ptr(), es);
    }
}

// ── Extension init ──────────────────────────────────────

#[allow(non_snake_case)]
#[pg_guard]
pub unsafe extern "C-unwind" fn _PG_init() {
    // Register GUCs
    GucRegistry::define_bool_guc(
        c"orca.enabled",
        c"Enable pg_orca_rs optimizer",
        c"When on, pg_orca_rs replaces the standard planner for supported queries",
        &ORCA_ENABLED,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"orca.log_failure",
        c"Log pg_orca_rs fallback reasons",
        c"",
        &ORCA_LOG_FAILURE,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"orca.log_plan",
        c"Log when pg_orca_rs generates a plan",
        c"",
        &ORCA_LOG_PLAN,
        GucContext::Userset,
        GucFlags::default(),
    );

    // Chain planner hook
    PREV_PLANNER_HOOK = pg_sys::planner_hook;
    pg_sys::planner_hook = Some(orca_planner);

    // Chain ExplainOneQuery hook
    PREV_EXPLAIN_HOOK = pg_sys::ExplainOneQuery_hook;
    pg_sys::ExplainOneQuery_hook = Some(orca_explain_one_query);

    pgrx::log!("pg_orca_rs: planner hook installed");
}

// Tests are in tests/integration.rs — run with:
//   PG_TEST_PORT=28817 cargo test -p pg_bridge --test integration
