//===----------------------------------------------------------------------===//
//
// pg_orca/include/pg_orca/daft/process_gate.h
//
// Pure, PostgreSQL-independent predicate that decides whether a DaftExec
// CustomScan is allowed to run in the current process.
//
// This header deliberately depends on nothing except <cstdint> so that the
// gate logic can be unit-tested by a standalone driver (the P0-009-A
// selftest) without a live PostgreSQL backend.  The executor side
// (daft_exec.cpp::IsRegularBackend) feeds the real PG19 globals into it.
//
// Design contract: 03_PG19_ORCA_Daft开发集成方案.md §7.5.1
//   "BeginCustomScan 在 dlopen()/dpg_open_v1() 以前做防御检查：
//    IsParallelWorker() 为真，或 AmRegularBackendProcess() 为假时，
//    返回稳定的 FEATURE_NOT_SUPPORTED 错误，保证意外进入该路径也不会
//    启动 Rust runtime。"
//
// Inputs (mirroring frozen PG19 globals):
//   is_regular_backend  <- AmRegularBackendProcess()  (MyBackendType == B_BACKEND)
//   is_parallel_worker  <- IsParallelWorker()         (ParallelWorkerNumber >= 0)
//   is_bgworker         <- (MyBgworkerEntry != NULL)
//
//===----------------------------------------------------------------------===//
#ifndef PG_ORCA_DAFT_PROCESS_GATE_H
#define PG_ORCA_DAFT_PROCESS_GATE_H

#include <cstdint>

namespace pg_orca {
namespace daft {

/*
 * DaftExecProcessGate
 *
 * Returns true only for an ordinary client backend that is neither a
 * PostgreSQL parallel worker nor a background worker.  A parallel worker is
 * still B_BACKEND (so AmRegularBackendProcess() alone is insufficient) and
 * must be rejected explicitly; the bgworker flag is defense-in-depth.
 *
 * Keeping this a pure function of three booleans is what makes the P0/P1
 * process boundary (design §7.5.1) mechanically provable without spinning up
 * parallel-worker or background-worker processes.
 */
inline bool DaftExecProcessGate(bool is_regular_backend,
                                bool is_parallel_worker,
                                bool is_bgworker)
{
    return is_regular_backend && !is_parallel_worker && !is_bgworker;
}

}  // namespace daft
}  // namespace pg_orca

#endif  // PG_ORCA_DAFT_PROCESS_GATE_H

//===----------------------------------------------------------------------===//
// Standalone selftest driver (P0-009-A).
//
// Build & run (one shell line, shown wrapped here for readability):
//   g++ -std=c++17 -DDAFT_PROCESS_GATE_SELFTEST -Ipg_orca/include
//       pg_orca/include/pg_orca/daft/process_gate.h
//       -o /tmp/p0_009_a_process_gate_selftest
//   /tmp/p0_009_a_process_gate_selftest
//
// The block below is compiled ONLY when DAFT_PROCESS_GATE_SELFTEST is defined,
// so including this header from daft_exec.cpp during the normal pg_orca build
// never emits a main().
//===----------------------------------------------------------------------===//
#ifdef DAFT_PROCESS_GATE_SELFTEST

#include <cassert>
#include <cstdio>

int main(void)
{
    using pg_orca::daft::DaftExecProcessGate;

    unsigned pass = 0;
    unsigned total = 0;

    auto check = [&](bool got, bool exp, const char *name) {
        ++total;
        if (got == exp)
        {
            ++pass;
            std::printf("  [PASS] %s\n", name);
        }
        else
        {
            std::printf("  [FAIL] %s (got=%d expected=%d)\n", name,
                        (int) got, (int) exp);
        }
    };

    std::printf("=== P0-009-A process_gate selftest ===\n");
    std::printf("acceptance 5: Begin rejects parallel worker / non-regular "
                "backend before bridge open (design 7.5.1)\n");

    // Exhaustive truth table: allow iff (regular && !parallel && !bgworker).
    std::printf("  truth table (regular, parallel, bgworker) -> allow:\n");
    for (int r = 0; r < 2; ++r)
    {
        for (int p = 0; p < 2; ++p)
        {
            for (int b = 0; b < 2; ++b)
            {
                bool got = DaftExecProcessGate(r, p, b);
                bool exp = (r && !p && !b);
                char label[64];
                std::snprintf(label, sizeof(label),
                              "(r=%d,p=%d,b=%d) -> %d", r, p, b, (int) exp);
                check(got, exp, label);
            }
        }
    }

    // Concrete design §7.5.1 cases.
    check(DaftExecProcessGate(true, false, false), true,
          "ordinary client backend: ALLOWED");
    check(DaftExecProcessGate(true, true, false), false,
          "parallel worker (B_BACKEND but IsParallelWorker): rejected");
    check(DaftExecProcessGate(false, false, true), false,
          "background worker: rejected");
    check(DaftExecProcessGate(false, false, false), false,
          "autovacuum / walsender / startup etc.: rejected");

    // Idempotency: the predicate is pure.
    bool first = DaftExecProcessGate(true, false, false);
    bool second = DaftExecProcessGate(true, false, false);
    check(first && first == second, true, "pure predicate is stable");

    std::printf("process_gate selftest %u/%u PASS\n", pass, total);
    return (pass == total) ? 0 : 1;
}

#endif  // DAFT_PROCESS_GATE_SELFTEST
