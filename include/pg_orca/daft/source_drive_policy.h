//===----------------------------------------------------------------------===//
//
// pg_orca/include/pg_orca/daft/source_drive_policy.h
//
// Pure, PostgreSQL-independent decision core of the DaftExec input drive loop.
//
// The executor's non-blocking, multi-source drive loop (design §8.3.1) mixes
// three concerns that are awkward to exercise against a live bridge in P0:
//   (1) per-source batch ownership across DPG_AGAIN (acceptance P0-009-B #2);
//   (2) fair, non-starving multi-source scheduling (acceptance P0-009-B #3);
//   (3) the cooperative drive-step decision graph (§8.3.1 state machine).
//
// This header factors those three into pure functions of plain data so they
// can be unit-tested by a standalone driver (the P0-009-B selftest) without a
// PostgreSQL backend or a Rust bridge.  daft_exec.cpp feeds the real PG/bridge
// primitives into these decisions.
//
// Invariants encoded (design §3.4 note 1-2, §8.3.1):
//   * Only the PG main thread drives PG children (ExecProcNode); the bridge
//     and any Rust worker never call back into PG -> acceptance #1.
//   * A finalized input batch is owned by the caller until the bridge accepts
//     it (DPG_OK).  On DPG_AGAIN ownership is retained and the SAME batch is
//     retried; it is finalized at most once and never released on AGAIN.
//   * No source is starved: a rotating round-robin cursor visits every
//     non-finished source.
//
//===----------------------------------------------------------------------===//
#ifndef PG_ORCA_DAFT_SOURCE_DRIVE_POLICY_H
#define PG_ORCA_DAFT_SOURCE_DRIVE_POLICY_H

#include <cstdint>

namespace pg_orca {
namespace daft {

//===----------------------------------------------------------------------===//
// (1) Per-source batch send ownership (acceptance P0-009-B #2)
//===----------------------------------------------------------------------===//

enum class BatchSendPhase : uint8_t
{
    BSP_EMPTY = 0,        // no batch in flight
    BSP_BUILDING = 1,     // accumulating PG rows into a batch
    BSP_READY_PENDING = 2 // finalized batch owned by us, awaiting bridge accept
};

enum class BatchEvent : uint8_t
{
    BE_ROW = 0,           // a PG row was fetched from the child
    BE_BATCH_FULL = 1,    // accumulated batch reached the target size
    BE_CHILD_EOF = 2,     // child returned NULL (no more rows)
    BE_SEND_OK = 3,       // bridge accepted the batch (DPG_OK)
    BE_SEND_AGAIN = 4     // bridge would block (DPG_AGAIN); we keep ownership
};

struct BatchSendStep
{
    BatchSendPhase next_phase;
    bool finalize_now;    // transition BUILDING -> READY_PENDING: build exactly
                          // one finalized batch from the accumulator
    bool release_now;     // bridge accepted (DPG_OK): release the batch
    bool source_done;     // child EOF with nothing to send: finish the source
};

/*
 * AdvanceBatchSend -- pure transition of one source's batch ownership.
 *
 * Contract proven by the selftest:
 *   - finalize_now is true ONLY on BUILDING -> READY_PENDING, i.e. at most once
 *     per batch regardless of how many DPG_AGAIN retries follow;
 *   - on BE_SEND_AGAIN the phase stays READY_PENDING with release_now=false
 *     (ownership retained, same batch retried);
 *   - on BE_SEND_OK the batch is released and the source returns to EMPTY so a
 *     fresh batch can be built (no double-finalize, no leak).
 */
inline BatchSendStep AdvanceBatchSend(BatchSendPhase phase, BatchEvent ev)
{
    BatchSendStep s{phase, false, false, false};
    switch (phase)
    {
        case BatchSendPhase::BSP_EMPTY:
            if (ev == BatchEvent::BE_ROW)
                s.next_phase = BatchSendPhase::BSP_BUILDING;
            else if (ev == BatchEvent::BE_CHILD_EOF)
                s.source_done = true;
            break;

        case BatchSendPhase::BSP_BUILDING:
            if (ev == BatchEvent::BE_ROW)
                s.next_phase = BatchSendPhase::BSP_BUILDING;   // accumulate
            else if (ev == BatchEvent::BE_BATCH_FULL ||
                     ev == BatchEvent::BE_CHILD_EOF)
            {
                s.next_phase = BatchSendPhase::BSP_READY_PENDING;
                s.finalize_now = true;                          // finalize once
                if (ev == BatchEvent::BE_CHILD_EOF)
                    s.source_done = true; // EOF: finalize partial then finish
            }
            break;

        case BatchSendPhase::BSP_READY_PENDING:
            if (ev == BatchEvent::BE_SEND_OK)
            {
                s.next_phase = BatchSendPhase::BSP_EMPTY;
                s.release_now = true;                           // bridge owns it now
            }
            else if (ev == BatchEvent::BE_SEND_AGAIN)
            {
                s.next_phase = BatchSendPhase::BSP_READY_PENDING; // retain + retry
            }
            // BE_ROW / BE_CHILD_EOF while pending are ignored: we must first
            // get the pending batch accepted before fetching more rows.
            break;
    }
    return s;
}

//===----------------------------------------------------------------------===//
// (2) Fair multi-source scheduling (acceptance P0-009-B #3)
//===----------------------------------------------------------------------===//

/*
 * PickNextSourceToDrive -- rotating round-robin over non-finished sources.
 *
 * Returns the index in [0, n) of the first non-finished source at or after
 * `cursor` (wrapping), or -1 if every source is finished.  The caller advances
 * the cursor after each drive so that no source is starved: with n driveable
 * sources, source i is visited within n steps.  `finished` is an array of n
 * booleans (true = source fully driven / EOF).
 */
inline int PickNextSourceToDrive(const bool *finished, uint32_t n, uint32_t cursor)
{
    if (n == 0)
        return -1;
    for (uint32_t k = 0; k < n; ++k)
    {
        uint32_t idx = (cursor + k) % n;
        if (!finished[idx])
            return (int) idx;
    }
    return -1;  // all finished
}

//===----------------------------------------------------------------------===//
// (3) Cooperative drive-step decision (design §8.3.1)
//===----------------------------------------------------------------------===//

enum class DriveAction : uint8_t
{
    DA_RETURN_ROW = 0,   // an output row is already buffered -> emit it
    DA_DONE = 1,         // bridge signalled permanent END
    DA_DRIVE_INPUT = 2,  // at least one PG source can still make progress
    DA_WAIT = 3          // all directions temporarily blocked: bounded wait +
                         // CHECK_FOR_INTERRUPTS (real bridge: dpg_wait_v1)
};

/*
 * DecideDriveStep -- pure top-level decision of one drive iteration.
 *
 * Priority mirrors §8.3.1: serve already-buffered output first, then detect
 * permanent end, then drive input, and only when nothing else can progress do
 * a bounded wait (never an infinite block, never a busy spin).
 */
inline DriveAction DecideDriveStep(bool output_row_ready,
                                   bool output_eof,
                                   bool any_source_driveable,
                                   bool all_sources_finished)
{
    if (output_row_ready)
        return DriveAction::DA_RETURN_ROW;
    if (output_eof)
        return DriveAction::DA_DONE;
    if (any_source_driveable)
        return DriveAction::DA_DRIVE_INPUT;
    /*
     * No buffered row, no EOF, no driveable source.  With the synchronous P0
     * stub this state is unreachable once input is drained (receive returns
     * END); with a real async bridge it means both queues are temporarily
     * full/empty, so the only correct move is a bounded wait, not an error and
     * not an infinite push (stop-condition: push 会无限阻塞).
     */
    (void) all_sources_finished;
    return DriveAction::DA_WAIT;
}

}  // namespace daft
}  // namespace pg_orca

#endif  // PG_ORCA_DAFT_SOURCE_DRIVE_POLICY_H

//===----------------------------------------------------------------------===//
// Standalone selftest driver (P0-009-B).  Build & run (one shell line,
// wrapped for readability):
//   g++ -std=c++17 -x c++ -DDAFT_SOURCE_DRIVE_POLICY_SELFTEST -Ipg_orca/include
//       pg_orca/include/pg_orca/daft/source_drive_policy.h -o <bin>  &&  <bin>
//===----------------------------------------------------------------------===//
#ifdef DAFT_SOURCE_DRIVE_POLICY_SELFTEST

#include <cstdio>

int main(void)
{
    using namespace pg_orca::daft;
    unsigned pass = 0, total = 0;
    auto check = [&](bool ok, const char *name) {
        ++total;
        if (ok) { ++pass; std::printf("  [PASS] %s\n", name); }
        else    { std::printf("  [FAIL] %s\n", name); }
    };

    std::printf("=== P0-009-B source_drive_policy selftest ===\n");

    // --- acceptance #2: AGAIN retains batch ownership, finalize once ---
    std::printf("acceptance 2: AGAIN retains batch ownership\n");
    {
        BatchSendPhase ph = BatchSendPhase::BSP_EMPTY;
        unsigned finalize_count = 0, release_count = 0;

        auto step = [&](BatchEvent ev) {
            BatchSendStep s = AdvanceBatchSend(ph, ev);
            if (s.finalize_now) ++finalize_count;
            if (s.release_now) ++release_count;
            ph = s.next_phase;
            return s;
        };

        step(BatchEvent::BE_ROW);        // EMPTY -> BUILDING
        check(ph == BatchSendPhase::BSP_BUILDING, "EMPTY+ROW -> BUILDING");
        step(BatchEvent::BE_ROW);        // accumulate
        step(BatchEvent::BE_BATCH_FULL); // BUILDING -> READY_PENDING (finalize #1)
        check(ph == BatchSendPhase::BSP_READY_PENDING, "BUILDING+FULL -> READY_PENDING");
        check(finalize_count == 1, "finalized exactly once before any send");

        // Bridge busy several times: ownership retained, no re-finalize, no release.
        BatchSendStep a1 = step(BatchEvent::BE_SEND_AGAIN);
        check(a1.release_now == false && finalize_count == 1,
              "AGAIN: retain ownership, no release, no extra finalize");
        BatchSendStep a2 = step(BatchEvent::BE_SEND_AGAIN);
        check(ph == BatchSendPhase::BSP_READY_PENDING && finalize_count == 1 && release_count == 0,
              "repeated AGAIN: still READY_PENDING, still owned");

        // Row/EOF events while pending must be ignored (can't fetch more until
        // the pending batch is accepted).
        step(BatchEvent::BE_ROW);
        check(ph == BatchSendPhase::BSP_READY_PENDING, "ROW while pending ignored");
        check(finalize_count == 1, "no double-finalize while pending");

        // Bridge finally accepts: release + back to EMPTY.
        BatchSendStep ok = step(BatchEvent::BE_SEND_OK);
        check(ok.release_now == true && ph == BatchSendPhase::BSP_EMPTY,
              "SEND_OK: release batch, return to EMPTY");
        check(finalize_count == 1 && release_count == 1,
              "one finalize, one release over the whole lifecycle");
    }

    // EOF with rows already building: finalize partial then finish source.
    {
        BatchSendPhase ph = BatchSendPhase::BSP_BUILDING;
        BatchSendStep s = AdvanceBatchSend(ph, BatchEvent::BE_CHILD_EOF);
        check(s.finalize_now && s.source_done &&
              s.next_phase == BatchSendPhase::BSP_READY_PENDING,
              "BUILDING+EOF: finalize partial, mark source done");
    }
    // EOF with nothing built: finish source, no finalize.
    {
        BatchSendPhase ph = BatchSendPhase::BSP_EMPTY;
        BatchSendStep s = AdvanceBatchSend(ph, BatchEvent::BE_CHILD_EOF);
        check(s.source_done && !s.finalize_now && !s.release_now,
              "EMPTY+EOF: finish source, nothing finalized/released");
    }

    // --- acceptance #3: fair scheduling, no starvation ---
    std::printf("acceptance 3: no source starved (round-robin)\n");
    {
        bool fin[3] = {false, false, false};
        unsigned hits[3] = {0, 0, 0};
        uint32_t cursor = 0;
        // 9 picks across 3 driveable sources must touch each ~3x and never
        // leave one unvisited for more than n steps.
        for (int i = 0; i < 9; ++i)
        {
            int idx = PickNextSourceToDrive(fin, 3, cursor);
            check(idx >= 0, "driveable source selected");
            if (idx >= 0) { ++hits[idx]; cursor = (uint32_t) idx + 1; }
        }
        check(hits[0] == 3 && hits[1] == 3 && hits[2] == 3,
              "round-robin distributes evenly (no starvation)");

        // Finish source 1: only 0 and 2 should be picked thereafter.
        fin[1] = true;
        cursor = 0;
        bool visited1 = false;
        for (int i = 0; i < 6; ++i)
        {
            int idx = PickNextSourceToDrive(fin, 3, cursor);
            if (idx == 1) visited1 = true;
            if (idx >= 0) cursor = (uint32_t) idx + 1;
        }
        check(!visited1, "finished source never selected");

        // All finished -> -1.
        fin[0] = fin[2] = true;
        check(PickNextSourceToDrive(fin, 3, 0) == -1, "all finished -> -1");
        check(PickNextSourceToDrive(nullptr, 0, 0) == -1, "zero sources -> -1");
    }

    // --- §8.3.1 drive-step decision truth table ---
    std::printf("drive-step decision (design 8.3.1)\n");
    check(DecideDriveStep(true, false, false, false) == DriveAction::DA_RETURN_ROW,
          "buffered row -> RETURN_ROW");
    check(DecideDriveStep(false, true, false, false) == DriveAction::DA_DONE,
          "EOF -> DONE");
    check(DecideDriveStep(false, false, true, false) == DriveAction::DA_DRIVE_INPUT,
          "driveable source -> DRIVE_INPUT");
    check(DecideDriveStep(false, false, false, true) == DriveAction::DA_WAIT,
          "nothing driveable -> bounded WAIT (not error/spin)");
    check(DecideDriveStep(false, false, false, false) == DriveAction::DA_WAIT,
          "all blocked -> bounded WAIT");
    // Output is served even if input is also driveable.
    check(DecideDriveStep(true, false, true, false) == DriveAction::DA_RETURN_ROW,
          "output preferred over input");

    std::printf("source_drive_policy selftest %u/%u PASS\n", pass, total);
    return (pass == total) ? 0 : 1;
}

#endif  // DAFT_SOURCE_DRIVE_POLICY_SELFTEST
