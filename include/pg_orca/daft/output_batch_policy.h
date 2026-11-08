//===----------------------------------------------------------------------===//
//
// pg_orca/include/pg_orca/daft/output_batch_policy.h
//
// Pure, PostgreSQL-independent lifecycle of one DaftExec output batch.
//
// The executor consumes Daft results one Arrow batch at a time and emits one
// PG row per ExecCustomScan call (design §3.4, §7.3).  Three properties must
// hold and are awkward to exercise against a live bridge in P0:
//   (1) DPG_END is permanent: once END, no more rows are ever produced
//       (acceptance P0-009-C #1);
//   (2) a batch is released exactly once, only after its last row has been
//       emitted, so no returned Datum can outlive its Arrow buffer
//       (acceptance P0-009-C #3, stop-condition "Datum 寿命不足");
//   (3) DPG_AGAIN never causes a busy spin: it advances nothing and leaves the
//       caller to drive input instead (stop-condition "output AGAIN busy spin").
//
// This header factors the per-batch transition into a pure function so the
// P0-009-C selftest can prove those invariants without a PostgreSQL backend.
//
//===----------------------------------------------------------------------===//
#ifndef PG_ORCA_DAFT_OUTPUT_BATCH_POLICY_H
#define PG_ORCA_DAFT_OUTPUT_BATCH_POLICY_H

#include <cstdint>

namespace pg_orca {
namespace daft {

enum class OutputPhase : uint8_t
{
    OP_NEED_BATCH = 0,    // no batch buffered; need to receive one
    OP_HAS_ROW = 1,       // a batch is buffered with at least one unemitted row
    OP_EOF_PERMANENT = 2  // bridge signalled DPG_END; terminal
};

enum class OutputEvent : uint8_t
{
    OE_RECV_OK_ROWS = 0,  // received a batch with >=1 row
    OE_RECV_OK_EMPTY = 1, // received an empty (0-row) batch -> discard + refetch
    OE_RECV_AGAIN = 2,    // bridge would block (DPG_AGAIN)
    OE_RECV_END = 3,      // bridge signalled permanent end (DPG_END)
    OE_ROW_EMITTED_MORE = 4, // emitted a row; more rows remain in this batch
    OE_ROW_EMITTED_LAST = 5  // emitted the last row of this batch
};

struct OutputStep
{
    OutputPhase next;
    bool release_batch_now;  // release the current (exhausted/empty) batch
    bool emit_row_now;       // a row is ready to decode into the slot
    bool terminal;           // DPG_END reached (no further rows ever)
};

/*
 * AdvanceOutput -- pure transition of the output batch consumer.
 *
 * Invariants (proven by the selftest):
 *   - From OP_EOF_PERMANENT every event stays EOF with no emit/release: DPG_END
 *     is permanent (acceptance #1).
 *   - release_batch_now is true ONLY on OE_RECV_OK_EMPTY (discard empty) and on
 *     OE_ROW_EMITTED_LAST (batch exhausted after its last row) -> each batch is
 *     released exactly once, and never before its last row is emitted
 *     (acceptance #3, Datum lifetime).
 *   - OE_RECV_AGAIN from NEED advances nothing (no emit, no release, stays
 *     NEED) so the caller drives input instead of spinning on receive
 *     (stop-condition: no busy spin).
 *   - Rows are emitted in order: NEED --(OK_ROWS)--> HAS(row0) --(EMITTED_MORE)
 *     --> HAS(row1) ... --(EMITTED_LAST)--> release + NEED.
 */
inline OutputStep AdvanceOutput(OutputPhase phase, OutputEvent ev)
{
    OutputStep s{phase, false, false, false};

    switch (phase)
    {
        case OutputPhase::OP_NEED_BATCH:
            if (ev == OutputEvent::OE_RECV_OK_ROWS)
            {
                s.next = OutputPhase::OP_HAS_ROW;
                s.emit_row_now = true;            // first row of the new batch
            }
            else if (ev == OutputEvent::OE_RECV_OK_EMPTY)
            {
                s.next = OutputPhase::OP_NEED_BATCH;
                s.release_batch_now = true;       // discard empty, refetch
            }
            else if (ev == OutputEvent::OE_RECV_END)
            {
                s.next = OutputPhase::OP_EOF_PERMANENT;
                s.terminal = true;
            }
            // OE_RECV_AGAIN: stay NEED, no emit, no release (no spin).
            break;

        case OutputPhase::OP_HAS_ROW:
            if (ev == OutputEvent::OE_ROW_EMITTED_MORE)
            {
                s.next = OutputPhase::OP_HAS_ROW;
                s.emit_row_now = true;            // next row, same batch
            }
            else if (ev == OutputEvent::OE_ROW_EMITTED_LAST)
            {
                s.next = OutputPhase::OP_NEED_BATCH;
                s.release_batch_now = true;       // exhausted -> release once
            }
            break;

        case OutputPhase::OP_EOF_PERMANENT:
            // Terminal: nothing ever produces another row.
            break;
    }
    return s;
}

}  // namespace daft
}  // namespace pg_orca

#endif  // PG_ORCA_DAFT_OUTPUT_BATCH_POLICY_H

//===----------------------------------------------------------------------===//
// Standalone selftest driver (P0-009-C).  Build & run (one shell line):
//   g++ -std=c++17 -x c++ -DDAFT_OUTPUT_BATCH_POLICY_SELFTEST -Ipg_orca/include
//       pg_orca/include/pg_orca/daft/output_batch_policy.h -o <bin>  &&  <bin>
//===----------------------------------------------------------------------===//
#ifdef DAFT_OUTPUT_BATCH_POLICY_SELFTEST

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

    std::printf("=== P0-009-C output_batch_policy selftest ===\n");

    // --- acceptance 1: DPG_END is permanent ---
    std::printf("acceptance 1: DPG_END permanent\n");
    {
        OutputStep s = AdvanceOutput(OutputPhase::OP_NEED_BATCH, OutputEvent::OE_RECV_END);
        check(s.next == OutputPhase::OP_EOF_PERMANENT && s.terminal &&
              !s.emit_row_now && !s.release_batch_now,
              "NEED + END -> permanent EOF");
        // From EOF, every event stays EOF and emits/releases nothing.
        for (int e = 0; e <= 5; ++e)
        {
            OutputStep t = AdvanceOutput(OutputPhase::OP_EOF_PERMANENT, (OutputEvent) e);
            check(t.next == OutputPhase::OP_EOF_PERMANENT && !t.emit_row_now &&
                  !t.release_batch_now, "EOF + any event -> EOF, no emit/release");
        }
    }

    // --- acceptance 3 + Datum lifetime: release exactly once, after last row ---
    std::printf("acceptance 3: batch released exactly once after last row\n");
    {
        // Simulate a 3-row batch: NEED ->(OK_ROWS) HAS(row0) ->(MORE) HAS(row1)
        // ->(MORE) HAS(row2) ->(LAST) release + NEED.
        unsigned emit = 0, release = 0;
        OutputPhase ph = OutputPhase::OP_NEED_BATCH;
        OutputStep s;

        s = AdvanceOutput(ph, OutputEvent::OE_RECV_OK_ROWS);  // row 0
        ph = s.next; emit += s.emit_row_now; release += s.release_batch_now;

        s = AdvanceOutput(ph, OutputEvent::OE_ROW_EMITTED_MORE);  // row 1
        ph = s.next; emit += s.emit_row_now; release += s.release_batch_now;

        s = AdvanceOutput(ph, OutputEvent::OE_ROW_EMITTED_MORE);  // row 2
        ph = s.next; emit += s.emit_row_now; release += s.release_batch_now;

        check(ph == OutputPhase::OP_HAS_ROW && release == 0,
              "no release while rows remain (Datum lifetime)");

        s = AdvanceOutput(ph, OutputEvent::OE_ROW_EMITTED_LAST);  // done
        ph = s.next; emit += s.emit_row_now; release += s.release_batch_now;

        check(emit == 3, "emitted exactly 3 rows in order");
        check(release == 1, "batch released exactly once (on last-row transition)");
        check(ph == OutputPhase::OP_NEED_BATCH, "back to NEED after release");
    }

    // Empty batch: discard once, refetch, no emit.
    {
        OutputStep s = AdvanceOutput(OutputPhase::OP_NEED_BATCH, OutputEvent::OE_RECV_OK_EMPTY);
        check(s.release_batch_now && !s.emit_row_now &&
              s.next == OutputPhase::OP_NEED_BATCH,
              "empty batch: release once, no emit, refetch");
    }

    // --- stop-condition: output AGAIN never busy-spins ---
    std::printf("stop-condition: output AGAIN does not busy-spin\n");
    {
        OutputStep s = AdvanceOutput(OutputPhase::OP_NEED_BATCH, OutputEvent::OE_RECV_AGAIN);
        check(s.next == OutputPhase::OP_NEED_BATCH && !s.emit_row_now &&
              !s.release_batch_now,
              "AGAIN: stay NEED, no emit, no release (caller drives input)");
        // Repeated AGAIN never advances (loop must switch to input driving).
        OutputPhase ph = OutputPhase::OP_NEED_BATCH;
        bool advanced = false;
        for (int i = 0; i < 5; ++i)
        {
            OutputStep t = AdvanceOutput(ph, OutputEvent::OE_RECV_AGAIN);
            if (t.next != OutputPhase::OP_NEED_BATCH || t.emit_row_now || t.release_batch_now)
                advanced = true;
            ph = t.next;
        }
        check(!advanced, "repeated AGAIN never emits/releases/transitions");
    }

    std::printf("output_batch_policy selftest %u/%u PASS\n", pass, total);
    return (pass == total) ? 0 : 1;
}

#endif  // DAFT_OUTPUT_BATCH_POLICY_SELFTEST
