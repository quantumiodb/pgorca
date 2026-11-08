/*-------------------------------------------------------------------------
 *
 * pg_daft_arrow.h
 *    Frozen PostgreSQL/Arrow type and schema contract.
 *
 * The functions in this file are internal to pg_orca.  They are not part of
 * the dynamically loaded Bridge ABI.  P0-007-B/C build the batch adapters on
 * this contract rather than maintaining a second type table.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_DAFT_ARROW_H
#define PG_DAFT_ARROW_H

#include "postgres.h"
#include "executor/tuptable.h"

#if defined(PG_ORCA_ENABLE_DAFT) && PG_ORCA_ENABLE_DAFT
#include "arrow/c/abi.h"
#else
struct ArrowSchema;
struct ArrowArray;
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define PG_DAFT_ARROW_TYPE_CONTRACT_VERSION 1U
#define PG_DAFT_ARROW_P0_TYPE_COUNT 10U

#define PG_DAFT_ARROW_DATE_EPOCH_OFFSET_DAYS INT64_C(10957)
#define PG_DAFT_ARROW_TIMESTAMP_EPOCH_OFFSET_SECONDS INT64_C(946684800)
#define PG_DAFT_ARROW_TIMESTAMP_EPOCH_OFFSET_US \
    (PG_DAFT_ARROW_TIMESTAMP_EPOCH_OFFSET_SECONDS * INT64_C(1000000))

#define PG_DAFT_ARROW_SCHEMA_FINGERPRINT_BYTES 32U
#define PG_DAFT_ARROW_SCHEMA_FINGERPRINT_HEX_BYTES 65U

/*
 * One row in the closed P0 transport whitelist.  arrow_format uses the
 * canonical Arrow C Data format string and must match exactly.
 */
typedef struct PgDaftArrowTypeMappingV1
{
    Oid         pg_type_oid;
    const char *arrow_format;
    const char *daft_dtype;
} PgDaftArrowTypeMappingV1;

/*
 * Canonical logical field identity used by the schema fingerprint.  Keep
 * these values explicit: deriving them again on the Rust side would lose PG
 * typmod, collation, or nullability semantics.
 */
typedef struct PgDaftArrowFieldV1
{
    Oid     pg_type_oid;
    int32   typmod;
    Oid     collation_oid;
    bool    nullable;
} PgDaftArrowFieldV1;

/* Return NULL for every type outside the ten-entry P0 whitelist. */
const PgDaftArrowTypeMappingV1 *pg_daft_arrow_lookup_type_v1(Oid pg_type_oid);

/*
 * Require the exact format assigned to pg_type_oid.  Unsupported OIDs,
 * NULL formats, aliases, and otherwise unknown formats are rejected.
 */
bool pg_daft_arrow_validate_format_v1(Oid pg_type_oid,
                                      const char *arrow_format,
                                      char **error_msg);

/*
 * SHA-256 over the canonical big-endian schema encoding documented in
 * spec/pg_daft_arrow_v1.yaml.  Column order is significant.  Only the P0
 * whitelist is accepted.
 */
bool pg_daft_arrow_schema_fingerprint_v1(
    const PgDaftArrowFieldV1 *fields,
    uint32 nfields,
    uint8 out[PG_DAFT_ARROW_SCHEMA_FINGERPRINT_BYTES],
    char **error_msg);

/* Convenience adapter preserving TupleDesc OID/typmod/collation/nullability. */
bool pg_daft_arrow_tupledesc_fingerprint_v1(
    TupleDesc tupdesc,
    uint8 out[PG_DAFT_ARROW_SCHEMA_FINGERPRINT_BYTES],
    char **error_msg);

/* Encode a raw fingerprint as 64 lowercase hexadecimal characters plus NUL. */
void pg_daft_arrow_fingerprint_hex_v1(
    const uint8 fingerprint[PG_DAFT_ARROW_SCHEMA_FINGERPRINT_BYTES],
    char out[PG_DAFT_ARROW_SCHEMA_FINGERPRINT_HEX_BYTES]);

/*
 * PG -> Arrow input builder.  All PostgreSQL slot access occurs synchronously
 * in the backend thread.  A successfully finished ArrowArray/ArrowSchema owns
 * only libc allocations; its release callback may therefore run on a Rust
 * worker thread.  Every non-successful append/finalize keeps caller ownership.
 */
typedef struct PgDaftArrowInputBuilderV1 PgDaftArrowInputBuilderV1;

typedef enum PgDaftArrowAppendResultV1
{
    PG_DAFT_ARROW_APPEND_OK = 0,
    PG_DAFT_ARROW_APPEND_FULL = 1,
    PG_DAFT_ARROW_APPEND_ERROR = 2
} PgDaftArrowAppendResultV1;

PgDaftArrowInputBuilderV1 *pg_daft_arrow_input_builder_create_v1(
    TupleDesc tupdesc,
    uint32 target_rows,
    uint64 target_bytes,
    MemoryContext parent_mcxt,
    char **error_msg);

void pg_daft_arrow_input_builder_destroy_v1(
    PgDaftArrowInputBuilderV1 *builder);

PgDaftArrowAppendResultV1 pg_daft_arrow_input_builder_append_v1(
    PgDaftArrowInputBuilderV1 *builder,
    TupleTableSlot *slot,
    char **error_msg);

bool pg_daft_arrow_input_builder_should_flush_v1(
    const PgDaftArrowInputBuilderV1 *builder);

uint32 pg_daft_arrow_input_builder_row_count_v1(
    const PgDaftArrowInputBuilderV1 *builder);

uint64 pg_daft_arrow_input_builder_bytes_v1(
    const PgDaftArrowInputBuilderV1 *builder);

/*
 * Export the current rows and reset the builder.  array/schema must be
 * zero-initialized (release == NULL).  On success with out_rows > 0, their
 * independent release callbacks each run exactly once and use libc only.
 * With no rows, success returns zeroed outputs and out_rows == 0.
 */
bool pg_daft_arrow_input_builder_finish_v1(
    PgDaftArrowInputBuilderV1 *builder,
    struct ArrowArray *array,
    struct ArrowSchema *schema,
    uint32 *out_rows,
    char **error_msg);

/* Opaque state implemented by the P0-007-C Arrow-to-PG batch adapter. */
typedef struct DaftArrowBatchContext DaftArrowBatchContext;

bool dpg_arrow_validate_metadata(const struct ArrowSchema *schema,
                                 const struct ArrowArray *array,
                                 TupleDesc tupdesc,
                                 char **error_msg);
DaftArrowBatchContext *dpg_arrow_create_batch(struct ArrowArray *array,
                                              struct ArrowSchema *schema,
                                              TupleDesc tupdesc,
                                              MemoryContext batch_mcxt,
                                              char **error_msg);
void dpg_arrow_materialize_row(DaftArrowBatchContext *batch,
                               int row_index,
                               TupleTableSlot *slot);
int64 dpg_arrow_batch_row_count(DaftArrowBatchContext *batch);
/*
 * Logical bytes crossing the Arrow boundary for this validated batch:
 * validity bitmaps, variable-width offsets, and value buffers.  This is not
 * allocator capacity or process RSS.
 */
uint64 dpg_arrow_batch_logical_bytes(DaftArrowBatchContext *batch);
void dpg_arrow_release_batch(DaftArrowBatchContext *batch);

static inline int32
arrow_date32_to_pg_date(int32 arrow_days)
{
    return arrow_days - (int32) PG_DAFT_ARROW_DATE_EPOCH_OFFSET_DAYS;
}

static inline int32
pg_date_to_arrow_date32(int32 pg_days)
{
    return pg_days + (int32) PG_DAFT_ARROW_DATE_EPOCH_OFFSET_DAYS;
}

static inline int64
arrow_timestamp_us_to_pg_timestamp(int64 arrow_us)
{
    return arrow_us - PG_DAFT_ARROW_TIMESTAMP_EPOCH_OFFSET_US;
}

static inline int64
pg_timestamp_to_arrow_timestamp_us(int64 pg_us)
{
    return pg_us + PG_DAFT_ARROW_TIMESTAMP_EPOCH_OFFSET_US;
}

#ifdef __cplusplus
}
#endif

#endif /* PG_DAFT_ARROW_H */
