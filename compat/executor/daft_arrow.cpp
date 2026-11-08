/*-------------------------------------------------------------------------
 *
 * daft_arrow.cpp
 *    Frozen PostgreSQL/Arrow type and schema contract.
 *
 * P0-007-A intentionally implements metadata only.  Batch construction and
 * materialization are added by P0-007-B/C after this single mapping has been
 * frozen and verified.
 *
 *-------------------------------------------------------------------------
 */
extern "C" {
#include "postgres.h"
#include "access/tupdesc.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_type_d.h"
#include "common/cryptohash.h"
#include "common/int.h"
#include "datatype/timestamp.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "varatt.h"
}

#include "pg_orca/daft/pg_daft_arrow.h"

#include <array>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <pthread.h>

namespace
{

constexpr char kSchemaFingerprintDomain[] = "PGDAFT_ARROW_SCHEMA_V1";

constexpr std::array<PgDaftArrowTypeMappingV1,
                     PG_DAFT_ARROW_P0_TYPE_COUNT> kP0TypeMappings = {{
    {BOOLOID, "b", "Boolean"},
    {BYTEAOID, "z", "Binary"},
    {INT8OID, "l", "Int64"},
    {INT2OID, "s", "Int16"},
    {INT4OID, "i", "Int32"},
    {TEXTOID, "u", "Utf8"},
    {VARCHAROID, "u", "Utf8"},
    {DATEOID, "tdD", "Date"},
    {TIMESTAMPOID, "tsu:", "TimestampMicroseconds"},
    {TIMESTAMPTZOID, "tsu:UTC", "TimestampMicrosecondsUTC"},
}};

constexpr bool
ContainsType(Oid oid)
{
    for (const auto &mapping : kP0TypeMappings)
    {
        if (mapping.pg_type_oid == oid)
            return true;
    }
    return false;
}

static_assert(kP0TypeMappings.size() == PG_DAFT_ARROW_P0_TYPE_COUNT,
              "P0 Arrow type count changed without a contract version bump");
static_assert(!ContainsType(FLOAT4OID) && !ContainsType(FLOAT8OID),
              "floating-point transport must remain disabled in P0");
static_assert(!ContainsType(NUMERICOID),
              "numeric transport must remain disabled in P0");
static_assert(PG_DAFT_ARROW_TIMESTAMP_EPOCH_OFFSET_US == INT64_C(946684800000000),
              "PostgreSQL/Unix timestamp epoch offset changed");

void
SetError(char **error_msg, const char *message)
{
    if (error_msg != nullptr)
        *error_msg = pstrdup(message);
}

bool
HashBytes(pg_cryptohash_ctx *ctx, const void *data, size_t length,
          char **error_msg)
{
    if (pg_cryptohash_update(ctx, static_cast<const uint8 *>(data), length) < 0)
    {
        SetError(error_msg, "PGDAFT_ARROW_SCHEMA_FINGERPRINT_UPDATE_FAILED");
        return false;
    }
    return true;
}

bool
HashU32(pg_cryptohash_ctx *ctx, uint32 value, char **error_msg)
{
    const uint8 encoded[4] = {
        static_cast<uint8>((value >> 24) & 0xffU),
        static_cast<uint8>((value >> 16) & 0xffU),
        static_cast<uint8>((value >> 8) & 0xffU),
        static_cast<uint8>(value & 0xffU),
    };
    return HashBytes(ctx, encoded, sizeof(encoded), error_msg);
}

bool
HashFields(const PgDaftArrowFieldV1 *fields, uint32 nfields,
           pg_cryptohash_ctx *ctx, char **error_msg)
{
    if (!HashBytes(ctx, kSchemaFingerprintDomain,
                   sizeof(kSchemaFingerprintDomain) - 1, error_msg) ||
        !HashU32(ctx, PG_DAFT_ARROW_TYPE_CONTRACT_VERSION, error_msg) ||
        !HashU32(ctx, nfields, error_msg))
        return false;

    for (uint32 i = 0; i < nfields; ++i)
    {
        const PgDaftArrowFieldV1 &field = fields[i];
        const PgDaftArrowTypeMappingV1 *mapping =
            pg_daft_arrow_lookup_type_v1(field.pg_type_oid);

        if (mapping == nullptr)
        {
            if (error_msg != nullptr)
                *error_msg = psprintf(
                    "PGDAFT_ARROW_UNSUPPORTED_TYPE_OID field=%u oid=%u",
                    i, field.pg_type_oid);
            return false;
        }

        const size_t format_length = std::strlen(mapping->arrow_format);
        if (format_length > std::numeric_limits<uint8>::max())
        {
            SetError(error_msg, "PGDAFT_ARROW_FORMAT_TOO_LONG");
            return false;
        }

        const uint8 format_length_u8 = static_cast<uint8>(format_length);
        const uint8 nullable = field.nullable ? 1U : 0U;

        /* int32 typmod is encoded as its two's-complement uint32 bit pattern. */
        if (!HashU32(ctx, i, error_msg) ||
            !HashU32(ctx, field.pg_type_oid, error_msg) ||
            !HashU32(ctx, static_cast<uint32>(field.typmod), error_msg) ||
            !HashU32(ctx, field.collation_oid, error_msg) ||
            !HashBytes(ctx, &nullable, sizeof(nullable), error_msg) ||
            !HashBytes(ctx, &format_length_u8, sizeof(format_length_u8),
                       error_msg) ||
            !HashBytes(ctx, mapping->arrow_format, format_length, error_msg))
            return false;
    }

    return true;
}

}  // namespace

extern "C" const PgDaftArrowTypeMappingV1 *
pg_daft_arrow_lookup_type_v1(Oid pg_type_oid)
{
    for (const auto &mapping : kP0TypeMappings)
    {
        if (mapping.pg_type_oid == pg_type_oid)
            return &mapping;
    }
    return nullptr;
}

extern "C" bool
pg_daft_arrow_validate_format_v1(Oid pg_type_oid, const char *arrow_format,
                                 char **error_msg)
{
    if (error_msg != nullptr)
        *error_msg = nullptr;

    const PgDaftArrowTypeMappingV1 *mapping =
        pg_daft_arrow_lookup_type_v1(pg_type_oid);
    if (mapping == nullptr)
    {
        if (error_msg != nullptr)
            *error_msg = psprintf("PGDAFT_ARROW_UNSUPPORTED_TYPE_OID oid=%u",
                                  pg_type_oid);
        return false;
    }
    if (arrow_format == nullptr ||
        std::strcmp(mapping->arrow_format, arrow_format) != 0)
    {
        if (error_msg != nullptr)
            *error_msg = psprintf(
                "PGDAFT_ARROW_FORMAT_MISMATCH oid=%u expected=%s",
                pg_type_oid, mapping->arrow_format);
        return false;
    }
    return true;
}

extern "C" bool
pg_daft_arrow_schema_fingerprint_v1(
    const PgDaftArrowFieldV1 *fields, uint32 nfields,
    uint8 out[PG_DAFT_ARROW_SCHEMA_FINGERPRINT_BYTES], char **error_msg)
{
    if (error_msg != nullptr)
        *error_msg = nullptr;
    if (out == nullptr || (nfields != 0 && fields == nullptr))
    {
        SetError(error_msg, "PGDAFT_ARROW_INVALID_SCHEMA_ARGUMENT");
        return false;
    }

    pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);
    if (ctx == nullptr)
    {
        SetError(error_msg, "PGDAFT_ARROW_SCHEMA_FINGERPRINT_CREATE_FAILED");
        return false;
    }

    bool ok = pg_cryptohash_init(ctx) == 0;
    if (!ok)
        SetError(error_msg, "PGDAFT_ARROW_SCHEMA_FINGERPRINT_INIT_FAILED");
    if (ok)
        ok = HashFields(fields, nfields, ctx, error_msg);
    if (ok && pg_cryptohash_final(ctx, out,
                                  PG_DAFT_ARROW_SCHEMA_FINGERPRINT_BYTES) < 0)
    {
        SetError(error_msg, "PGDAFT_ARROW_SCHEMA_FINGERPRINT_FINAL_FAILED");
        ok = false;
    }
    pg_cryptohash_free(ctx);
    return ok;
}

extern "C" bool
pg_daft_arrow_tupledesc_fingerprint_v1(
    TupleDesc tupdesc,
    uint8 out[PG_DAFT_ARROW_SCHEMA_FINGERPRINT_BYTES], char **error_msg)
{
    if (error_msg != nullptr)
        *error_msg = nullptr;
    if (tupdesc == nullptr || out == nullptr)
    {
        SetError(error_msg, "PGDAFT_ARROW_INVALID_TUPLEDESC_ARGUMENT");
        return false;
    }

    PgDaftArrowFieldV1 *fields = static_cast<PgDaftArrowFieldV1 *>(
        palloc(sizeof(PgDaftArrowFieldV1) * tupdesc->natts));

    for (int i = 0; i < tupdesc->natts; ++i)
    {
        Form_pg_attribute attribute = TupleDescAttr(tupdesc, i);
        if (attribute->attisdropped)
        {
            if (error_msg != nullptr)
                *error_msg = psprintf(
                    "PGDAFT_ARROW_DROPPED_ATTRIBUTE field=%d", i);
            pfree(fields);
            return false;
        }
        fields[i] = {
            attribute->atttypid,
            attribute->atttypmod,
            attribute->attcollation,
            !attribute->attnotnull,
        };
    }

    const bool ok = pg_daft_arrow_schema_fingerprint_v1(
        fields, static_cast<uint32>(tupdesc->natts), out, error_msg);
    pfree(fields);
    return ok;
}

extern "C" void
pg_daft_arrow_fingerprint_hex_v1(
    const uint8 fingerprint[PG_DAFT_ARROW_SCHEMA_FINGERPRINT_BYTES],
    char out[PG_DAFT_ARROW_SCHEMA_FINGERPRINT_HEX_BYTES])
{
    static constexpr char kHex[] = "0123456789abcdef";
    for (uint32 i = 0; i < PG_DAFT_ARROW_SCHEMA_FINGERPRINT_BYTES; ++i)
    {
        out[i * 2] = kHex[fingerprint[i] >> 4];
        out[i * 2 + 1] = kHex[fingerprint[i] & 0x0fU];
    }
    out[PG_DAFT_ARROW_SCHEMA_FINGERPRINT_BYTES * 2] = '\0';
}

#if defined(PG_ORCA_ENABLE_DAFT) && PG_ORCA_ENABLE_DAFT

struct PgDaftArrowInputColumnV1
{
    Oid pg_type_oid;
    int32 typmod;
    Oid collation_oid;
    bool nullable;
    const PgDaftArrowTypeMappingV1 *mapping;
    char *name;

    uint8 *validity;
    void *data;
    int32 *offsets;
    uint32 row_capacity;
    size_t data_capacity;
    size_t payload_bytes;
    uint32 null_count;
};

struct PgDaftArrowInputBuilderV1
{
    MemoryContext builder_mcxt;
    uint32 ncolumns;
    uint32 target_rows;
    uint64 target_bytes;
    uint32 nrows;
    uint64 bytes_used;
    PgDaftArrowInputColumnV1 *columns;
};

struct DaftArrowBatchContext
{
    ArrowArray array;
    ArrowSchema schema;
    MemoryContext output_batch_mcxt;
    TupleDesc tupdesc;
    int64 nrows;
    int ncolumns;
    uint64 logical_bytes;
};

namespace
{

constexpr uint32 kDefaultBatchRows = 1024;
constexpr uint32 kMaximumBatchRows = 65536;
constexpr uint64 kDefaultBatchBytes = UINT64_C(16) * 1024U * 1024U;
constexpr char kFingerprintMetadataKey[] = "pg_daft_schema_fingerprint_v1";

static_assert(sizeof(ArrowSchema) == 9 * sizeof(void *),
              "unexpected ArrowSchema C Data ABI size");
static_assert(sizeof(ArrowArray) == 10 * sizeof(void *),
              "unexpected ArrowArray C Data ABI size");

struct LibcArrayOwner
{
    bool owns_buffers;
    int64 nchildren;
    ArrowArray **children;
    const void **published_buffers;
    uint32 nowned_buffers;
    void *owned_buffers[3];
};

struct LibcSchemaOwner
{
    int64 nchildren;
    ArrowSchema **children;
    char *format;
    char *name;
    char *metadata;
};

struct VarlenaView
{
    const char *data;
    size_t length;
};

void
ReleaseLibcArray(ArrowArray *array)
{
    if (array == nullptr || array->release == nullptr)
        return;

    LibcArrayOwner *owner = static_cast<LibcArrayOwner *>(array->private_data);
    array->release = nullptr;
    if (owner != nullptr)
    {
        for (int64 i = 0; i < owner->nchildren; ++i)
        {
            ArrowArray *child = owner->children[i];
            if (child != nullptr)
            {
                if (child->release != nullptr)
                    child->release(child);
                std::free(child);
            }
        }
        if (owner->owns_buffers)
        {
            for (uint32 i = 0; i < owner->nowned_buffers; ++i)
                std::free(owner->owned_buffers[i]);
        }
        std::free(owner->published_buffers);
        std::free(owner->children);
        std::free(owner);
    }
    std::memset(array, 0, sizeof(*array));
}

void
ReleaseLibcSchema(ArrowSchema *schema)
{
    if (schema == nullptr || schema->release == nullptr)
        return;

    LibcSchemaOwner *owner = static_cast<LibcSchemaOwner *>(schema->private_data);
    schema->release = nullptr;
    if (owner != nullptr)
    {
        for (int64 i = 0; i < owner->nchildren; ++i)
        {
            ArrowSchema *child = owner->children[i];
            if (child != nullptr)
            {
                if (child->release != nullptr)
                    child->release(child);
                std::free(child);
            }
        }
        std::free(owner->format);
        std::free(owner->name);
        std::free(owner->metadata);
        std::free(owner->children);
        std::free(owner);
    }
    std::memset(schema, 0, sizeof(*schema));
}

char *
LibcStringDup(const char *value)
{
    const size_t length = std::strlen(value) + 1;
    char *copy = static_cast<char *>(std::malloc(length));
    if (copy != nullptr)
        std::memcpy(copy, value, length);
    return copy;
}

bool
GrowBuffer(void **buffer, size_t old_size, size_t new_size, bool zero_new,
           char **error_msg)
{
    void *replacement = std::realloc(*buffer, new_size);
    if (replacement == nullptr && new_size != 0)
    {
        SetError(error_msg, "PGDAFT_ARROW_LIBC_ALLOCATION_FAILED");
        return false;
    }
    if (zero_new && new_size > old_size)
        std::memset(static_cast<char *>(replacement) + old_size, 0,
                    new_size - old_size);
    *buffer = replacement;
    return true;
}

bool
IsVariableWidth(Oid oid)
{
    return oid == TEXTOID || oid == VARCHAROID || oid == BYTEAOID;
}

bool
IsBoolean(Oid oid)
{
    return oid == BOOLOID;
}

bool
IsAligned(const void *pointer, size_t alignment)
{
    return pointer == nullptr ||
           reinterpret_cast<uintptr_t>(pointer) % alignment == 0;
}

size_t
FixedWidth(Oid oid)
{
    switch (oid)
    {
        case INT2OID:
            return sizeof(int16);
        case INT4OID:
        case DATEOID:
            return sizeof(int32);
        case INT8OID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            return sizeof(int64);
        default:
            return 0;
    }
}

bool
EnsureRowCapacity(PgDaftArrowInputBuilderV1 *builder,
                  PgDaftArrowInputColumnV1 *column, uint32 required,
                  char **error_msg)
{
    if (required <= column->row_capacity)
        return true;

    uint32 new_capacity = column->row_capacity == 0
                              ? std::min<uint32>(builder->target_rows, 64U)
                              : column->row_capacity;
    while (new_capacity < required)
    {
        if (new_capacity > builder->target_rows / 2U)
            new_capacity = builder->target_rows;
        else
            new_capacity *= 2U;
    }

    const size_t old_bitmap_bytes = (column->row_capacity + 7U) / 8U;
    const size_t new_bitmap_bytes = (new_capacity + 7U) / 8U;
    if (!GrowBuffer(reinterpret_cast<void **>(&column->validity),
                    old_bitmap_bytes, new_bitmap_bytes, true, error_msg))
        return false;

    if (IsBoolean(column->pg_type_oid))
    {
        if (!GrowBuffer(&column->data, old_bitmap_bytes, new_bitmap_bytes,
                        true, error_msg))
            return false;
    }
    else if (IsVariableWidth(column->pg_type_oid))
    {
        const size_t old_offsets =
            (static_cast<size_t>(column->row_capacity) + 1U) * sizeof(int32);
        const size_t new_offsets =
            (static_cast<size_t>(new_capacity) + 1U) * sizeof(int32);
        if (!GrowBuffer(reinterpret_cast<void **>(&column->offsets),
                        column->row_capacity == 0 ? 0 : old_offsets,
                        new_offsets, true, error_msg))
            return false;
        if (column->row_capacity == 0)
            column->offsets[0] = 0;
    }
    else
    {
        const size_t width = FixedWidth(column->pg_type_oid);
        if (!GrowBuffer(&column->data,
                        static_cast<size_t>(column->row_capacity) * width,
                        static_cast<size_t>(new_capacity) * width, true,
                        error_msg))
            return false;
    }

    column->row_capacity = new_capacity;
    return true;
}

bool
EnsurePayloadCapacity(PgDaftArrowInputColumnV1 *column, size_t required,
                      char **error_msg)
{
    if (required <= column->data_capacity)
        return true;

    size_t new_capacity = column->data_capacity == 0 ? 256U
                                                     : column->data_capacity;
    while (new_capacity < required)
    {
        if (new_capacity > static_cast<size_t>(INT32_MAX) / 2U)
        {
            new_capacity = required;
            break;
        }
        new_capacity *= 2U;
    }
    if (!GrowBuffer(&column->data, column->data_capacity, new_capacity, false,
                    error_msg))
        return false;
    column->data_capacity = new_capacity;
    return true;
}

uint64
CalculateUsedBytes(const PgDaftArrowInputBuilderV1 *builder)
{
    uint64 total = 0;
    for (uint32 i = 0; i < builder->ncolumns; ++i)
    {
        const PgDaftArrowInputColumnV1 &column = builder->columns[i];
        total += (builder->nrows + 7U) / 8U;
        if (IsBoolean(column.pg_type_oid))
            total += (builder->nrows + 7U) / 8U;
        else if (IsVariableWidth(column.pg_type_oid))
            total += (static_cast<uint64>(builder->nrows) + 1U) * sizeof(int32) +
                     column.payload_bytes;
        else
            total += static_cast<uint64>(builder->nrows) *
                     FixedWidth(column.pg_type_oid);
    }
    return total;
}

bool
InitializeSchemaNode(ArrowSchema *schema, const char *format, const char *name,
                     int64 flags, int64 nchildren, char **error_msg)
{
    std::memset(schema, 0, sizeof(*schema));
    LibcSchemaOwner *owner =
        static_cast<LibcSchemaOwner *>(std::calloc(1, sizeof(*owner)));
    if (owner == nullptr)
    {
        SetError(error_msg, "PGDAFT_ARROW_LIBC_ALLOCATION_FAILED");
        return false;
    }
    schema->release = ReleaseLibcSchema;
    schema->private_data = owner;
    owner->nchildren = nchildren;
    owner->format = LibcStringDup(format);
    owner->name = LibcStringDup(name);
    if (owner->format == nullptr || owner->name == nullptr)
    {
        SetError(error_msg, "PGDAFT_ARROW_LIBC_ALLOCATION_FAILED");
        schema->release(schema);
        return false;
    }
    if (nchildren > 0)
    {
        owner->children = static_cast<ArrowSchema **>(
            std::calloc(static_cast<size_t>(nchildren), sizeof(ArrowSchema *)));
        if (owner->children == nullptr)
        {
            SetError(error_msg, "PGDAFT_ARROW_LIBC_ALLOCATION_FAILED");
            schema->release(schema);
            return false;
        }
    }
    schema->format = owner->format;
    schema->name = owner->name;
    schema->flags = flags;
    schema->n_children = nchildren;
    schema->children = owner->children;
    return true;
}

char *
BuildFingerprintMetadata(const uint8 fingerprint[32], char **error_msg)
{
    char hex[PG_DAFT_ARROW_SCHEMA_FINGERPRINT_HEX_BYTES];
    pg_daft_arrow_fingerprint_hex_v1(fingerprint, hex);
    const int32 entries = 1;
    const int32 key_length = sizeof(kFingerprintMetadataKey) - 1;
    const int32 value_length = PG_DAFT_ARROW_SCHEMA_FINGERPRINT_HEX_BYTES - 1;
    const size_t total = sizeof(int32) * 3U + key_length + value_length;
    char *metadata = static_cast<char *>(std::malloc(total));
    if (metadata == nullptr)
    {
        SetError(error_msg, "PGDAFT_ARROW_LIBC_ALLOCATION_FAILED");
        return nullptr;
    }
    char *cursor = metadata;
    std::memcpy(cursor, &entries, sizeof(entries));
    cursor += sizeof(entries);
    std::memcpy(cursor, &key_length, sizeof(key_length));
    cursor += sizeof(key_length);
    std::memcpy(cursor, kFingerprintMetadataKey, key_length);
    cursor += key_length;
    std::memcpy(cursor, &value_length, sizeof(value_length));
    cursor += sizeof(value_length);
    std::memcpy(cursor, hex, value_length);
    return metadata;
}

bool
BuildSchemaTree(const PgDaftArrowInputBuilderV1 *builder, ArrowSchema *schema,
                char **error_msg)
{
    if (!InitializeSchemaNode(schema, "+s", "", 0, builder->ncolumns,
                              error_msg))
        return false;

    LibcSchemaOwner *top_owner =
        static_cast<LibcSchemaOwner *>(schema->private_data);
    for (uint32 i = 0; i < builder->ncolumns; ++i)
    {
        ArrowSchema *child =
            static_cast<ArrowSchema *>(std::calloc(1, sizeof(*child)));
        if (child == nullptr)
        {
            SetError(error_msg, "PGDAFT_ARROW_LIBC_ALLOCATION_FAILED");
            schema->release(schema);
            return false;
        }
        top_owner->children[i] = child;
        const PgDaftArrowInputColumnV1 &column = builder->columns[i];
        const int64 flags = column.nullable ? ARROW_FLAG_NULLABLE : 0;
        if (!InitializeSchemaNode(child, column.mapping->arrow_format,
                                  column.name, flags, 0, error_msg))
        {
            schema->release(schema);
            return false;
        }
    }

    PgDaftArrowFieldV1 *fields = static_cast<PgDaftArrowFieldV1 *>(
        palloc(sizeof(*fields) * builder->ncolumns));
    for (uint32 i = 0; i < builder->ncolumns; ++i)
    {
        const PgDaftArrowInputColumnV1 &column = builder->columns[i];
        fields[i] = {column.pg_type_oid, column.typmod,
                     column.collation_oid, column.nullable};
    }
    uint8 fingerprint[PG_DAFT_ARROW_SCHEMA_FINGERPRINT_BYTES];
    const bool fingerprint_ok = pg_daft_arrow_schema_fingerprint_v1(
        fields, builder->ncolumns, fingerprint, error_msg);
    pfree(fields);
    if (!fingerprint_ok)
    {
        schema->release(schema);
        return false;
    }
    top_owner->metadata = BuildFingerprintMetadata(fingerprint, error_msg);
    if (top_owner->metadata == nullptr)
    {
        schema->release(schema);
        return false;
    }
    schema->metadata = top_owner->metadata;
    return true;
}

bool
InitializeArrayNode(ArrowArray *array, int64 length, int64 null_count,
                    int64 nbuffers, int64 nchildren, char **error_msg)
{
    std::memset(array, 0, sizeof(*array));
    LibcArrayOwner *owner =
        static_cast<LibcArrayOwner *>(std::calloc(1, sizeof(*owner)));
    if (owner == nullptr)
    {
        SetError(error_msg, "PGDAFT_ARROW_LIBC_ALLOCATION_FAILED");
        return false;
    }
    array->release = ReleaseLibcArray;
    array->private_data = owner;
    owner->nchildren = nchildren;
    if (nbuffers > 0)
    {
        owner->published_buffers = static_cast<const void **>(
            std::calloc(static_cast<size_t>(nbuffers), sizeof(void *)));
        if (owner->published_buffers == nullptr)
        {
            SetError(error_msg, "PGDAFT_ARROW_LIBC_ALLOCATION_FAILED");
            array->release(array);
            return false;
        }
    }
    if (nchildren > 0)
    {
        owner->children = static_cast<ArrowArray **>(
            std::calloc(static_cast<size_t>(nchildren), sizeof(ArrowArray *)));
        if (owner->children == nullptr)
        {
            SetError(error_msg, "PGDAFT_ARROW_LIBC_ALLOCATION_FAILED");
            array->release(array);
            return false;
        }
    }
    array->length = length;
    array->null_count = null_count;
    array->n_buffers = nbuffers;
    array->n_children = nchildren;
    array->buffers = owner->published_buffers;
    array->children = owner->children;
    return true;
}

bool
BuildArrayTree(PgDaftArrowInputBuilderV1 *builder, ArrowArray *array,
               char **error_msg)
{
    if (!InitializeArrayNode(array, builder->nrows, 0, 1, builder->ncolumns,
                             error_msg))
        return false;

    LibcArrayOwner *top_owner =
        static_cast<LibcArrayOwner *>(array->private_data);
    for (uint32 i = 0; i < builder->ncolumns; ++i)
    {
        PgDaftArrowInputColumnV1 &column = builder->columns[i];
        const int64 nbuffers = IsVariableWidth(column.pg_type_oid) ? 3 : 2;
        ArrowArray *child =
            static_cast<ArrowArray *>(std::calloc(1, sizeof(*child)));
        if (child == nullptr)
        {
            SetError(error_msg, "PGDAFT_ARROW_LIBC_ALLOCATION_FAILED");
            array->release(array);
            return false;
        }
        top_owner->children[i] = child;
        if (!InitializeArrayNode(child, builder->nrows, column.null_count,
                                 nbuffers, 0, error_msg))
        {
            array->release(array);
            return false;
        }
        LibcArrayOwner *owner =
            static_cast<LibcArrayOwner *>(child->private_data);
        owner->nowned_buffers = static_cast<uint32>(nbuffers);
        owner->owned_buffers[0] = column.validity;
        owner->published_buffers[0] = column.validity;
        if (IsVariableWidth(column.pg_type_oid))
        {
            owner->owned_buffers[1] = column.offsets;
            owner->owned_buffers[2] = column.data;
            owner->published_buffers[1] = column.offsets;
            owner->published_buffers[2] = column.data;
        }
        else
        {
            owner->owned_buffers[1] = column.data;
            owner->published_buffers[1] = column.data;
        }
    }
    return true;
}

bool
IsNullAt(const ArrowArray *array, int64 row)
{
    if (array->null_count == 0 || array->buffers[0] == nullptr)
        return false;
    const uint8 *validity = static_cast<const uint8 *>(array->buffers[0]);
    return (validity[row / 8] & static_cast<uint8>(1U << (row % 8))) == 0;
}

bool
ValidateFingerprintMetadata(const ArrowSchema *schema, TupleDesc tupdesc,
                            char **error_msg)
{
    if (schema->metadata == nullptr)
    {
        SetError(error_msg, "PGDAFT_ARROW_SCHEMA_FINGERPRINT_MISSING");
        return false;
    }

    const char *cursor = schema->metadata;
    int32 entries;
    int32 key_length;
    int32 value_length;
    std::memcpy(&entries, cursor, sizeof(entries));
    cursor += sizeof(entries);
    if (entries != 1)
    {
        SetError(error_msg, "PGDAFT_ARROW_SCHEMA_METADATA_ENTRY_COUNT");
        return false;
    }
    std::memcpy(&key_length, cursor, sizeof(key_length));
    cursor += sizeof(key_length);
    if (key_length != static_cast<int32>(sizeof(kFingerprintMetadataKey) - 1) ||
        std::memcmp(cursor, kFingerprintMetadataKey,
                    sizeof(kFingerprintMetadataKey) - 1) != 0)
    {
        SetError(error_msg, "PGDAFT_ARROW_SCHEMA_FINGERPRINT_KEY_MISMATCH");
        return false;
    }
    cursor += key_length;
    std::memcpy(&value_length, cursor, sizeof(value_length));
    cursor += sizeof(value_length);
    if (value_length !=
        static_cast<int32>(PG_DAFT_ARROW_SCHEMA_FINGERPRINT_HEX_BYTES - 1))
    {
        SetError(error_msg, "PGDAFT_ARROW_SCHEMA_FINGERPRINT_LENGTH");
        return false;
    }

    uint8 expected[PG_DAFT_ARROW_SCHEMA_FINGERPRINT_BYTES];
    if (!pg_daft_arrow_tupledesc_fingerprint_v1(tupdesc, expected, error_msg))
        return false;
    char expected_hex[PG_DAFT_ARROW_SCHEMA_FINGERPRINT_HEX_BYTES];
    pg_daft_arrow_fingerprint_hex_v1(expected, expected_hex);
    if (std::memcmp(cursor, expected_hex,
                    PG_DAFT_ARROW_SCHEMA_FINGERPRINT_HEX_BYTES - 1) != 0)
    {
        SetError(error_msg, "PGDAFT_ARROW_SCHEMA_FINGERPRINT_MISMATCH");
        return false;
    }
    return true;
}

bool
ValidateChild(const ArrowSchema *schema, const ArrowArray *array,
              Form_pg_attribute attribute, int column_index, int64 nrows,
              char **error_msg)
{
    if (schema == nullptr || array == nullptr || schema->format == nullptr)
    {
        if (error_msg != nullptr)
            *error_msg = psprintf("PGDAFT_ARROW_NULL_CHILD field=%d",
                                  column_index);
        return false;
    }
    if (schema->release == nullptr || array->release == nullptr)
    {
        if (error_msg != nullptr)
            *error_msg = psprintf("PGDAFT_ARROW_RELEASE_MISSING field=%d",
                                  column_index);
        return false;
    }
    if (schema->dictionary != nullptr || array->dictionary != nullptr ||
        schema->n_children != 0 || array->n_children != 0)
    {
        if (error_msg != nullptr)
            *error_msg = psprintf("PGDAFT_ARROW_NESTED_TYPE_REJECTED field=%d",
                                  column_index);
        return false;
    }
    const int64 expected_flags = attribute->attnotnull ? 0 : ARROW_FLAG_NULLABLE;
    if (schema->flags != expected_flags)
    {
        if (error_msg != nullptr)
            *error_msg = psprintf(
                "PGDAFT_ARROW_NULLABILITY_MISMATCH field=%d expected_flags=%lld actual_flags=%lld",
                column_index, static_cast<long long>(expected_flags),
                static_cast<long long>(schema->flags));
        return false;
    }
    if (!pg_daft_arrow_validate_format_v1(attribute->atttypid,
                                           schema->format, error_msg))
        return false;
    if (array->length != nrows || array->offset != 0 ||
        array->null_count < 0 || array->null_count > nrows)
    {
        if (error_msg != nullptr)
            *error_msg = psprintf(
                "PGDAFT_ARROW_CHILD_LENGTH_INVALID field=%d", column_index);
        return false;
    }

    const bool variable = IsVariableWidth(attribute->atttypid);
    const int64 expected_buffers = variable ? 3 : 2;
    if (array->n_buffers != expected_buffers || array->buffers == nullptr)
    {
        if (error_msg != nullptr)
            *error_msg = psprintf(
                "PGDAFT_ARROW_BUFFER_COUNT_INVALID field=%d", column_index);
        return false;
    }
    if (array->null_count > 0 && array->buffers[0] == nullptr)
    {
        if (error_msg != nullptr)
            *error_msg = psprintf(
                "PGDAFT_ARROW_VALIDITY_BUFFER_MISSING field=%d", column_index);
        return false;
    }

    int64 observed_nulls = 0;
    for (int64 row = 0; row < nrows; ++row)
        observed_nulls += IsNullAt(array, row) ? 1 : 0;
    if (observed_nulls != array->null_count ||
        (attribute->attnotnull && observed_nulls != 0))
    {
        if (error_msg != nullptr)
            *error_msg = psprintf(
                "PGDAFT_ARROW_NULL_COUNT_MISMATCH field=%d", column_index);
        return false;
    }

    const int64 nonnull_rows = nrows - observed_nulls;
    if (variable)
    {
        if (nrows > 0 && array->buffers[1] == nullptr)
        {
            if (error_msg != nullptr)
                *error_msg = psprintf(
                    "PGDAFT_ARROW_OFFSET_BUFFER_MISSING field=%d",
                    column_index);
            return false;
        }
        if (nrows == 0)
            return true;

        const int32 *offsets = static_cast<const int32 *>(array->buffers[1]);
        if (!IsAligned(offsets, alignof(int32)))
        {
            if (error_msg != nullptr)
                *error_msg = psprintf(
                    "PGDAFT_ARROW_BUFFER_ALIGNMENT_INVALID field=%d",
                    column_index);
            return false;
        }
        if (offsets[0] != 0)
        {
            if (error_msg != nullptr)
                *error_msg = psprintf(
                    "PGDAFT_ARROW_OFFSET_BASE_INVALID field=%d", column_index);
            return false;
        }
        for (int64 row = 0; row < nrows; ++row)
        {
            if (offsets[row] < 0 || offsets[row + 1] < offsets[row])
            {
                if (error_msg != nullptr)
                    *error_msg = psprintf(
                        "PGDAFT_ARROW_OFFSETS_NOT_MONOTONIC field=%d row=%lld",
                        column_index, static_cast<long long>(row));
                return false;
            }
        }
        const int32 payload_length = offsets[nrows];
        if (payload_length > 0 && array->buffers[2] == nullptr)
        {
            if (error_msg != nullptr)
                *error_msg = psprintf(
                    "PGDAFT_ARROW_DATA_BUFFER_MISSING field=%d", column_index);
            return false;
        }
        if ((attribute->atttypid == TEXTOID ||
             attribute->atttypid == VARCHAROID) &&
            payload_length > 0)
        {
            const char *data = static_cast<const char *>(array->buffers[2]);
            for (int64 row = 0; row < nrows; ++row)
            {
                if (IsNullAt(array, row))
                    continue;
                const int32 length = offsets[row + 1] - offsets[row];
                if (pg_verify_mbstr_len(PG_UTF8, data + offsets[row], length,
                                        true) < 0)
                {
                    if (error_msg != nullptr)
                        *error_msg = psprintf(
                            "PGDAFT_ARROW_UTF8_INVALID field=%d row=%lld",
                            column_index, static_cast<long long>(row));
                    return false;
                }
            }
        }
        return true;
    }

    if (nonnull_rows > 0 && array->buffers[1] == nullptr)
    {
        if (error_msg != nullptr)
            *error_msg = psprintf(
                "PGDAFT_ARROW_DATA_BUFFER_MISSING field=%d", column_index);
        return false;
    }
    const size_t fixed_alignment = IsBoolean(attribute->atttypid)
                                       ? alignof(uint8)
                                       : FixedWidth(attribute->atttypid);
    if (!IsAligned(array->buffers[1], fixed_alignment))
    {
        if (error_msg != nullptr)
            *error_msg = psprintf(
                "PGDAFT_ARROW_BUFFER_ALIGNMENT_INVALID field=%d",
                column_index);
        return false;
    }
    if (nonnull_rows == 0)
        return true;

    if (attribute->atttypid == DATEOID)
    {
        const int32 *values = static_cast<const int32 *>(array->buffers[1]);
        for (int64 row = 0; row < nrows; ++row)
        {
            if (IsNullAt(array, row))
                continue;
            const int64 converted = static_cast<int64>(values[row]) -
                                    PG_DAFT_ARROW_DATE_EPOCH_OFFSET_DAYS;
            if (converted < std::numeric_limits<int32>::min() ||
                converted > std::numeric_limits<int32>::max() ||
                DATE_NOT_FINITE(static_cast<DateADT>(converted)) ||
                !IS_VALID_DATE(static_cast<DateADT>(converted)))
            {
                SetError(error_msg, "PGDAFT_ARROW_DATE_OUT_OF_RANGE");
                return false;
            }
        }
    }
    else if (attribute->atttypid == TIMESTAMPOID ||
             attribute->atttypid == TIMESTAMPTZOID)
    {
        const int64 *values = static_cast<const int64 *>(array->buffers[1]);
        for (int64 row = 0; row < nrows; ++row)
        {
            if (IsNullAt(array, row))
                continue;
            int64 converted;
            if (pg_sub_s64_overflow(values[row],
                                    PG_DAFT_ARROW_TIMESTAMP_EPOCH_OFFSET_US,
                                    &converted) ||
                TIMESTAMP_NOT_FINITE(converted) ||
                !IS_VALID_TIMESTAMP(converted))
            {
                SetError(error_msg, "PGDAFT_ARROW_TIMESTAMP_OUT_OF_RANGE");
                return false;
            }
        }
    }
    return true;
}

struct ReleaseOnWorkerArgs
{
    ArrowArray *array;
    ArrowSchema *schema;
};

void *
ReleaseOnWorker(void *opaque)
{
    ReleaseOnWorkerArgs *args = static_cast<ReleaseOnWorkerArgs *>(opaque);
    args->array->release(args->array);
    args->schema->release(args->schema);
    /* Arrow C Data makes NULL release the released state; a second cleanup
     * pass must therefore be a no-op instead of a double free. */
    if (args->array->release != nullptr)
        args->array->release(args->array);
    if (args->schema->release != nullptr)
        args->schema->release(args->schema);
    return nullptr;
}

void
TransferArrayBuffers(PgDaftArrowInputBuilderV1 *builder, ArrowArray *array)
{
    for (uint32 i = 0; i < builder->ncolumns; ++i)
    {
        ArrowArray *child = array->children[i];
        LibcArrayOwner *owner =
            static_cast<LibcArrayOwner *>(child->private_data);
        owner->owns_buffers = true;

        PgDaftArrowInputColumnV1 &column = builder->columns[i];
        column.validity = nullptr;
        column.data = nullptr;
        column.offsets = nullptr;
        column.row_capacity = 0;
        column.data_capacity = 0;
        column.payload_bytes = 0;
        column.null_count = 0;
    }
    builder->nrows = 0;
    builder->bytes_used = 0;
}

}  // namespace

extern "C" PgDaftArrowInputBuilderV1 *
pg_daft_arrow_input_builder_create_v1(TupleDesc tupdesc, uint32 target_rows,
                                      uint64 target_bytes,
                                      MemoryContext parent_mcxt,
                                      char **error_msg)
{
    if (error_msg != nullptr)
        *error_msg = nullptr;
    if (tupdesc == nullptr)
    {
        SetError(error_msg, "PGDAFT_ARROW_INVALID_TUPLEDESC_ARGUMENT");
        return nullptr;
    }
    if (target_rows == 0)
        target_rows = kDefaultBatchRows;
    if (target_rows > kMaximumBatchRows)
    {
        SetError(error_msg, "PGDAFT_ARROW_BATCH_ROWS_OUT_OF_RANGE");
        return nullptr;
    }
    if (target_bytes == 0)
        target_bytes = kDefaultBatchBytes;
    if (parent_mcxt == nullptr)
        parent_mcxt = CurrentMemoryContext;

    for (int i = 0; i < tupdesc->natts; ++i)
    {
        Form_pg_attribute attribute = TupleDescAttr(tupdesc, i);
        if (attribute->attisdropped)
        {
            if (error_msg != nullptr)
                *error_msg = psprintf(
                    "PGDAFT_ARROW_DROPPED_ATTRIBUTE field=%d", i);
            return nullptr;
        }
        if (pg_daft_arrow_lookup_type_v1(attribute->atttypid) == nullptr)
        {
            if (error_msg != nullptr)
                *error_msg = psprintf(
                    "PGDAFT_ARROW_UNSUPPORTED_TYPE_OID field=%d oid=%u",
                    i, attribute->atttypid);
            return nullptr;
        }
        if ((attribute->atttypid == TEXTOID ||
             attribute->atttypid == VARCHAROID) &&
            GetDatabaseEncoding() != PG_UTF8)
        {
            SetError(error_msg, "PGDAFT_ARROW_DATABASE_ENCODING_NOT_UTF8");
            return nullptr;
        }
    }

    MemoryContext builder_mcxt = AllocSetContextCreate(
        parent_mcxt, "PG Daft Arrow input builder", ALLOCSET_DEFAULT_SIZES);
    MemoryContext old_mcxt = MemoryContextSwitchTo(builder_mcxt);
    PgDaftArrowInputBuilderV1 *builder =
        static_cast<PgDaftArrowInputBuilderV1 *>(palloc0(sizeof(*builder)));
    builder->builder_mcxt = builder_mcxt;
    builder->ncolumns = static_cast<uint32>(tupdesc->natts);
    builder->target_rows = target_rows;
    builder->target_bytes = target_bytes;
    builder->columns = static_cast<PgDaftArrowInputColumnV1 *>(
        palloc0(sizeof(*builder->columns) * builder->ncolumns));
    for (uint32 i = 0; i < builder->ncolumns; ++i)
    {
        Form_pg_attribute attribute = TupleDescAttr(tupdesc, i);
        PgDaftArrowInputColumnV1 &column = builder->columns[i];
        column.pg_type_oid = attribute->atttypid;
        column.typmod = attribute->atttypmod;
        column.collation_oid = attribute->attcollation;
        column.nullable = !attribute->attnotnull;
        column.mapping = pg_daft_arrow_lookup_type_v1(attribute->atttypid);
        column.name = pstrdup(NameStr(attribute->attname));
    }
    MemoryContextSwitchTo(old_mcxt);
    return builder;
}

extern "C" void
pg_daft_arrow_input_builder_destroy_v1(PgDaftArrowInputBuilderV1 *builder)
{
    if (builder == nullptr)
        return;
    MemoryContext builder_mcxt = builder->builder_mcxt;
    for (uint32 i = 0; i < builder->ncolumns; ++i)
    {
        std::free(builder->columns[i].validity);
        std::free(builder->columns[i].data);
        std::free(builder->columns[i].offsets);
    }
    MemoryContextDelete(builder_mcxt);
}

extern "C" PgDaftArrowAppendResultV1
pg_daft_arrow_input_builder_append_v1(PgDaftArrowInputBuilderV1 *builder,
                                      TupleTableSlot *slot, char **error_msg)
{
    if (error_msg != nullptr)
        *error_msg = nullptr;
    if (builder == nullptr || slot == nullptr || slot->tts_tupleDescriptor == nullptr)
    {
        SetError(error_msg, "PGDAFT_ARROW_INVALID_APPEND_ARGUMENT");
        return PG_DAFT_ARROW_APPEND_ERROR;
    }
    if (builder->nrows > 0 && pg_daft_arrow_input_builder_should_flush_v1(builder))
        return PG_DAFT_ARROW_APPEND_FULL;
    if (slot->tts_tupleDescriptor->natts != static_cast<int>(builder->ncolumns))
    {
        SetError(error_msg, "PGDAFT_ARROW_SLOT_COLUMN_COUNT_MISMATCH");
        return PG_DAFT_ARROW_APPEND_ERROR;
    }

    slot_getallattrs(slot);
    VarlenaView *views = static_cast<VarlenaView *>(
        palloc0(sizeof(*views) * builder->ncolumns));

    for (uint32 i = 0; i < builder->ncolumns; ++i)
    {
        PgDaftArrowInputColumnV1 &column = builder->columns[i];
        Form_pg_attribute attribute = TupleDescAttr(slot->tts_tupleDescriptor, i);
        if (attribute->atttypid != column.pg_type_oid ||
            attribute->atttypmod != column.typmod ||
            attribute->attcollation != column.collation_oid)
        {
            if (error_msg != nullptr)
                *error_msg = psprintf(
                    "PGDAFT_ARROW_SLOT_SCHEMA_MISMATCH field=%u", i);
            pfree(views);
            return PG_DAFT_ARROW_APPEND_ERROR;
        }
        if (slot->tts_isnull[i])
            continue;

        const Datum datum = slot->tts_values[i];
        if (IsVariableWidth(column.pg_type_oid))
        {
            struct varlena *value =
                reinterpret_cast<struct varlena *>(
                    PG_DETOAST_DATUM_PACKED(datum));
            views[i].data = VARDATA_ANY(value);
            views[i].length = static_cast<size_t>(VARSIZE_ANY_EXHDR(value));
            if (views[i].length > static_cast<size_t>(INT32_MAX) -
                                      column.payload_bytes)
            {
                SetError(error_msg, "PGDAFT_ARROW_VARLENA_OFFSET_OVERFLOW");
                pfree(views);
                return PG_DAFT_ARROW_APPEND_ERROR;
            }
        }
        else if (column.pg_type_oid == DATEOID)
        {
            const DateADT value = DatumGetDateADT(datum);
            if (DATE_NOT_FINITE(value) || !IS_VALID_DATE(value) ||
                static_cast<int64>(value) +
                        PG_DAFT_ARROW_DATE_EPOCH_OFFSET_DAYS >
                    std::numeric_limits<int32>::max() ||
                static_cast<int64>(value) +
                        PG_DAFT_ARROW_DATE_EPOCH_OFFSET_DAYS <
                    std::numeric_limits<int32>::min())
            {
                SetError(error_msg, "PGDAFT_ARROW_DATE_OUT_OF_RANGE");
                pfree(views);
                return PG_DAFT_ARROW_APPEND_ERROR;
            }
        }
        else if (column.pg_type_oid == TIMESTAMPOID ||
                 column.pg_type_oid == TIMESTAMPTZOID)
        {
            const int64 value = DatumGetTimestamp(datum);
            int64 converted;
            if (TIMESTAMP_NOT_FINITE(value) || !IS_VALID_TIMESTAMP(value) ||
                pg_add_s64_overflow(value,
                                    PG_DAFT_ARROW_TIMESTAMP_EPOCH_OFFSET_US,
                                    &converted))
            {
                SetError(error_msg, "PGDAFT_ARROW_TIMESTAMP_OUT_OF_RANGE");
                pfree(views);
                return PG_DAFT_ARROW_APPEND_ERROR;
            }
        }
    }

    const uint32 required_rows = builder->nrows + 1U;
    for (uint32 i = 0; i < builder->ncolumns; ++i)
    {
        PgDaftArrowInputColumnV1 &column = builder->columns[i];
        if (!EnsureRowCapacity(builder, &column, required_rows, error_msg) ||
            (IsVariableWidth(column.pg_type_oid) &&
             !EnsurePayloadCapacity(&column,
                                    column.payload_bytes + views[i].length,
                                    error_msg)))
        {
            pfree(views);
            return PG_DAFT_ARROW_APPEND_ERROR;
        }
    }

    const uint32 row = builder->nrows;
    for (uint32 i = 0; i < builder->ncolumns; ++i)
    {
        PgDaftArrowInputColumnV1 &column = builder->columns[i];
        if (slot->tts_isnull[i])
        {
            ++column.null_count;
            if (IsVariableWidth(column.pg_type_oid))
                column.offsets[row + 1U] =
                    static_cast<int32>(column.payload_bytes);
            continue;
        }

        column.validity[row / 8U] |= static_cast<uint8>(1U << (row % 8U));
        const Datum datum = slot->tts_values[i];
        switch (column.pg_type_oid)
        {
            case BOOLOID:
                if (DatumGetBool(datum))
                    static_cast<uint8 *>(column.data)[row / 8U] |=
                        static_cast<uint8>(1U << (row % 8U));
                break;
            case INT2OID:
                static_cast<int16 *>(column.data)[row] = DatumGetInt16(datum);
                break;
            case INT4OID:
                static_cast<int32 *>(column.data)[row] = DatumGetInt32(datum);
                break;
            case INT8OID:
                static_cast<int64 *>(column.data)[row] = DatumGetInt64(datum);
                break;
            case DATEOID:
                static_cast<int32 *>(column.data)[row] =
                    pg_date_to_arrow_date32(DatumGetDateADT(datum));
                break;
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
                static_cast<int64 *>(column.data)[row] =
                    pg_timestamp_to_arrow_timestamp_us(
                        DatumGetTimestamp(datum));
                break;
            case TEXTOID:
            case VARCHAROID:
            case BYTEAOID:
                if (views[i].length > 0)
                    std::memcpy(static_cast<char *>(column.data) +
                                    column.payload_bytes,
                                views[i].data, views[i].length);
                column.payload_bytes += views[i].length;
                column.offsets[row + 1U] =
                    static_cast<int32>(column.payload_bytes);
                break;
            default:
                pg_unreachable();
        }
    }
    ++builder->nrows;
    builder->bytes_used = CalculateUsedBytes(builder);
    pfree(views);
    return PG_DAFT_ARROW_APPEND_OK;
}

extern "C" bool
pg_daft_arrow_input_builder_should_flush_v1(
    const PgDaftArrowInputBuilderV1 *builder)
{
    return builder != nullptr && builder->nrows > 0 &&
           (builder->nrows >= builder->target_rows ||
            builder->bytes_used >= builder->target_bytes);
}

extern "C" uint32
pg_daft_arrow_input_builder_row_count_v1(
    const PgDaftArrowInputBuilderV1 *builder)
{
    return builder == nullptr ? 0 : builder->nrows;
}

extern "C" uint64
pg_daft_arrow_input_builder_bytes_v1(
    const PgDaftArrowInputBuilderV1 *builder)
{
    return builder == nullptr ? 0 : builder->bytes_used;
}

extern "C" bool
pg_daft_arrow_input_builder_finish_v1(PgDaftArrowInputBuilderV1 *builder,
                                      ArrowArray *array, ArrowSchema *schema,
                                      uint32 *out_rows, char **error_msg)
{
    if (error_msg != nullptr)
        *error_msg = nullptr;
    if (builder == nullptr || array == nullptr || schema == nullptr ||
        out_rows == nullptr)
    {
        SetError(error_msg, "PGDAFT_ARROW_INVALID_FINISH_ARGUMENT");
        return false;
    }
    if (array->release != nullptr || schema->release != nullptr)
    {
        SetError(error_msg, "PGDAFT_ARROW_OUTPUT_NOT_EMPTY");
        return false;
    }
    std::memset(array, 0, sizeof(*array));
    std::memset(schema, 0, sizeof(*schema));
    *out_rows = 0;
    if (builder->nrows == 0)
        return true;

    ArrowSchema built_schema{};
    if (!BuildSchemaTree(builder, &built_schema, error_msg))
        return false;
    ArrowArray built_array{};
    if (!BuildArrayTree(builder, &built_array, error_msg))
    {
        built_schema.release(&built_schema);
        return false;
    }

    *out_rows = builder->nrows;
    TransferArrayBuffers(builder, &built_array);
    *array = built_array;
    *schema = built_schema;
    return true;
}

extern "C" bool
dpg_arrow_validate_metadata(const ArrowSchema *schema, const ArrowArray *array,
                            TupleDesc tupdesc, char **error_msg)
{
    if (error_msg != nullptr)
        *error_msg = nullptr;
    if (schema == nullptr || array == nullptr || tupdesc == nullptr)
    {
        SetError(error_msg, "PGDAFT_ARROW_INVALID_IMPORT_ARGUMENT");
        return false;
    }
    if (schema->release == nullptr || array->release == nullptr)
    {
        SetError(error_msg, "PGDAFT_ARROW_RELEASE_MISSING");
        return false;
    }
    if (schema->format == nullptr || std::strcmp(schema->format, "+s") != 0)
    {
        SetError(error_msg, "PGDAFT_ARROW_TOPLEVEL_NOT_STRUCT");
        return false;
    }
    if (schema->flags != 0 || schema->dictionary != nullptr ||
        array->dictionary != nullptr)
    {
        SetError(error_msg, "PGDAFT_ARROW_TOPLEVEL_METADATA_INVALID");
        return false;
    }
    if (array->length < 0 || array->length > kMaximumBatchRows ||
        array->offset != 0 || array->null_count != 0)
    {
        SetError(error_msg, "PGDAFT_ARROW_BATCH_LENGTH_INVALID");
        return false;
    }
    if (schema->n_children != tupdesc->natts ||
        array->n_children != tupdesc->natts ||
        schema->n_children < 0)
    {
        SetError(error_msg, "PGDAFT_ARROW_COLUMN_COUNT_MISMATCH");
        return false;
    }
    if ((tupdesc->natts > 0 &&
         (schema->children == nullptr || array->children == nullptr)) ||
        array->n_buffers != 1 || array->buffers == nullptr ||
        array->buffers[0] != nullptr)
    {
        SetError(error_msg, "PGDAFT_ARROW_TOPLEVEL_LAYOUT_INVALID");
        return false;
    }
    if (!ValidateFingerprintMetadata(schema, tupdesc, error_msg))
        return false;

    for (int i = 0; i < tupdesc->natts; ++i)
    {
        Form_pg_attribute attribute = TupleDescAttr(tupdesc, i);
        if (attribute->attisdropped)
        {
            if (error_msg != nullptr)
                *error_msg = psprintf(
                    "PGDAFT_ARROW_DROPPED_ATTRIBUTE field=%d", i);
            return false;
        }
        if ((attribute->atttypid == TEXTOID ||
             attribute->atttypid == VARCHAROID) &&
            GetDatabaseEncoding() != PG_UTF8)
        {
            SetError(error_msg, "PGDAFT_ARROW_DATABASE_ENCODING_NOT_UTF8");
            return false;
        }
        if (!ValidateChild(schema->children[i], array->children[i], attribute,
                           i, array->length, error_msg))
            return false;
    }
    return true;
}

extern "C" DaftArrowBatchContext *
dpg_arrow_create_batch(ArrowArray *array, ArrowSchema *schema,
                       TupleDesc tupdesc, MemoryContext batch_mcxt,
                       char **error_msg)
{
    if (!dpg_arrow_validate_metadata(schema, array, tupdesc, error_msg))
        return nullptr;
    if (batch_mcxt == nullptr)
        batch_mcxt = CurrentMemoryContext;

    MemoryContext output_batch_mcxt = AllocSetContextCreate(
        batch_mcxt, "PG Daft Arrow output batch", ALLOCSET_DEFAULT_SIZES);
    MemoryContext old_mcxt = MemoryContextSwitchTo(output_batch_mcxt);
    DaftArrowBatchContext *batch =
        static_cast<DaftArrowBatchContext *>(palloc0(sizeof(*batch)));
    batch->output_batch_mcxt = output_batch_mcxt;
    batch->tupdesc = tupdesc;
    batch->nrows = array->length;
    batch->ncolumns = tupdesc->natts;
    /*
     * Metadata/layout validation above proves offset=0, bounded length,
     * canonical buffer counts, monotonic int32 offsets, and the P0 type
     * whitelist.  Count logical boundary bytes here, before transferring the
     * Arrow C Data ownership into the opaque batch context.
     */
    for (int i = 0; i < tupdesc->natts; ++i)
    {
        const ArrowArray *column = array->children[i];
        Form_pg_attribute attribute = TupleDescAttr(tupdesc, i);
        const uint64 bitmap_bytes =
            (static_cast<uint64>(array->length) + 7U) / 8U;
        if (column->buffers[0] != nullptr)
            batch->logical_bytes += bitmap_bytes;
        switch (attribute->atttypid)
        {
            case BOOLOID:
                batch->logical_bytes += bitmap_bytes;
                break;
            case INT2OID:
                batch->logical_bytes +=
                    static_cast<uint64>(array->length) * sizeof(int16);
                break;
            case INT4OID:
            case DATEOID:
                batch->logical_bytes +=
                    static_cast<uint64>(array->length) * sizeof(int32);
                break;
            case INT8OID:
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
                batch->logical_bytes +=
                    static_cast<uint64>(array->length) * sizeof(int64);
                break;
            case TEXTOID:
            case VARCHAROID:
            case BYTEAOID:
            {
                const int32 *offsets =
                    static_cast<const int32 *>(column->buffers[1]);
                batch->logical_bytes +=
                    (static_cast<uint64>(array->length) + 1U) * sizeof(int32);
                batch->logical_bytes +=
                    static_cast<uint64>(offsets[array->length]);
                break;
            }
            default:
                pg_unreachable();
        }
    }
    batch->array = *array;
    batch->schema = *schema;
    std::memset(array, 0, sizeof(*array));
    std::memset(schema, 0, sizeof(*schema));
    MemoryContextSwitchTo(old_mcxt);
    return batch;
}

extern "C" void
dpg_arrow_materialize_row(DaftArrowBatchContext *batch, int row_index,
                          TupleTableSlot *slot)
{
    if (batch == nullptr || slot == nullptr ||
        row_index < 0 || row_index >= batch->nrows)
        ereport(ERROR, (errmsg("PGDAFT_ARROW_ROW_INDEX_INVALID")));
    if (!TTS_IS_VIRTUAL(slot) || slot->tts_tupleDescriptor == nullptr ||
        slot->tts_tupleDescriptor->natts != batch->ncolumns)
        ereport(ERROR, (errmsg("PGDAFT_ARROW_OUTPUT_SLOT_INVALID")));

    for (int i = 0; i < batch->ncolumns; ++i)
    {
        Form_pg_attribute expected = TupleDescAttr(batch->tupdesc, i);
        Form_pg_attribute actual = TupleDescAttr(slot->tts_tupleDescriptor, i);
        if (actual->atttypid != expected->atttypid ||
            actual->atttypmod != expected->atttypmod ||
            actual->attcollation != expected->attcollation ||
            actual->attnotnull != expected->attnotnull)
            ereport(ERROR,
                    (errmsg("PGDAFT_ARROW_OUTPUT_SLOT_SCHEMA_MISMATCH field=%d",
                            i)));
    }

    ExecClearTuple(slot);
    for (int i = 0; i < batch->ncolumns; ++i)
    {
        Form_pg_attribute attribute = TupleDescAttr(batch->tupdesc, i);
        const ArrowArray *column = batch->array.children[i];
        if (IsNullAt(column, row_index))
        {
            slot->tts_isnull[i] = true;
            slot->tts_values[i] = static_cast<Datum>(0);
            continue;
        }

        slot->tts_isnull[i] = false;
        switch (attribute->atttypid)
        {
            case BOOLOID:
            {
                const uint8 *values =
                    static_cast<const uint8 *>(column->buffers[1]);
                const bool value =
                    (values[row_index / 8] &
                     static_cast<uint8>(1U << (row_index % 8))) != 0;
                slot->tts_values[i] = BoolGetDatum(value);
                break;
            }
            case INT2OID:
                slot->tts_values[i] = Int16GetDatum(
                    static_cast<const int16 *>(column->buffers[1])[row_index]);
                break;
            case INT4OID:
                slot->tts_values[i] = Int32GetDatum(
                    static_cast<const int32 *>(column->buffers[1])[row_index]);
                break;
            case INT8OID:
                slot->tts_values[i] = Int64GetDatum(
                    static_cast<const int64 *>(column->buffers[1])[row_index]);
                break;
            case DATEOID:
                slot->tts_values[i] = DateADTGetDatum(
                    arrow_date32_to_pg_date(
                        static_cast<const int32 *>(column->buffers[1])[row_index]));
                break;
            case TIMESTAMPOID:
                slot->tts_values[i] = TimestampGetDatum(
                    arrow_timestamp_us_to_pg_timestamp(
                        static_cast<const int64 *>(column->buffers[1])[row_index]));
                break;
            case TIMESTAMPTZOID:
                slot->tts_values[i] = TimestampTzGetDatum(
                    arrow_timestamp_us_to_pg_timestamp(
                        static_cast<const int64 *>(column->buffers[1])[row_index]));
                break;
            case TEXTOID:
            case VARCHAROID:
            case BYTEAOID:
            {
                const int32 *offsets =
                    static_cast<const int32 *>(column->buffers[1]);
                const int32 start = offsets[row_index];
                const int32 length = offsets[row_index + 1] - start;
                const char *source = length == 0
                                         ? nullptr
                                         : static_cast<const char *>(
                                               column->buffers[2]) + start;
                MemoryContext old_mcxt =
                    MemoryContextSwitchTo(batch->output_batch_mcxt);
                struct varlena *value =
                    static_cast<struct varlena *>(palloc(VARHDRSZ + length));
                SET_VARSIZE(value, VARHDRSZ + length);
                if (length > 0)
                    std::memcpy(VARDATA(value), source, length);
                MemoryContextSwitchTo(old_mcxt);
                slot->tts_values[i] = PointerGetDatum(value);
                break;
            }
            default:
                ereport(ERROR,
                        (errmsg("PGDAFT_ARROW_UNSUPPORTED_TYPE_OID oid=%u",
                                attribute->atttypid)));
        }
    }
    ExecStoreVirtualTuple(slot);
}

extern "C" int64
dpg_arrow_batch_row_count(DaftArrowBatchContext *batch)
{
    return batch == nullptr ? 0 : batch->nrows;
}

extern "C" uint64
dpg_arrow_batch_logical_bytes(DaftArrowBatchContext *batch)
{
    return batch == nullptr ? 0 : batch->logical_bytes;
}

extern "C" void
dpg_arrow_release_batch(DaftArrowBatchContext *batch)
{
    if (batch == nullptr)
        return;
    MemoryContext output_batch_mcxt = batch->output_batch_mcxt;
    if (batch->array.release != nullptr)
        batch->array.release(&batch->array);
    if (batch->schema.release != nullptr)
        batch->schema.release(&batch->schema);
    MemoryContextDelete(output_batch_mcxt);
}

/*
 * A narrow SQL-callable debug probe used by the daft_arrow regression.  It
 * crosses a per-tuple MemoryContext reset and invokes the exported Arrow
 * release callbacks on a non-PG pthread.  It is intentionally not installed
 * as an extension SQL object; the regression creates it in its temporary DB.
 */
extern "C" {
PG_FUNCTION_INFO_V1(pg_daft_arrow_input_selftest_v1);
PG_FUNCTION_INFO_V1(pg_daft_arrow_boundary_selftest_v1);

Datum
pg_daft_arrow_input_selftest_v1(PG_FUNCTION_ARGS)
{
    TupleDesc tupdesc = CreateTemplateTupleDesc(PG_DAFT_ARROW_P0_TYPE_COUNT);
    const Oid oids[PG_DAFT_ARROW_P0_TYPE_COUNT] = {
        BOOLOID, BYTEAOID, INT8OID, INT2OID, INT4OID, TEXTOID, VARCHAROID,
        DATEOID, TIMESTAMPOID, TIMESTAMPTZOID};
    const char *names[PG_DAFT_ARROW_P0_TYPE_COUNT] = {
        "b", "bin", "i8", "i2", "i4", "txt", "vc", "d", "ts", "tstz"};
    for (uint32 i = 0; i < PG_DAFT_ARROW_P0_TYPE_COUNT; ++i)
        TupleDescInitEntry(tupdesc, static_cast<AttrNumber>(i + 1), names[i],
                           oids[i], -1, 0);
    TupleDescInitEntryCollation(tupdesc, 6, C_COLLATION_OID);
    TupleDescInitEntryCollation(tupdesc, 7, C_COLLATION_OID);
    TupleDescAttr(tupdesc, 0)->attnotnull = true;
    TupleDescFinalize(tupdesc);
    BlessTupleDesc(tupdesc);

    char *error_msg = nullptr;
    PgDaftArrowInputBuilderV1 *builder =
        pg_daft_arrow_input_builder_create_v1(tupdesc, 2, 4096,
                                              CurrentMemoryContext, &error_msg);
    if (builder == nullptr)
        ereport(ERROR, (errmsg("Arrow input selftest create failed: %s",
                               error_msg == nullptr ? "unknown" : error_msg)));

    TupleTableSlot *slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsVirtual);
    MemoryContext row_mcxt = AllocSetContextCreate(
        CurrentMemoryContext, "PG Daft Arrow input selftest row",
        ALLOCSET_DEFAULT_SIZES);
    MemoryContext old_mcxt = MemoryContextSwitchTo(row_mcxt);
    text *txt = cstring_to_text("alpha");
    text *vc = cstring_to_text("v");
    bytea *bin = static_cast<bytea *>(palloc(VARHDRSZ + 3));
    SET_VARSIZE(bin, VARHDRSZ + 3);
    std::memcpy(VARDATA(bin), "xyz", 3);
    MemoryContextSwitchTo(old_mcxt);

    slot->tts_values[0] = BoolGetDatum(true);
    slot->tts_values[1] = PointerGetDatum(bin);
    slot->tts_values[2] = Int64GetDatum(INT64_C(9223372036854775807));
    slot->tts_values[3] = Int16GetDatum(std::numeric_limits<int16>::min());
    slot->tts_values[4] = Int32GetDatum(std::numeric_limits<int32>::max());
    slot->tts_values[5] = PointerGetDatum(txt);
    slot->tts_values[6] = PointerGetDatum(vc);
    slot->tts_values[7] = DateADTGetDatum(0);
    slot->tts_values[8] = TimestampGetDatum(0);
    slot->tts_values[9] = TimestampTzGetDatum(0);
    std::memset(slot->tts_isnull, false,
                sizeof(bool) * PG_DAFT_ARROW_P0_TYPE_COUNT);
    ExecStoreVirtualTuple(slot);
    if (pg_daft_arrow_input_builder_append_v1(builder, slot, &error_msg) !=
        PG_DAFT_ARROW_APPEND_OK)
        ereport(ERROR, (errmsg("Arrow input selftest first append failed: %s",
                               error_msg == nullptr ? "unknown" : error_msg)));

    ExecClearTuple(slot);
    MemoryContextReset(row_mcxt);
    std::memset(slot->tts_values, 0,
                sizeof(Datum) * PG_DAFT_ARROW_P0_TYPE_COUNT);
    std::memset(slot->tts_isnull, true,
                sizeof(bool) * PG_DAFT_ARROW_P0_TYPE_COUNT);
    slot->tts_values[0] = BoolGetDatum(false);
    slot->tts_isnull[0] = false;
    ExecStoreVirtualTuple(slot);
    if (pg_daft_arrow_input_builder_append_v1(builder, slot, &error_msg) !=
        PG_DAFT_ARROW_APPEND_OK)
        ereport(ERROR, (errmsg("Arrow input selftest second append failed: %s",
                               error_msg == nullptr ? "unknown" : error_msg)));
    if (!pg_daft_arrow_input_builder_should_flush_v1(builder) ||
        pg_daft_arrow_input_builder_row_count_v1(builder) != 2 ||
        pg_daft_arrow_input_builder_append_v1(builder, slot, &error_msg) !=
            PG_DAFT_ARROW_APPEND_FULL)
        ereport(ERROR, (errmsg("Arrow input selftest batch boundary failed")));

    ArrowArray array{};
    ArrowSchema schema{};
    uint32 rows = 0;
    if (!pg_daft_arrow_input_builder_finish_v1(builder, &array, &schema,
                                               &rows, &error_msg))
        ereport(ERROR, (errmsg("Arrow input selftest finish failed: %s",
                               error_msg == nullptr ? "unknown" : error_msg)));

    const ArrowArray *binary_array = array.children[1];
    const ArrowArray *text_array = array.children[5];
    const int32 *binary_offsets =
        static_cast<const int32 *>(binary_array->buffers[1]);
    const int32 *text_offsets =
        static_cast<const int32 *>(text_array->buffers[1]);
    const bool layout_ok =
        rows == 2 && array.length == 2 && array.n_children == 10 &&
        schema.n_children == 10 &&
        text_array->null_count == 1 && text_array->buffers[0] != nullptr &&
        (static_cast<const uint8 *>(text_array->buffers[0])[0] & 0x03U) == 0x01U &&
        text_offsets[0] == 0 && text_offsets[1] == 5 && text_offsets[2] == 5 &&
        std::memcmp(text_array->buffers[2], "alpha", 5) == 0 &&
        binary_offsets[0] == 0 && binary_offsets[1] == 3 &&
        binary_offsets[2] == 3 &&
        std::memcmp(binary_array->buffers[2], "xyz", 3) == 0 &&
        static_cast<const int32 *>(array.children[7]->buffers[1])[0] == 10957 &&
        static_cast<const int64 *>(array.children[8]->buffers[1])[0] ==
            INT64_C(946684800000000) &&
        std::strcmp(schema.children[8]->format, "tsu:") == 0 &&
        std::strcmp(schema.children[9]->format, "tsu:UTC") == 0 &&
        schema.metadata != nullptr;
    if (!layout_ok)
        ereport(ERROR, (errmsg("Arrow input selftest exported invalid layout")));

    ReleaseOnWorkerArgs worker_args{&array, &schema};
    pthread_t worker;
    if (pthread_create(&worker, nullptr, ReleaseOnWorker, &worker_args) != 0 ||
        pthread_join(worker, nullptr) != 0)
        ereport(ERROR, (errmsg("Arrow input selftest worker release failed")));
    if (array.release != nullptr || schema.release != nullptr)
        ereport(ERROR, (errmsg("Arrow input selftest release was not final")));

    /* Build a fresh one-row batch for Arrow -> PostgreSQL validation. */
    ExecClearTuple(slot);
    old_mcxt = MemoryContextSwitchTo(row_mcxt);
    txt = cstring_to_text("roundtrip");
    vc = cstring_to_text("");
    bin = static_cast<bytea *>(palloc(VARHDRSZ));
    SET_VARSIZE(bin, VARHDRSZ);
    MemoryContextSwitchTo(old_mcxt);
    slot->tts_values[0] = BoolGetDatum(true);
    slot->tts_values[1] = PointerGetDatum(bin);
    slot->tts_values[2] = Int64GetDatum(-7);
    slot->tts_values[3] = Int16GetDatum(8);
    slot->tts_values[4] = Int32GetDatum(9);
    slot->tts_values[5] = PointerGetDatum(txt);
    slot->tts_values[6] = PointerGetDatum(vc);
    slot->tts_values[7] = DateADTGetDatum(0);
    slot->tts_values[8] = TimestampGetDatum(0);
    slot->tts_values[9] = TimestampTzGetDatum(0);
    std::memset(slot->tts_isnull, false,
                sizeof(bool) * PG_DAFT_ARROW_P0_TYPE_COUNT);
    ExecStoreVirtualTuple(slot);
    if (pg_daft_arrow_input_builder_append_v1(builder, slot, &error_msg) !=
        PG_DAFT_ARROW_APPEND_OK)
        ereport(ERROR, (errmsg("Arrow import selftest append failed: %s",
                               error_msg == nullptr ? "unknown" : error_msg)));
    ExecClearTuple(slot);
    MemoryContextReset(row_mcxt);

    if (!pg_daft_arrow_input_builder_finish_v1(builder, &array, &schema,
                                               &rows, &error_msg))
        ereport(ERROR, (errmsg("Arrow import selftest finish failed: %s",
                               error_msg == nullptr ? "unknown" : error_msg)));

    /* Each rejected mutation must leave producer ownership unchanged. */
    const int64 saved_length = array.length;
    array.length = -1;
    if (dpg_arrow_validate_metadata(&schema, &array, tupdesc, &error_msg) ||
        array.release == nullptr || schema.release == nullptr ||
        error_msg == nullptr ||
        std::strcmp(error_msg, "PGDAFT_ARROW_BATCH_LENGTH_INVALID") != 0)
        ereport(ERROR, (errmsg("Arrow import selftest accepted negative length")));
    array.length = saved_length;
    if (error_msg != nullptr)
    {
        pfree(error_msg);
        error_msg = nullptr;
    }

    char *format = const_cast<char *>(schema.children[4]->format);
    const char saved_format = format[0];
    format[0] = 'x';
    if (dpg_arrow_validate_metadata(&schema, &array, tupdesc, &error_msg) ||
        array.release == nullptr || schema.release == nullptr ||
        error_msg == nullptr ||
        std::strcmp(error_msg,
                    "PGDAFT_ARROW_FORMAT_MISMATCH oid=23 expected=i") != 0)
        ereport(ERROR, (errmsg("Arrow import selftest accepted unknown format")));
    format[0] = saved_format;
    if (error_msg != nullptr)
    {
        pfree(error_msg);
        error_msg = nullptr;
    }

    int32 *mutable_offsets = const_cast<int32 *>(
        static_cast<const int32 *>(array.children[5]->buffers[1]));
    const int32 saved_offset = mutable_offsets[1];
    mutable_offsets[1] = -1;
    if (dpg_arrow_validate_metadata(&schema, &array, tupdesc, &error_msg) ||
        array.release == nullptr || schema.release == nullptr ||
        error_msg == nullptr ||
        std::strcmp(error_msg,
                    "PGDAFT_ARROW_OFFSETS_NOT_MONOTONIC field=5 row=0") != 0)
        ereport(ERROR, (errmsg("Arrow import selftest accepted invalid offsets")));
    mutable_offsets[1] = saved_offset;
    if (error_msg != nullptr)
    {
        pfree(error_msg);
        error_msg = nullptr;
    }

    char *metadata = const_cast<char *>(schema.metadata);
    int32 metadata_key_length;
    std::memcpy(&metadata_key_length, metadata + sizeof(int32), sizeof(int32));
    char *fingerprint_value = metadata + sizeof(int32) * 3 +
                              metadata_key_length;
    const char saved_fingerprint = fingerprint_value[0];
    fingerprint_value[0] = saved_fingerprint == '0' ? '1' : '0';
    if (dpg_arrow_validate_metadata(&schema, &array, tupdesc, &error_msg) ||
        array.release == nullptr || schema.release == nullptr ||
        error_msg == nullptr ||
        std::strcmp(error_msg,
                    "PGDAFT_ARROW_SCHEMA_FINGERPRINT_MISMATCH") != 0)
        ereport(ERROR,
                (errmsg("Arrow import selftest accepted fingerprint mismatch")));
    fingerprint_value[0] = saved_fingerprint;
    if (error_msg != nullptr)
    {
        pfree(error_msg);
        error_msg = nullptr;
    }

    DaftArrowBatchContext *output_batch = dpg_arrow_create_batch(
        &array, &schema, tupdesc, CurrentMemoryContext, &error_msg);
    if (output_batch == nullptr || array.release != nullptr ||
        schema.release != nullptr || dpg_arrow_batch_row_count(output_batch) != 1)
        ereport(ERROR, (errmsg("Arrow import selftest ownership failed: %s",
                               error_msg == nullptr ? "unknown" : error_msg)));

    dpg_arrow_materialize_row(output_batch, 0, slot);
    char *source_text = const_cast<char *>(static_cast<const char *>(
        output_batch->array.children[5]->buffers[2]));
    source_text[0] = 'X';
    text *materialized_text = DatumGetTextPP(slot->tts_values[5]);
    bytea *materialized_binary = DatumGetByteaPP(slot->tts_values[1]);
    const bool import_ok =
        !slot->tts_isnull[5] && VARSIZE_ANY_EXHDR(materialized_text) == 9 &&
        std::memcmp(VARDATA_ANY(materialized_text), "roundtrip", 9) == 0 &&
        !slot->tts_isnull[1] && VARSIZE_ANY_EXHDR(materialized_binary) == 0 &&
        DatumGetDateADT(slot->tts_values[7]) == 0 &&
        DatumGetTimestamp(slot->tts_values[8]) == 0 &&
        DatumGetTimestampTz(slot->tts_values[9]) == 0;
    if (!import_ok)
        ereport(ERROR, (errmsg("Arrow import selftest materialization failed")));
    ExecClearTuple(slot);
    dpg_arrow_release_batch(output_batch);

    ExecDropSingleTupleTableSlot(slot);
    MemoryContextDelete(row_mcxt);
    pg_daft_arrow_input_builder_destroy_v1(builder);
    PG_RETURN_TEXT_P(cstring_to_text(
        "rows=2 copied_after_reset=yes null_bitmap=ok offsets=0,5,5 worker_release=libc_only import_validation=ok stable_errors=ok epoch_reverse=ok datum_lifetime=batch"));
}

Datum
pg_daft_arrow_boundary_selftest_v1(PG_FUNCTION_ARGS)
{
    TupleDesc tupdesc = CreateTemplateTupleDesc(PG_DAFT_ARROW_P0_TYPE_COUNT);
    const Oid oids[PG_DAFT_ARROW_P0_TYPE_COUNT] = {
        BOOLOID, BYTEAOID, INT8OID, INT2OID, INT4OID, TEXTOID, VARCHAROID,
        DATEOID, TIMESTAMPOID, TIMESTAMPTZOID};
    const char *names[PG_DAFT_ARROW_P0_TYPE_COUNT] = {
        "b", "bin", "i8", "i2", "i4", "txt", "vc", "d", "ts", "tstz"};
    for (uint32 i = 0; i < PG_DAFT_ARROW_P0_TYPE_COUNT; ++i)
        TupleDescInitEntry(tupdesc, static_cast<AttrNumber>(i + 1), names[i],
                           oids[i], -1, 0);
    TupleDescInitEntryCollation(tupdesc, 6, C_COLLATION_OID);
    TupleDescInitEntryCollation(tupdesc, 7, C_COLLATION_OID);
    TupleDescFinalize(tupdesc);
    BlessTupleDesc(tupdesc);

    char *error_msg = nullptr;
    PgDaftArrowInputBuilderV1 *builder =
        pg_daft_arrow_input_builder_create_v1(
            tupdesc, 4, UINT64_C(8) * 1024U * 1024U,
            CurrentMemoryContext, &error_msg);
    if (builder == nullptr)
        ereport(ERROR, (errmsg("Arrow boundary selftest create failed: %s",
                               error_msg == nullptr ? "unknown" : error_msg)));

    TupleTableSlot *slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsVirtual);
    MemoryContext row_mcxt = AllocSetContextCreate(
        CurrentMemoryContext, "PG Daft Arrow boundary selftest row",
        ALLOCSET_DEFAULT_SIZES);
    const DateADT min_date = DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE;
    const DateADT max_date = DATE_END_JULIAN - POSTGRES_EPOCH_JDATE - 1;
    const int64 max_arrow_compatible_timestamp =
        PG_INT64_MAX - PG_DAFT_ARROW_TIMESTAMP_EPOCH_OFFSET_US;
    constexpr size_t kLargeBytes = 1024U * 1024U;

    for (int row = 0; row < 4; ++row)
    {
        ExecClearTuple(slot);
        std::memset(slot->tts_values, 0,
                    sizeof(Datum) * PG_DAFT_ARROW_P0_TYPE_COUNT);
        std::memset(slot->tts_isnull, false,
                    sizeof(bool) * PG_DAFT_ARROW_P0_TYPE_COUNT);
        MemoryContext old_mcxt = MemoryContextSwitchTo(row_mcxt);

        if (row == 0)
        {
            bytea *binary = static_cast<bytea *>(palloc(VARHDRSZ + 1));
            SET_VARSIZE(binary, VARHDRSZ + 1);
            VARDATA(binary)[0] = '\0';
            slot->tts_values[0] = BoolGetDatum(false);
            slot->tts_values[1] = PointerGetDatum(binary);
            slot->tts_values[2] = Int64GetDatum(PG_INT64_MIN);
            slot->tts_values[3] = Int16GetDatum(PG_INT16_MIN);
            slot->tts_values[4] = Int32GetDatum(PG_INT32_MIN);
            slot->tts_values[5] = PointerGetDatum(cstring_to_text("min"));
            slot->tts_values[6] = PointerGetDatum(cstring_to_text("vmin"));
            slot->tts_values[7] = DateADTGetDatum(min_date);
            slot->tts_values[8] = TimestampGetDatum(MIN_TIMESTAMP);
            slot->tts_values[9] = TimestampTzGetDatum(MIN_TIMESTAMP);
        }
        else if (row == 1)
        {
            bytea *binary = static_cast<bytea *>(palloc(VARHDRSZ));
            SET_VARSIZE(binary, VARHDRSZ);
            slot->tts_values[0] = BoolGetDatum(true);
            slot->tts_values[1] = PointerGetDatum(binary);
            slot->tts_values[2] = Int64GetDatum(PG_INT64_MAX);
            slot->tts_values[3] = Int16GetDatum(PG_INT16_MAX);
            slot->tts_values[4] = Int32GetDatum(PG_INT32_MAX);
            slot->tts_values[5] = PointerGetDatum(cstring_to_text(""));
            slot->tts_values[6] = PointerGetDatum(cstring_to_text(""));
            slot->tts_values[7] = DateADTGetDatum(max_date);
            slot->tts_values[8] =
                TimestampGetDatum(max_arrow_compatible_timestamp);
            slot->tts_values[9] =
                TimestampTzGetDatum(max_arrow_compatible_timestamp);
        }
        else if (row == 2)
        {
            std::memset(slot->tts_isnull, true,
                        sizeof(bool) * PG_DAFT_ARROW_P0_TYPE_COUNT);
        }
        else
        {
            text *large_text =
                static_cast<text *>(palloc(VARHDRSZ + kLargeBytes));
            SET_VARSIZE(large_text, VARHDRSZ + kLargeBytes);
            std::memset(VARDATA(large_text), 'q', kLargeBytes);
            bytea *large_binary =
                static_cast<bytea *>(palloc(VARHDRSZ + kLargeBytes));
            SET_VARSIZE(large_binary, VARHDRSZ + kLargeBytes);
            std::memset(VARDATA(large_binary), 0xa5, kLargeBytes);
            slot->tts_values[0] = BoolGetDatum(true);
            slot->tts_values[1] = PointerGetDatum(large_binary);
            slot->tts_values[2] = Int64GetDatum(0);
            slot->tts_values[3] = Int16GetDatum(0);
            slot->tts_values[4] = Int32GetDatum(0);
            slot->tts_values[5] = PointerGetDatum(large_text);
            slot->tts_values[6] = PointerGetDatum(cstring_to_text("large"));
            slot->tts_values[7] = DateADTGetDatum(0);
            slot->tts_values[8] = TimestampGetDatum(0);
            slot->tts_values[9] = TimestampTzGetDatum(0);
        }
        MemoryContextSwitchTo(old_mcxt);
        ExecStoreVirtualTuple(slot);
        if (pg_daft_arrow_input_builder_append_v1(builder, slot, &error_msg) !=
            PG_DAFT_ARROW_APPEND_OK)
            ereport(ERROR,
                    (errmsg("Arrow boundary selftest append row %d failed: %s",
                            row,
                            error_msg == nullptr ? "unknown" : error_msg)));
        ExecClearTuple(slot);
        MemoryContextReset(row_mcxt);
    }

    if (!pg_daft_arrow_input_builder_should_flush_v1(builder) ||
        pg_daft_arrow_input_builder_row_count_v1(builder) != 4 ||
        pg_daft_arrow_input_builder_append_v1(builder, slot, &error_msg) !=
            PG_DAFT_ARROW_APPEND_FULL)
        ereport(ERROR, (errmsg("Arrow boundary selftest row boundary failed")));

    ArrowArray array{};
    ArrowSchema schema{};
    uint32 rows = 0;
    if (!pg_daft_arrow_input_builder_finish_v1(builder, &array, &schema,
                                               &rows, &error_msg))
        ereport(ERROR, (errmsg("Arrow boundary selftest finish failed: %s",
                               error_msg == nullptr ? "unknown" : error_msg)));
    DaftArrowBatchContext *batch = dpg_arrow_create_batch(
        &array, &schema, tupdesc, CurrentMemoryContext, &error_msg);
    if (batch == nullptr || rows != 4)
        ereport(ERROR, (errmsg("Arrow boundary selftest import failed: %s",
                               error_msg == nullptr ? "unknown" : error_msg)));

    dpg_arrow_materialize_row(batch, 0, slot);
    if (DatumGetBool(slot->tts_values[0]) ||
        DatumGetInt64(slot->tts_values[2]) != PG_INT64_MIN ||
        DatumGetInt16(slot->tts_values[3]) != PG_INT16_MIN ||
        DatumGetInt32(slot->tts_values[4]) != PG_INT32_MIN ||
        DatumGetDateADT(slot->tts_values[7]) != min_date ||
        DatumGetTimestamp(slot->tts_values[8]) != MIN_TIMESTAMP ||
        DatumGetTimestampTz(slot->tts_values[9]) != MIN_TIMESTAMP)
        ereport(ERROR, (errmsg("Arrow boundary selftest minimum row failed")));

    dpg_arrow_materialize_row(batch, 1, slot);
    if (!DatumGetBool(slot->tts_values[0]) ||
        DatumGetInt64(slot->tts_values[2]) != PG_INT64_MAX ||
        DatumGetInt16(slot->tts_values[3]) != PG_INT16_MAX ||
        DatumGetInt32(slot->tts_values[4]) != PG_INT32_MAX ||
        VARSIZE_ANY_EXHDR(DatumGetTextPP(slot->tts_values[5])) != 0 ||
        VARSIZE_ANY_EXHDR(DatumGetTextPP(slot->tts_values[6])) != 0 ||
        VARSIZE_ANY_EXHDR(DatumGetByteaPP(slot->tts_values[1])) != 0 ||
        DatumGetDateADT(slot->tts_values[7]) != max_date ||
        DatumGetTimestamp(slot->tts_values[8]) !=
            max_arrow_compatible_timestamp ||
        DatumGetTimestampTz(slot->tts_values[9]) !=
            max_arrow_compatible_timestamp)
        ereport(ERROR, (errmsg("Arrow boundary selftest maximum row failed")));

    dpg_arrow_materialize_row(batch, 2, slot);
    for (uint32 i = 0; i < PG_DAFT_ARROW_P0_TYPE_COUNT; ++i)
    {
        if (!slot->tts_isnull[i])
            ereport(ERROR,
                    (errmsg("Arrow boundary selftest NULL row failed field=%u",
                            i)));
    }

    dpg_arrow_materialize_row(batch, 3, slot);
    text *large_text = DatumGetTextPP(slot->tts_values[5]);
    bytea *large_binary = DatumGetByteaPP(slot->tts_values[1]);
    if (VARSIZE_ANY_EXHDR(large_text) != kLargeBytes ||
        VARDATA_ANY(large_text)[0] != 'q' ||
        VARDATA_ANY(large_text)[kLargeBytes - 1] != 'q' ||
        VARSIZE_ANY_EXHDR(large_binary) != kLargeBytes ||
        static_cast<unsigned char>(VARDATA_ANY(large_binary)[0]) != 0xa5 ||
        static_cast<unsigned char>(VARDATA_ANY(large_binary)[kLargeBytes - 1]) !=
            0xa5)
        ereport(ERROR, (errmsg("Arrow boundary selftest large varlena failed")));

    ExecClearTuple(slot);
    dpg_arrow_release_batch(batch);
    ExecDropSingleTupleTableSlot(slot);
    MemoryContextDelete(row_mcxt);
    pg_daft_arrow_input_builder_destroy_v1(builder);
    PG_RETURN_TEXT_P(cstring_to_text(
        "types=10 minmax=ok nulls=ok empty_varlena=ok large_varlena_bytes=1048576 batch_rows=4 roundtrip=ok"));
}
}  /* extern C */

#else  /* PG_ORCA_ENABLE_DAFT */

struct PgDaftArrowInputBuilderV1
{
    uint8 unavailable;
};

struct DaftArrowBatchContext
{
    uint8 unavailable;
};

extern "C" PgDaftArrowInputBuilderV1 *
pg_daft_arrow_input_builder_create_v1(TupleDesc, uint32, uint64,
                                      MemoryContext, char **error_msg)
{
    SetError(error_msg, "PGDAFT_ARROW_BUILD_DISABLED");
    return nullptr;
}

extern "C" void
pg_daft_arrow_input_builder_destroy_v1(PgDaftArrowInputBuilderV1 *)
{
}

extern "C" PgDaftArrowAppendResultV1
pg_daft_arrow_input_builder_append_v1(PgDaftArrowInputBuilderV1 *,
                                      TupleTableSlot *, char **error_msg)
{
    SetError(error_msg, "PGDAFT_ARROW_BUILD_DISABLED");
    return PG_DAFT_ARROW_APPEND_ERROR;
}

extern "C" bool
pg_daft_arrow_input_builder_should_flush_v1(
    const PgDaftArrowInputBuilderV1 *)
{
    return false;
}

extern "C" uint32
pg_daft_arrow_input_builder_row_count_v1(const PgDaftArrowInputBuilderV1 *)
{
    return 0;
}

extern "C" uint64
pg_daft_arrow_input_builder_bytes_v1(const PgDaftArrowInputBuilderV1 *)
{
    return 0;
}

extern "C" bool
pg_daft_arrow_input_builder_finish_v1(PgDaftArrowInputBuilderV1 *,
                                      ArrowArray *, ArrowSchema *, uint32 *,
                                      char **error_msg)
{
    SetError(error_msg, "PGDAFT_ARROW_BUILD_DISABLED");
    return false;
}

extern "C" bool
dpg_arrow_validate_metadata(const ArrowSchema *, const ArrowArray *, TupleDesc,
                            char **error_msg)
{
    SetError(error_msg, "PGDAFT_ARROW_BUILD_DISABLED");
    return false;
}

extern "C" DaftArrowBatchContext *
dpg_arrow_create_batch(ArrowArray *, ArrowSchema *, TupleDesc, MemoryContext,
                       char **error_msg)
{
    SetError(error_msg, "PGDAFT_ARROW_BUILD_DISABLED");
    return nullptr;
}

extern "C" void
dpg_arrow_materialize_row(DaftArrowBatchContext *, int, TupleTableSlot *)
{
    ereport(ERROR, (errmsg("PGDAFT_ARROW_BUILD_DISABLED")));
}

extern "C" int64
dpg_arrow_batch_row_count(DaftArrowBatchContext *)
{
    return 0;
}

extern "C" uint64
dpg_arrow_batch_logical_bytes(DaftArrowBatchContext *)
{
    return 0;
}

extern "C" void
dpg_arrow_release_batch(DaftArrowBatchContext *)
{
}

#endif /* PG_ORCA_ENABLE_DAFT */
