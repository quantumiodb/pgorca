/*-------------------------------------------------------------------------
 *
 * pg_daft_plan.h
 *    Self-contained Daft plan IR data model + JSON serialization.
 *
 * Pure C++ (no GPOS / ORCA / PostgreSQL headers) so the model and its
 * byte-exact JSON round-trip can be unit-tested standalone (P0-008-C style).
 * plan_ir.h includes this header and wraps it with the ORCA-facing
 * DaftPlanSerializer, so existing consumers keep compiling unchanged.
 *
 * Design (P0-008-D acceptances):
 * - Stable IDs only; no raw pointers anywhere in the model => the payload is
 *   safe for copyObject / nodeToString / prepared-plan caching.
 * - No row values, no secrets, no pointer addresses: the model carries only
 *   structural metadata (names, type OIDs, op types, limit counts, mappings).
 * - SerializePlanToBytes rejects oversize plans with a stable error instead of
 *   producing a partial payload.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ORCA_PG_DAFT_PLAN_H
#define PG_ORCA_PG_DAFT_PLAN_H

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace pg_orca {
namespace daft {

using NodeId = uint32_t;
using SourceId = uint32_t;
using ColumnId = uint32_t;

constexpr NodeId INVALID_NODE_ID = 0;
constexpr SourceId INVALID_SOURCE_ID = 0;
constexpr ColumnId INVALID_COLUMN_ID = 0;

// SHA-256 capability-registry digest carried in the payload (same width as
// pg_orca::daft::CapabilityDigest, so the two assign directly).
constexpr size_t kDaftPlanDigestBytes = 32;
using DaftPlanDigest = std::array<uint8_t, kDaftPlanDigestBytes>;

/* Hard cap on the serialized payload. A plan exceeding this is rejected with a
 * stable error (P0-008-D acceptance: 超限稳定拒绝) rather than truncated. */
constexpr size_t kPgDaftPlanMaxBytes = 1024 * 1024;  // 1 MiB, frozen v1 cap

enum class DaftOperatorType : uint32_t
{
    INVALID = 0,
    PROJECT = 1,
    FILTER = 2,
    LIMIT = 3,
    SCAN_EXTERNAL = 4,
    PG_SOURCE = 5,
    HASH_JOIN = 6,
    HASH_AGGREGATE = 7,
    SORT = 8,
    TOP_N = 9
};

struct DaftColumnRef
{
    ColumnId id = INVALID_COLUMN_ID;
    std::string name;
    int32_t pg_type_oid = 0;
    int32_t pg_type_mod = -1;
    bool is_nullable = true;
    uint32_t collation_oid = 0;
    std::string arrow_format;
    std::string daft_dtype;
};

enum class DaftExprType : uint32_t
{
    INVALID = 0,
    COLUMN_REF = 1,
    CONST = 2,
    FUNC_CALL = 3,
    OP_EXPR = 4,
    BOOL_EXPR = 5,
    COMPARE = 6,
    NULL_TEST = 7
};

struct DaftExpression
{
    DaftExprType type = DaftExprType::INVALID;
    ColumnId column_id = INVALID_COLUMN_ID;
    std::string const_value;
    std::string literal_encoding;
    int32_t const_type_oid = 0;
    std::string func_name;
    uint32_t pg_oid = 0;
    bool is_not = false;
    DaftColumnRef result_type;
    std::vector<DaftExpression> args;
};

struct DaftProjection
{
    DaftExpression expr;
    ColumnId output_column_id = INVALID_COLUMN_ID;
    std::string output_column_name;
};

struct DaftHashKey
{
    DaftExpression left;
    DaftExpression right;
    // P1 admits ordinary SQL equality only.  This is deliberately explicit
    // on the wire so Daft can never silently change NULL matching semantics.
    bool null_equals_null = false;
};

struct DaftAggregateExpr
{
    std::string semantic_id;
    uint32_t pg_aggregate_oid = 0;
    std::vector<DaftExpression> args;
    bool distinct = false;
    std::vector<DaftExpression> order_by;
    bool has_filter = false;
    DaftExpression filter;
    ColumnId output_column_id = INVALID_COLUMN_ID;
};

struct DaftSortKey
{
    DaftExpression expr;
    uint32_t pg_sort_operator_oid = 0;
    bool descending = false;
    bool nulls_first = false;
    // P1-003-B admits only non-collatable keys.  The explicit string remains
    // on the wire so a later collation-aware phase cannot inherit a default.
    std::string collation_semantics = "none";
};

struct DaftPlanNode
{
    NodeId node_id = INVALID_NODE_ID;
    DaftOperatorType op_type = DaftOperatorType::INVALID;
    std::vector<NodeId> child_ids;
    std::vector<DaftProjection> projections;
    std::vector<DaftExpression> filters;
    std::string join_type;
    bool build_on_left = false;
    std::vector<DaftHashKey> hash_keys;
    std::vector<DaftExpression> join_filters;
    std::vector<DaftExpression> post_filters;
    std::vector<DaftProjection> group_keys;
    std::vector<DaftAggregateExpr> aggregates;
    std::vector<DaftSortKey> sort_keys;
    int64_t limit_count = -1;
    int64_t limit_offset = 0;
    SourceId scan_source_id = INVALID_SOURCE_ID;
    std::vector<ColumnId> output_column_ids;
    // Fragment this node belongs to after fusion (0 = not assigned). Fusion
    // only stamps this metadata; it never changes child_ids or op_type.
    uint32_t fragment_id = 0;
};

struct DaftDataSource
{
    SourceId source_id = INVALID_SOURCE_ID;
    std::string source_name;
    std::string source_type;
    std::string location;
    std::vector<ColumnId> column_ids;
};

struct DaftOutputMapping
{
    ColumnId daft_column_id = INVALID_COLUMN_ID;
    int32_t pg_target_index = 0;  // 1-based
    bool is_resjunk = false;
};

struct PgDaftPlan
{
    uint32_t plan_version = 1;
    NodeId root_node_id = INVALID_NODE_ID;
    std::unordered_map<NodeId, DaftPlanNode> nodes;
    std::unordered_map<ColumnId, DaftColumnRef> columns;
    std::unordered_map<SourceId, DaftDataSource> sources;
    std::vector<DaftOutputMapping> output_mappings;
    double estimated_rows = 0.0;
    double estimated_cost = 0.0;

    // Capability-registry digest stamped by the serializer from the query-frozen
    // snapshot (P0-008-A). The final validator compares it against the snapshot
    // still in scope; a mismatch is a frozen-digest consistency ERROR.
    DaftPlanDigest capability_digest{};
    bool has_capability_digest = false;

    // Canonical v1 envelope fields. `wire_json` is populated only by
    // DeserializePlanFromBytes so a copied BYTEAOID payload can be emitted
    // byte-for-byte without retaining any PostgreSQL or ORCA pointer.
    uint32_t fragment_id = 1;
    uint64_t required_runtime_capability_bits = 0;
    std::vector<SourceId> pg_source_drive_order;
    std::string payload_sha256;
    std::string wire_json;
};

/*-------------------------------------------------------------------------
 * JSON serialization (hand-rolled, deterministic, byte-exact round-trip).
 *-----------------------------------------------------------------------*/
namespace detail {

inline void
JsonEscape(std::string &out, const std::string &s)
{
    out += '"';
    for (char c : s)
    {
        switch (c)
        {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20)
                {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                }
                else
                {
                    out += c;
                }
        }
    }
    out += '"';
}

inline const char *
OpTypeName(DaftOperatorType t)
{
    switch (t)
    {
        case DaftOperatorType::PROJECT: return "PROJECT";
        case DaftOperatorType::FILTER: return "FILTER";
        case DaftOperatorType::LIMIT: return "LIMIT";
        case DaftOperatorType::SCAN_EXTERNAL: return "SCAN_EXTERNAL";
        case DaftOperatorType::PG_SOURCE: return "PG_SOURCE";
        case DaftOperatorType::HASH_JOIN: return "HASH_JOIN";
        case DaftOperatorType::HASH_AGGREGATE: return "HASH_AGGREGATE";
        case DaftOperatorType::SORT: return "SORT";
        case DaftOperatorType::TOP_N: return "TOP_N";
        case DaftOperatorType::INVALID: return "INVALID";
    }
    return "INVALID";
}

inline const char *
ExprTypeName(DaftExprType t)
{
    switch (t)
    {
        case DaftExprType::COLUMN_REF: return "COLUMN_REF";
        case DaftExprType::CONST: return "CONST";
        case DaftExprType::FUNC_CALL: return "FUNC_CALL";
        case DaftExprType::OP_EXPR: return "OP_EXPR";
        case DaftExprType::BOOL_EXPR: return "BOOL_EXPR";
        case DaftExprType::COMPARE: return "COMPARE";
        case DaftExprType::NULL_TEST: return "NULL_TEST";
        case DaftExprType::INVALID: return "INVALID";
    }
    return "INVALID";
}

} // namespace detail

/*-------------------------------------------------------------------------
 * Validation (structural integrity; no ORCA/PG types involved).
 *-----------------------------------------------------------------------*/
inline bool
ValidatePlan(const PgDaftPlan &plan, std::string &error_msg)
{
    if (plan.plan_version != 1)
    {
        error_msg = "unsupported plan version: " + std::to_string(plan.plan_version);
        return false;
    }
    if (plan.nodes.find(plan.root_node_id) == plan.nodes.end())
    {
        error_msg = "root node not found: " + std::to_string(plan.root_node_id);
        return false;
    }
    for (const auto &kv : plan.nodes)
    {
        const DaftPlanNode &node = kv.second;
        for (NodeId child_id : node.child_ids)
        {
            if (plan.nodes.find(child_id) == plan.nodes.end())
            {
                error_msg = "child node not found: " + std::to_string(child_id);
                return false;
            }
        }
        for (ColumnId col_id : node.output_column_ids)
        {
            if (plan.columns.find(col_id) == plan.columns.end())
            {
                error_msg = "output column not found: " + std::to_string(col_id);
                return false;
            }
        }
    }
    for (const auto &m : plan.output_mappings)
    {
        if (plan.columns.find(m.daft_column_id) == plan.columns.end())
        {
            error_msg = "output mapping references unknown column: " +
                        std::to_string(m.daft_column_id);
            return false;
        }
        if (m.pg_target_index <= 0)
        {
            error_msg = "invalid target index in output mapping: " +
                        std::to_string(m.pg_target_index);
            return false;
        }
    }
    error_msg.clear();
    return true;
}

/*
 * Frozen-digest consistency (design §4.5). A plan with no stamped digest is
 * trivially consistent; otherwise its stamp must be byte-equal to the
 * query-frozen expected digest. Pure function so the rule is unit-testable
 * standalone; PlanValidator::ValidateCapabilityDigest delegates here.
 */
inline bool
CapabilityDigestConsistent(const PgDaftPlan &plan, const DaftPlanDigest &expected)
{
    if (!plan.has_capability_digest)
        return true;
    return plan.capability_digest == expected;
}

/*-------------------------------------------------------------------------
 * JSON parser (minimal, structured, symmetric with SerializePlanToJson).
 *-----------------------------------------------------------------------*/
namespace detail {

struct JsonParser
{
    const std::string &s;
    size_t pos = 0;
    std::string err;

    explicit JsonParser(const std::string &str) : s(str) {}

    void SkipWs()
    {
        while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t' ||
               s[pos] == '\n' || s[pos] == '\r'))
            ++pos;
    }
    bool Peek(char c) { SkipWs(); return pos < s.size() && s[pos] == c; }
    bool Accept(char c)
    {
        if (Peek(c)) { ++pos; return true; }
        return false;
    }
    bool Expect(char c)
    {
        if (!Accept(c)) { err = "expected '"; err += c; err += "'"; return false; }
        return true;
    }
    bool ParseString(std::string &out)
    {
        SkipWs();
        if (pos >= s.size() || s[pos] != '"') { err = "expected string"; return false; }
        ++pos;
        out.clear();
        while (pos < s.size() && s[pos] != '"')
        {
            if (s[pos] == '\\' && pos + 1 < s.size())
            {
                char n = s[pos + 1];
                switch (n)
                {
                    case '"': out += '"'; break;
                    case '\\': out += '\\'; break;
                    case 'n': out += '\n'; break;
                    case 'r': out += '\r'; break;
                    case 't': out += '\t'; break;
                    default: out += n; break;
                }
                pos += 2;
            }
            else { out += s[pos++]; }
        }
        if (pos >= s.size()) { err = "unterminated string"; return false; }
        ++pos;  // closing quote
        return true;
    }
    bool ParseInt(long long &out)
    {
        SkipWs();
        size_t start = pos;
        if (pos < s.size() && (s[pos] == '-' || s[pos] == '+')) ++pos;
        while (pos < s.size() && (s[pos] >= '0' && s[pos] <= '9')) ++pos;
        if (start == pos || (pos == start + 1 && (s[start] == '-' || s[start] == '+')))
        { err = "expected integer"; return false; }
        try { out = std::stoll(s.substr(start, pos - start)); }
        catch (...) { err = "integer value out of range"; return false; }
        return true;
    }
    bool ParseDouble(double &out)
    {
        SkipWs();
        size_t start = pos;
        if (pos < s.size() && (s[pos] == '-' || s[pos] == '+')) ++pos;
        bool sawDigit = false, sawDot = false, sawE = false;
        while (pos < s.size())
        {
            char c = s[pos];
            if (c >= '0' && c <= '9') { sawDigit = true; ++pos; }
            else if (c == '.' && !sawDot) { sawDot = true; ++pos; }
            else if ((c == 'e' || c == 'E') && !sawE) { sawE = true; ++pos;
                if (pos < s.size() && (s[pos] == '+' || s[pos] == '-')) ++pos; }
            else break;
        }
        if (!sawDigit) { err = "expected number"; return false; }
        try { out = std::stod(s.substr(start, pos - start)); }
        catch (...) { err = "numeric value out of range"; return false; }
        return true;
    }
};

} // namespace detail

inline std::string
DigestToHex(const DaftPlanDigest &d)
{
    static const char hex[] = "0123456789abcdef";
    std::string s(kDaftPlanDigestBytes * 2, '0');
    for (size_t i = 0; i < kDaftPlanDigestBytes; ++i)
    {
        s[2 * i] = hex[(d[i] >> 4) & 0xF];
        s[2 * i + 1] = hex[d[i] & 0xF];
    }
    return s;
}

inline bool
HexToDigest(const std::string &hex, DaftPlanDigest &d)
{
    if (hex.size() != kDaftPlanDigestBytes * 2)
        return false;
    auto nib = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    for (size_t i = 0; i < kDaftPlanDigestBytes; ++i)
    {
        int hi = nib(hex[2 * i]), lo = nib(hex[2 * i + 1]);
        if (hi < 0 || lo < 0) return false;
        d[i] = static_cast<uint8_t>((hi << 4) | lo);
    }
    return true;
}

/*-------------------------------------------------------------------------
 * Serialization ( PgDaftPlan -> JSON text ).
 *-----------------------------------------------------------------------*/
inline void
SerializeExpression(std::string &out, const DaftExpression &e)
{
    out += "{\"type\":\"";
    out += detail::ExprTypeName(e.type);
    out += "\",\"column_id\":";
    out += std::to_string(e.column_id);
    out += ",\"const_value\":";
    detail::JsonEscape(out, e.const_value);
    out += ",\"const_type_oid\":";
    out += std::to_string(e.const_type_oid);
    out += ",\"func_name\":";
    detail::JsonEscape(out, e.func_name);
    out += ",\"args\":[";
    for (size_t i = 0; i < e.args.size(); ++i)
    {
        if (i) out += ',';
        SerializeExpression(out, e.args[i]);
    }
    out += "]}";
}

inline void
SerializeProjection(std::string &out, const DaftProjection &p)
{
    out += "{\"expr\":";
    SerializeExpression(out, p.expr);
    out += ",\"output_column_id\":";
    out += std::to_string(p.output_column_id);
    out += ",\"output_column_name\":";
    detail::JsonEscape(out, p.output_column_name);
    out += "}";
}

inline void
SerializeNode(std::string &out, const DaftPlanNode &n)
{
    out += "{\"node_id\":";
    out += std::to_string(n.node_id);
    out += ",\"op_type\":\"";
    out += detail::OpTypeName(n.op_type);
    out += "\",\"child_ids\":[";
    for (size_t i = 0; i < n.child_ids.size(); ++i)
    { if (i) out += ','; out += std::to_string(n.child_ids[i]); }
    out += "],\"projections\":[";
    for (size_t i = 0; i < n.projections.size(); ++i)
    { if (i) out += ','; SerializeProjection(out, n.projections[i]); }
    out += "],\"filters\":[";
    for (size_t i = 0; i < n.filters.size(); ++i)
    { if (i) out += ','; SerializeExpression(out, n.filters[i]); }
    out += "],\"limit_count\":";
    out += std::to_string(n.limit_count);
    out += ",\"limit_offset\":";
    out += std::to_string(n.limit_offset);
    out += ",\"scan_source_id\":";
    out += std::to_string(n.scan_source_id);
    out += ",\"output_column_ids\":[";
    for (size_t i = 0; i < n.output_column_ids.size(); ++i)
    { if (i) out += ','; out += std::to_string(n.output_column_ids[i]); }
    out += "],\"fragment_id\":";
    out += std::to_string(n.fragment_id);
    out += "}";
}

inline std::string PgDaftSha256Hex(const std::string &input);

inline std::string
SerializePlanToJson(const PgDaftPlan &plan)
{
    if (!plan.wire_json.empty())
        return plan.wire_json;

    auto serialize_type = [](std::string &dst, const DaftColumnRef &c) {
        dst += "{\"arrow_format\":";
        detail::JsonEscape(dst, c.arrow_format);
        dst += ",\"collation_oid\":" + std::to_string(c.collation_oid);
        dst += ",\"daft_dtype\":";
        detail::JsonEscape(dst, c.daft_dtype);
        dst += ",\"nullable\":";
        dst += c.is_nullable ? "true" : "false";
        dst += ",\"pg_oid\":" + std::to_string(c.pg_type_oid);
        dst += ",\"typmod\":" + std::to_string(c.pg_type_mod) + "}";
    };
    auto serialize_field = [&](std::string &dst, ColumnId id) {
        const DaftColumnRef &c = plan.columns.at(id);
        dst += "{\"column_id\":" + std::to_string(id) + ",\"name\":";
        detail::JsonEscape(dst, c.name);
        dst += ",\"type\":";
        serialize_type(dst, c);
        dst += "}";
    };
    std::function<void(std::string &, const DaftExpression &)> serialize_expr;
    serialize_expr = [&](std::string &dst, const DaftExpression &e) {
        switch (e.type)
        {
            case DaftExprType::COLUMN_REF:
                dst += "{\"column_id\":" + std::to_string(e.column_id) +
                       ",\"kind\":\"input_ref\",\"type\":";
                serialize_type(dst, plan.columns.at(e.column_id));
                dst += "}";
                return;
            case DaftExprType::CONST:
                dst += "{\"encoding\":";
                detail::JsonEscape(dst, e.literal_encoding);
                dst += ",\"kind\":\"literal\",\"type\":";
                serialize_type(dst, e.result_type);
                dst += ",\"value\":" + e.const_value + "}";
                return;
            case DaftExprType::BOOL_EXPR:
                dst += "{\"args\":[";
                for (size_t i = 0; i < e.args.size(); ++i)
                {
                    if (i) dst += ',';
                    serialize_expr(dst, e.args[i]);
                }
                dst += "],\"kind\":\"bool\",\"op\":";
                detail::JsonEscape(dst, e.func_name);
                dst += ",\"type\":";
                serialize_type(dst, e.result_type);
                dst += "}";
                return;
            case DaftExprType::COMPARE:
                dst += "{\"args\":[";
                for (size_t i = 0; i < e.args.size(); ++i)
                {
                    if (i) dst += ',';
                    serialize_expr(dst, e.args[i]);
                }
                dst += "],\"kind\":\"compare\",\"pg_oid\":" +
                       std::to_string(e.pg_oid) + ",\"semantic_id\":";
                detail::JsonEscape(dst, e.func_name);
                dst += ",\"type\":";
                serialize_type(dst, e.result_type);
                dst += "}";
                return;
            case DaftExprType::NULL_TEST:
                dst += "{\"input\":";
                serialize_expr(dst, e.args.front());
                dst += ",\"is_not\":";
                dst += e.is_not ? "true" : "false";
                dst += ",\"kind\":\"null_test\",\"type\":";
                serialize_type(dst, e.result_type);
                dst += "}";
                return;
            default:
                dst += "{\"kind\":\"invalid\"}";
                return;
        }
    };
    auto serialize_sort_key = [&](std::string &dst, const DaftSortKey &key) {
        dst += "{\"collation_semantics\":";
        detail::JsonEscape(dst, key.collation_semantics);
        dst += ",\"direction\":\"";
        dst += key.descending ? "desc" : "asc";
        dst += "\",\"expr\":";
        serialize_expr(dst, key.expr);
        dst += ",\"nulls_first\":";
        dst += key.nulls_first ? "true" : "false";
        dst += ",\"pg_sort_operator_oid\":" +
               std::to_string(key.pg_sort_operator_oid) + "}";
    };

    std::vector<SourceId> source_ids;
    for (const auto &kv : plan.sources) source_ids.push_back(kv.first);
    std::sort(source_ids.begin(), source_ids.end());
    std::vector<NodeId> node_ids;
    for (const auto &kv : plan.nodes) node_ids.push_back(kv.first);
    std::sort(node_ids.begin(), node_ids.end());

    uint64_t required_bits = plan.required_runtime_capability_bits;
    if (required_bits == 0)
    {
        // PLAN_V1 + ARROW_C_DATA_V1 + EMBEDDED_SWORDFISH are prerequisites
        // for every executable fragment. Source, Project, Limit are bits 3..5.
        required_bits = (1ULL << 0) | (1ULL << 1) | (1ULL << 2);
        if (!plan.sources.empty()) required_bits |= (1ULL << 3);
        for (NodeId id : node_ids)
        {
            const DaftPlanNode &n = plan.nodes.at(id);
            if (n.op_type == DaftOperatorType::PROJECT) required_bits |= (1ULL << 4);
            if (n.op_type == DaftOperatorType::LIMIT) required_bits |= (1ULL << 5);
            if (n.op_type == DaftOperatorType::FILTER) required_bits |= (1ULL << 6);
            if (n.op_type == DaftOperatorType::HASH_JOIN) required_bits |= (1ULL << 7);
            if (n.op_type == DaftOperatorType::HASH_AGGREGATE) required_bits |= (1ULL << 8);
            if (n.op_type == DaftOperatorType::SORT) required_bits |= (1ULL << 9);
            if (n.op_type == DaftOperatorType::TOP_N) required_bits |= (1ULL << 10);
        }
    }

    std::string body;
    body += "{\"capability_registry_sha256\":";
    detail::JsonEscape(body, DigestToHex(plan.capability_digest));
    body += ",\"fragment_id\":" + std::to_string(plan.fragment_id);
    body += ",\"nodes\":[";
    bool first_node = true;
    // PostgreSQL-owned inputs are real leaf nodes in the Bridge IR.
    for (SourceId sid : source_ids)
    {
        if (!first_node) body += ',';
        first_node = false;
        const DaftDataSource &src = plan.sources.at(sid);
        NodeId source_node_id = INVALID_NODE_ID;
        for (NodeId candidate : node_ids)
        {
            const DaftPlanNode &node = plan.nodes.at(candidate);
            if (node.op_type == DaftOperatorType::PG_SOURCE &&
                node.scan_source_id == sid)
            {
                source_node_id = candidate;
                break;
            }
        }
        body += "{\"id\":" + std::to_string(source_node_id) +
                ",\"kind\":\"source\",\"output\":[";
        for (size_t i = 0; i < src.column_ids.size(); ++i)
        {
            if (i) body += ',';
            serialize_field(body, src.column_ids[i]);
        }
        body += "],\"source_id\":" + std::to_string(sid) + "}";
    }
    for (NodeId id : node_ids)
    {
        const DaftPlanNode &n = plan.nodes.at(id);
        if (n.op_type == DaftOperatorType::PG_SOURCE) continue;
        if (!first_node) body += ',';
        first_node = false;
        if (n.op_type == DaftOperatorType::PROJECT)
        {
            body += "{\"exprs\":[";
            for (size_t i = 0; i < n.projections.size(); ++i)
            {
                if (i) body += ',';
                const DaftProjection &p = n.projections[i];
                body += "{\"expr\":";
                serialize_expr(body, p.expr);
                body += ",\"output\":";
                serialize_field(body, p.output_column_id);
                body += "}";
            }
            NodeId input = n.child_ids.empty() ? 0 : n.child_ids.front();
            body += "],\"id\":" + std::to_string(id) +
                    ",\"input\":" + std::to_string(input) +
                    ",\"kind\":\"project\"}";
        }
        else if (n.op_type == DaftOperatorType::LIMIT)
        {
            NodeId input = n.child_ids.empty() ? 0 : n.child_ids.front();
            body += "{\"count\":" + std::to_string(n.limit_count) +
                    ",\"id\":" + std::to_string(id) +
                    ",\"input\":" + std::to_string(input) +
                    ",\"kind\":\"limit\",\"offset\":" +
                    std::to_string(n.limit_offset) + "}";
        }
        else if (n.op_type == DaftOperatorType::FILTER)
        {
            NodeId input = n.child_ids.empty() ? 0 : n.child_ids.front();
            body += "{\"id\":" + std::to_string(id) +
                    ",\"input\":" + std::to_string(input) +
                    ",\"kind\":\"filter\",\"predicate\":";
            serialize_expr(body, n.filters.front());
            body += "}";
        }
        else if (n.op_type == DaftOperatorType::HASH_JOIN)
        {
            const NodeId left = n.child_ids.size() > 0 ? n.child_ids[0] : 0;
            const NodeId right = n.child_ids.size() > 1 ? n.child_ids[1] : 0;
            body += "{\"build_side\":\"";
            body += n.build_on_left ? "left" : "right";
            body += "\",\"id\":" + std::to_string(id) +
                    ",\"join_filter\":";
            if (n.join_filters.empty())
                body += "null";
            else
                serialize_expr(body, n.join_filters.front());
            body += ",\"join_type\":";
            detail::JsonEscape(body, n.join_type);
            body += ",\"keys\":[";
            for (size_t i = 0; i < n.hash_keys.size(); ++i)
            {
                if (i) body += ',';
                body += "{\"left\":";
                serialize_expr(body, n.hash_keys[i].left);
                body += ",\"null_equals_null\":";
                body += n.hash_keys[i].null_equals_null ? "true" : "false";
                body += ",\"right\":";
                serialize_expr(body, n.hash_keys[i].right);
                body += "}";
            }
            body += "],\"kind\":\"hash_join\",\"left\":" +
                    std::to_string(left) + ",\"post_filter\":";
            if (n.post_filters.empty())
                body += "null";
            else
                serialize_expr(body, n.post_filters.front());
            body += ",\"right\":" + std::to_string(right) + "}";
        }
        else if (n.op_type == DaftOperatorType::HASH_AGGREGATE)
        {
            const NodeId input = n.child_ids.empty() ? 0 : n.child_ids.front();
            body += "{\"aggregates\":[";
            for (size_t i = 0; i < n.aggregates.size(); ++i)
            {
                if (i) body += ',';
                const DaftAggregateExpr &aggregate = n.aggregates[i];
                body += "{\"args\":[";
                for (size_t j = 0; j < aggregate.args.size(); ++j)
                {
                    if (j) body += ',';
                    serialize_expr(body, aggregate.args[j]);
                }
                body += "],\"distinct\":";
                body += aggregate.distinct ? "true" : "false";
                body += ",\"filter\":";
                if (aggregate.has_filter)
                    serialize_expr(body, aggregate.filter);
                else
                    body += "null";
                body += ",\"order_by\":[";
                // P1 rejects aggregate-local ORDER BY before serialization.
                body += "],\"output\":";
                serialize_field(body, aggregate.output_column_id);
                body += ",\"pg_aggregate_oid\":" +
                        std::to_string(aggregate.pg_aggregate_oid) +
                        ",\"semantic_id\":";
                detail::JsonEscape(body, aggregate.semantic_id);
                body += "}";
            }
            body += "],\"group_keys\":[";
            for (size_t i = 0; i < n.group_keys.size(); ++i)
            {
                if (i) body += ',';
                body += "{\"expr\":";
                serialize_expr(body, n.group_keys[i].expr);
                body += ",\"output\":";
                serialize_field(body, n.group_keys[i].output_column_id);
                body += "}";
            }
            body += "],\"grouping_mode\":\"simple\",\"id\":" +
                    std::to_string(id) + ",\"input\":" +
                    std::to_string(input) +
                    ",\"kind\":\"hash_aggregate\",\"stage\":\"complete\"}";
        }
        else if (n.op_type == DaftOperatorType::SORT)
        {
            const NodeId input = n.child_ids.empty() ? 0 : n.child_ids.front();
            body += "{\"id\":" + std::to_string(id) +
                    ",\"input\":" + std::to_string(input) +
                    ",\"keys\":[";
            for (size_t i = 0; i < n.sort_keys.size(); ++i)
            {
                if (i) body += ',';
                serialize_sort_key(body, n.sort_keys[i]);
            }
            body += "],\"kind\":\"sort\"}";
        }
        else if (n.op_type == DaftOperatorType::TOP_N)
        {
            const NodeId input = n.child_ids.empty() ? 0 : n.child_ids.front();
            body += "{\"id\":" + std::to_string(id) +
                    ",\"input\":" + std::to_string(input) +
                    ",\"keys\":[";
            for (size_t i = 0; i < n.sort_keys.size(); ++i)
            {
                if (i) body += ',';
                serialize_sort_key(body, n.sort_keys[i]);
            }
            body += "],\"kind\":\"top_n\",\"limit\":" +
                    std::to_string(n.limit_count) + ",\"offset\":" +
                    std::to_string(n.limit_offset) + "}";
        }
    }
    body += "],\"output_schema\":[";
    std::vector<DaftOutputMapping> mappings = plan.output_mappings;
    std::sort(mappings.begin(), mappings.end(),
              [](const DaftOutputMapping &a, const DaftOutputMapping &b) {
                  return a.pg_target_index < b.pg_target_index;
              });
    for (size_t i = 0; i < mappings.size(); ++i)
    {
        if (i) body += ',';
        serialize_field(body, mappings[i].daft_column_id);
    }
    body += "],\"pg_source_drive_order\":[";
    const std::vector<SourceId> &drive = plan.pg_source_drive_order.empty()
                                              ? source_ids
                                              : plan.pg_source_drive_order;
    for (size_t i = 0; i < drive.size(); ++i)
    {
        if (i) body += ',';
        body += std::to_string(drive[i]);
    }
    body += "],\"required_runtime_capability_bits\":" +
            std::to_string(required_bits);
    body += ",\"root_node_id\":" + std::to_string(plan.root_node_id);
    body += ",\"sources\":[";
    for (size_t i = 0; i < source_ids.size(); ++i)
    {
        if (i) body += ',';
        SourceId sid = source_ids[i];
        const DaftDataSource &src = plan.sources.at(sid);
        body += "{\"fields\":[";
        for (size_t j = 0; j < src.column_ids.size(); ++j)
        {
            if (j) body += ',';
            serialize_field(body, src.column_ids[j]);
        }
        body += "],\"source_id\":" + std::to_string(sid) + "}";
    }
    body += "]}";

    // SHA-256 implementation is declared below. Insert the digest without
    // changing `body`: Rust removes payload_sha256 and hashes this same
    // lexicographically ordered JSON value.
    const std::string digest = PgDaftSha256Hex(body);
    std::string out = body;
    const std::string marker = "\"pg_source_drive_order\"";
    size_t pos = out.find(marker);
    out.insert(pos, "\"payload_sha256\":\"" + digest + "\",");
    return out;
#if 0
    std::string out;
    out += "{\"plan_version\":";
    out += std::to_string(plan.plan_version);
    out += ",\"root_node_id\":";
    out += std::to_string(plan.root_node_id);

    // maps serialized as sorted arrays of {"key":k,"value":v} for determinism
    out += ",\"nodes\":[";
    {
        std::vector<NodeId> keys;
        keys.reserve(plan.nodes.size());
        for (const auto &kv : plan.nodes) keys.push_back(kv.first);
        std::sort(keys.begin(), keys.end());
        for (size_t i = 0; i < keys.size(); ++i)
        {
            if (i) out += ',';
            out += "{\"key\":";
            out += std::to_string(keys[i]);
            out += ",\"value\":";
            SerializeNode(out, plan.nodes.at(keys[i]));
            out += "}";
        }
    }
    out += "],\"columns\":[";
    {
        std::vector<ColumnId> keys;
        keys.reserve(plan.columns.size());
        for (const auto &kv : plan.columns) keys.push_back(kv.first);
        std::sort(keys.begin(), keys.end());
        for (size_t i = 0; i < keys.size(); ++i)
        {
            const DaftColumnRef &c = plan.columns.at(keys[i]);
            if (i) out += ',';
            out += "{\"key\":";
            out += std::to_string(keys[i]);
            out += ",\"value\":{\"id\":";
            out += std::to_string(c.id);
            out += ",\"name\":";
            detail::JsonEscape(out, c.name);
            out += ",\"pg_type_oid\":";
            out += std::to_string(c.pg_type_oid);
            out += ",\"pg_type_mod\":";
            out += std::to_string(c.pg_type_mod);
            out += ",\"is_nullable\":";
            out += (c.is_nullable ? "true" : "false");
            out += "}}";
        }
    }
    out += "],\"sources\":[";
    {
        std::vector<SourceId> keys;
        keys.reserve(plan.sources.size());
        for (const auto &kv : plan.sources) keys.push_back(kv.first);
        std::sort(keys.begin(), keys.end());
        for (size_t i = 0; i < keys.size(); ++i)
        {
            const DaftDataSource &src = plan.sources.at(keys[i]);
            if (i) out += ',';
            out += "{\"key\":";
            out += std::to_string(keys[i]);
            out += ",\"value\":{\"source_id\":";
            out += std::to_string(src.source_id);
            out += ",\"source_name\":";
            detail::JsonEscape(out, src.source_name);
            out += ",\"source_type\":";
            detail::JsonEscape(out, src.source_type);
            out += ",\"location\":";
            detail::JsonEscape(out, src.location);
            out += ",\"column_ids\":[";
            for (size_t j = 0; j < src.column_ids.size(); ++j)
            { if (j) out += ','; out += std::to_string(src.column_ids[j]); }
            out += "]}}";
        }
    }
    out += "],\"output_mappings\":[";
    for (size_t i = 0; i < plan.output_mappings.size(); ++i)
    {
        const DaftOutputMapping &m = plan.output_mappings[i];
        if (i) out += ',';
        out += "{\"daft_column_id\":";
        out += std::to_string(m.daft_column_id);
        out += ",\"pg_target_index\":";
        out += std::to_string(m.pg_target_index);
        out += ",\"is_resjunk\":";
        out += (m.is_resjunk ? "true" : "false");
        out += "}";
    }
    out += "],\"estimated_rows\":";
    {
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%.17g", plan.estimated_rows);
        out += buf;
    }
    out += ",\"estimated_cost\":";
    {
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%.17g", plan.estimated_cost);
        out += buf;
    }
    out += ",\"capability_digest\":";
    detail::JsonEscape(out, DigestToHex(plan.capability_digest));
    out += ",\"has_capability_digest\":";
    out += (plan.has_capability_digest ? "true" : "false");
    out += "}";
    return out;
#endif
}

/* Small self-contained SHA-256 used by both standalone tests and the ORCA
 * translator. Keeping it here avoids OpenSSL/PostgreSQL process dependencies
 * in the frozen wire-contract layer. */
inline std::string
PgDaftSha256Hex(const std::string &input)
{
    static constexpr uint32_t k[64] = {
        0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
        0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
        0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
        0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
        0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
        0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
        0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
        0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2};
    auto rotr = [](uint32_t v, unsigned n) { return (v >> n) | (v << (32 - n)); };
    std::vector<uint8_t> msg(input.begin(), input.end());
    const uint64_t bit_len = static_cast<uint64_t>(msg.size()) * 8;
    msg.push_back(0x80);
    while ((msg.size() % 64) != 56) msg.push_back(0);
    for (int i = 7; i >= 0; --i) msg.push_back(static_cast<uint8_t>(bit_len >> (i * 8)));
    uint32_t h[8] = {0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,
                     0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19};
    for (size_t off = 0; off < msg.size(); off += 64)
    {
        uint32_t w[64];
        for (size_t i = 0; i < 16; ++i)
            w[i] = (uint32_t(msg[off+i*4]) << 24) | (uint32_t(msg[off+i*4+1]) << 16) |
                   (uint32_t(msg[off+i*4+2]) << 8) | uint32_t(msg[off+i*4+3]);
        for (size_t i = 16; i < 64; ++i)
        {
            uint32_t s0 = rotr(w[i-15],7) ^ rotr(w[i-15],18) ^ (w[i-15] >> 3);
            uint32_t s1 = rotr(w[i-2],17) ^ rotr(w[i-2],19) ^ (w[i-2] >> 10);
            w[i] = w[i-16] + s0 + w[i-7] + s1;
        }
        uint32_t a=h[0],b=h[1],c=h[2],d=h[3],e=h[4],f=h[5],g=h[6],hh=h[7];
        for (size_t i = 0; i < 64; ++i)
        {
            uint32_t s1=rotr(e,6)^rotr(e,11)^rotr(e,25), ch=(e&f)^((~e)&g);
            uint32_t t1=hh+s1+ch+k[i]+w[i];
            uint32_t s0=rotr(a,2)^rotr(a,13)^rotr(a,22), maj=(a&b)^(a&c)^(b&c);
            uint32_t t2=s0+maj;
            hh=g; g=f; f=e; e=d+t1; d=c; c=b; b=a; a=t1+t2;
        }
        h[0]+=a; h[1]+=b; h[2]+=c; h[3]+=d; h[4]+=e; h[5]+=f; h[6]+=g; h[7]+=hh;
    }
    char buf[65];
    for (size_t i = 0; i < 8; ++i) std::snprintf(buf + i*8, 9, "%08x", h[i]);
    buf[64] = '\0';
    return std::string(buf);
}

/*-------------------------------------------------------------------------
 * Deserialization ( JSON text -> PgDaftPlan ), symmetric with the above.
 *-----------------------------------------------------------------------*/
namespace detail {

inline bool ParseExpr(JsonParser &p, DaftExpression &e);
inline bool ParseProjection(JsonParser &p, DaftProjection &pr);
inline bool ParseNode(JsonParser &p, DaftPlanNode &n);

inline bool
ParseStringField(JsonParser &p, std::string &out)
{
    return p.ParseString(out);
}
inline bool
ExpectKeyColon(JsonParser &p, const char *key)
{
    std::string k;
    if (!p.ParseString(k)) return false;
    if (k != key) { p.err = std::string("expected key '") + key + "' got '" + k + "'"; return false; }
    return p.Expect(':');
}

inline bool
ParseExpr(JsonParser &p, DaftExpression &e)
{
    if (!p.Expect('{')) return false;
    if (!ExpectKeyColon(p, "type")) return false;
    { std::string t; if (!p.ParseString(t)) return false;
      e.type = (t=="COLUMN_REF"?DaftExprType::COLUMN_REF: t=="CONST"?DaftExprType::CONST:
                t=="FUNC_CALL"?DaftExprType::FUNC_CALL: t=="OP_EXPR"?DaftExprType::OP_EXPR:
                DaftExprType::INVALID); }
    if (!p.Accept(',')) { p.err="expected ','"; return false; }
    if (!ExpectKeyColon(p, "column_id")) return false;
    { long long v=0; if (!p.ParseInt(v)) return false; e.column_id = (ColumnId)v; }
    if (!p.Accept(',')) { p.err="expected ','"; return false; }
    if (!ExpectKeyColon(p, "const_value")) return false;
    if (!ParseStringField(p, e.const_value)) return false;
    if (!p.Accept(',')) { p.err="expected ','"; return false; }
    if (!ExpectKeyColon(p, "const_type_oid")) return false;
    { long long v=0; if (!p.ParseInt(v)) return false; e.const_type_oid = (int32_t)v; }
    if (!p.Accept(',')) { p.err="expected ','"; return false; }
    if (!ExpectKeyColon(p, "func_name")) return false;
    if (!ParseStringField(p, e.func_name)) return false;
    if (!p.Accept(',')) { p.err="expected ','"; return false; }
    if (!ExpectKeyColon(p, "args")) return false;
    if (!p.Expect('[')) return false;
    if (!p.Peek(']'))
    {
        do { DaftExpression a; if (!ParseExpr(p, a)) return false; e.args.push_back(a);
        } while (p.Accept(','));
    }
    if (!p.Expect(']')) return false;
    if (!p.Expect('}')) return false;
    return true;
}

inline bool
ParseProjection(JsonParser &p, DaftProjection &pr)
{
    if (!p.Expect('{')) return false;
    if (!ExpectKeyColon(p, "expr")) return false;
    if (!ParseExpr(p, pr.expr)) return false;
    if (!p.Accept(',')) { p.err="expected ','"; return false; }
    if (!ExpectKeyColon(p, "output_column_id")) return false;
    { long long v=0; if (!p.ParseInt(v)) return false; pr.output_column_id = (ColumnId)v; }
    if (!p.Accept(',')) { p.err="expected ','"; return false; }
    if (!ExpectKeyColon(p, "output_column_name")) return false;
    if (!ParseStringField(p, pr.output_column_name)) return false;
    if (!p.Expect('}')) return false;
    return true;
}

inline bool
ParseNode(JsonParser &p, DaftPlanNode &n)
{
    if (!p.Expect('{')) return false;
    if (!ExpectKeyColon(p, "node_id")) return false;
    { long long v=0; if (!p.ParseInt(v)) return false; n.node_id = (NodeId)v; }
    if (!p.Accept(',')) { p.err="expected ','"; return false; }
    if (!ExpectKeyColon(p, "op_type")) return false;
    { std::string t; if (!p.ParseString(t)) return false;
      n.op_type = (t=="PROJECT"?DaftOperatorType::PROJECT: t=="FILTER"?DaftOperatorType::FILTER:
                   t=="LIMIT"?DaftOperatorType::LIMIT: t=="SCAN_EXTERNAL"?DaftOperatorType::SCAN_EXTERNAL:
                   DaftOperatorType::INVALID); }
    if (!p.Accept(',')) { p.err="expected ','"; return false; }
    if (!ExpectKeyColon(p, "child_ids")) return false;
    if (!p.Expect('[')) return false;
    if (!p.Peek(']')) { do { long long v=0; if (!p.ParseInt(v)) return false; n.child_ids.push_back((NodeId)v); } while (p.Accept(',')); }
    if (!p.Expect(']')) return false;
    if (!p.Accept(',')) { p.err="expected ','"; return false; }
    if (!ExpectKeyColon(p, "projections")) return false;
    if (!p.Expect('[')) return false;
    if (!p.Peek(']')) { do { DaftProjection pr; if (!ParseProjection(p, pr)) return false; n.projections.push_back(pr); } while (p.Accept(',')); }
    if (!p.Expect(']')) return false;
    if (!p.Accept(',')) { p.err="expected ','"; return false; }
    if (!ExpectKeyColon(p, "filters")) return false;
    if (!p.Expect('[')) return false;
    if (!p.Peek(']')) { do { DaftExpression e; if (!ParseExpr(p, e)) return false; n.filters.push_back(e); } while (p.Accept(',')); }
    if (!p.Expect(']')) return false;
    if (!p.Accept(',')) { p.err="expected ','"; return false; }
    if (!ExpectKeyColon(p, "limit_count")) return false;
    { long long v=0; if (!p.ParseInt(v)) return false; n.limit_count = v; }
    if (!p.Accept(',')) { p.err="expected ','"; return false; }
    if (!ExpectKeyColon(p, "limit_offset")) return false;
    { long long v=0; if (!p.ParseInt(v)) return false; n.limit_offset = v; }
    if (!p.Accept(',')) { p.err="expected ','"; return false; }
    if (!ExpectKeyColon(p, "scan_source_id")) return false;
    { long long v=0; if (!p.ParseInt(v)) return false; n.scan_source_id = (SourceId)v; }
    if (!p.Accept(',')) { p.err="expected ','"; return false; }
    if (!ExpectKeyColon(p, "output_column_ids")) return false;
    if (!p.Expect('[')) return false;
    if (!p.Peek(']')) { do { long long v=0; if (!p.ParseInt(v)) return false; n.output_column_ids.push_back((ColumnId)v); } while (p.Accept(',')); }
    if (!p.Expect(']')) return false;
    if (!p.Accept(',')) { p.err="expected ','"; return false; }
    if (!ExpectKeyColon(p, "fragment_id")) return false;
    { long long v=0; if (!p.ParseInt(v)) return false; n.fragment_id = (uint32_t)v; }
    if (!p.Expect('}')) return false;
    return true;
}

} // namespace detail

inline bool
DeserializePlanFromJson(const std::string &json, PgDaftPlan &plan, std::string &err)
{
    detail::JsonParser p(json);
    plan = PgDaftPlan{};
    if (!p.Expect('{')) { err = p.err; return false; }
    if (!detail::ExpectKeyColon(p, "plan_version")) { err = p.err; return false; }
    { long long v=0; if (!p.ParseInt(v)) { err=p.err; return false; } plan.plan_version = (uint32_t)v; }
    if (!p.Accept(',')) { err="expected ','"; return false; }
    if (!detail::ExpectKeyColon(p, "root_node_id")) { err=p.err; return false; }
    { long long v=0; if (!p.ParseInt(v)) { err=p.err; return false; } plan.root_node_id = (NodeId)v; }

    auto readIdMapNodes = [&](const char *key) -> bool {
        if (!p.Accept(',')) { p.err="expected ','"; return false; }
        if (!detail::ExpectKeyColon(p, key)) return false;
        if (!p.Expect('[')) return false;
        if (!p.Peek(']'))
        {
            do {
                if (!p.Expect('{')) return false;
                if (!detail::ExpectKeyColon(p, "key")) return false;
                long long k=0; if (!p.ParseInt(k)) return false;
                if (!p.Accept(',')) { p.err="expected ','"; return false; }
                if (!detail::ExpectKeyColon(p, "value")) return false;
                DaftPlanNode n; if (!detail::ParseNode(p, n)) return false;
                n.node_id = (NodeId)k;
                plan.nodes[(NodeId)k] = n;
                if (!p.Expect('}')) return false;
            } while (p.Accept(','));
        }
        if (!p.Expect(']')) return false;
        return true;
    };
    if (!readIdMapNodes("nodes")) { err = p.err; return false; }

    // columns
    if (!p.Accept(',')) { err="expected ','"; return false; }
    if (!detail::ExpectKeyColon(p, "columns")) { err=p.err; return false; }
    if (!p.Expect('[')) { err=p.err; return false; }
    if (!p.Peek(']'))
    {
        do {
            if (!p.Expect('{')) { err=p.err; return false; }
            if (!detail::ExpectKeyColon(p, "key")) { err=p.err; return false; }
            long long k=0; if (!p.ParseInt(k)) { err=p.err; return false; }
            if (!p.Accept(',')) { err="expected ','"; return false; }
            if (!detail::ExpectKeyColon(p, "value")) { err=p.err; return false; }
            if (!p.Expect('{')) { err=p.err; return false; }
            DaftColumnRef c;
            if (!detail::ExpectKeyColon(p, "id")) { err=p.err; return false; }
            { long long v=0; if (!p.ParseInt(v)) { err=p.err; return false; } c.id=(ColumnId)v; }
            if (!p.Accept(',')) { err="expected ','"; return false; }
            if (!detail::ExpectKeyColon(p, "name")) { err=p.err; return false; } if (!p.ParseString(c.name)) { err=p.err; return false; }
            if (!p.Accept(',')) { err="expected ','"; return false; }
            if (!detail::ExpectKeyColon(p, "pg_type_oid")) { err=p.err; return false; } { long long v=0; if (!p.ParseInt(v)) { err=p.err; return false; } c.pg_type_oid=(int32_t)v; }
            if (!p.Accept(',')) { err="expected ','"; return false; }
            if (!detail::ExpectKeyColon(p, "pg_type_mod")) { err=p.err; return false; } { long long v=0; if (!p.ParseInt(v)) { err=p.err; return false; } c.pg_type_mod=(int32_t)v; }
            if (!p.Accept(',')) { err="expected ','"; return false; }
            if (!detail::ExpectKeyColon(p, "is_nullable")) { err=p.err; return false; }
            { p.SkipWs(); if (p.pos < p.s.size() && p.s[p.pos]=='t') { c.is_nullable=true; p.pos+=4; } else { c.is_nullable=false; p.pos+=5; } }
            if (!p.Expect('}')) { err=p.err; return false; }
            plan.columns[(ColumnId)k] = c;
            if (!p.Expect('}')) { err=p.err; return false; }
        } while (p.Accept(','));
    }
    if (!p.Expect(']')) { err=p.err; return false; }

    // sources
    if (!p.Accept(',')) { err="expected ','"; return false; }
    if (!detail::ExpectKeyColon(p, "sources")) { err=p.err; return false; }
    if (!p.Expect('[')) { err=p.err; return false; }
    if (!p.Peek(']'))
    {
        do {
            if (!p.Expect('{')) { err=p.err; return false; }
            if (!detail::ExpectKeyColon(p, "key")) { err=p.err; return false; }
            long long k=0; if (!p.ParseInt(k)) { err=p.err; return false; }
            if (!p.Accept(',')) { err="expected ','"; return false; }
            if (!detail::ExpectKeyColon(p, "value")) { err=p.err; return false; }
            if (!p.Expect('{')) { err=p.err; return false; }
            DaftDataSource src;
            if (!detail::ExpectKeyColon(p, "source_id")) { err=p.err; return false; } { long long v=0; if (!p.ParseInt(v)) { err=p.err; return false; } src.source_id=(SourceId)v; }
            if (!p.Accept(',')) { err="expected ','"; return false; }
            if (!detail::ExpectKeyColon(p, "source_name")) { err=p.err; return false; } if (!p.ParseString(src.source_name)) { err=p.err; return false; }
            if (!p.Accept(',')) { err="expected ','"; return false; }
            if (!detail::ExpectKeyColon(p, "source_type")) { err=p.err; return false; } if (!p.ParseString(src.source_type)) { err=p.err; return false; }
            if (!p.Accept(',')) { err="expected ','"; return false; }
            if (!detail::ExpectKeyColon(p, "location")) { err=p.err; return false; } if (!p.ParseString(src.location)) { err=p.err; return false; }
            if (!p.Accept(',')) { err="expected ','"; return false; }
            if (!detail::ExpectKeyColon(p, "column_ids")) { err=p.err; return false; }
            if (!p.Expect('[')) { err=p.err; return false; }
            if (!p.Peek(']')) { do { long long v=0; if (!p.ParseInt(v)) { err=p.err; return false; } src.column_ids.push_back((ColumnId)v); } while (p.Accept(',')); }
            if (!p.Expect(']')) { err=p.err; return false; }
            if (!p.Expect('}')) { err=p.err; return false; }
            plan.sources[(SourceId)k] = src;
            if (!p.Expect('}')) { err=p.err; return false; }
        } while (p.Accept(','));
    }
    if (!p.Expect(']')) { err=p.err; return false; }

    // output_mappings
    if (!p.Accept(',')) { err="expected ','"; return false; }
    if (!detail::ExpectKeyColon(p, "output_mappings")) { err=p.err; return false; }
    if (!p.Expect('[')) { err=p.err; return false; }
    if (!p.Peek(']'))
    {
        do {
            if (!p.Expect('{')) { err=p.err; return false; }
            DaftOutputMapping m;
            if (!detail::ExpectKeyColon(p, "daft_column_id")) { err=p.err; return false; } { long long v=0; if (!p.ParseInt(v)) { err=p.err; return false; } m.daft_column_id=(ColumnId)v; }
            if (!p.Accept(',')) { err="expected ','"; return false; }
            if (!detail::ExpectKeyColon(p, "pg_target_index")) { err=p.err; return false; } { long long v=0; if (!p.ParseInt(v)) { err=p.err; return false; } m.pg_target_index=(int32_t)v; }
            if (!p.Accept(',')) { err="expected ','"; return false; }
            if (!detail::ExpectKeyColon(p, "is_resjunk")) { err=p.err; return false; }
            { p.SkipWs(); if (p.pos < p.s.size() && p.s[p.pos]=='t') { m.is_resjunk=true; p.pos+=4; } else { m.is_resjunk=false; p.pos+=5; } }
            if (!p.Expect('}')) { err=p.err; return false; }
            plan.output_mappings.push_back(m);
        } while (p.Accept(','));
    }
    if (!p.Expect(']')) { err=p.err; return false; }

    if (!p.Accept(',')) { err="expected ','"; return false; }
    if (!detail::ExpectKeyColon(p, "estimated_rows")) { err=p.err; return false; } if (!p.ParseDouble(plan.estimated_rows)) { err=p.err; return false; }
    if (!p.Accept(',')) { err="expected ','"; return false; }
    if (!detail::ExpectKeyColon(p, "estimated_cost")) { err=p.err; return false; } if (!p.ParseDouble(plan.estimated_cost)) { err=p.err; return false; }
    if (!p.Accept(',')) { err="expected ','"; return false; }
    if (!detail::ExpectKeyColon(p, "capability_digest")) { err=p.err; return false; }
    { std::string hex; if (!p.ParseString(hex)) { err=p.err; return false; } if (!HexToDigest(hex, plan.capability_digest)) { err="bad capability_digest hex"; return false; } }
    if (!p.Accept(',')) { err="expected ','"; return false; }
    if (!detail::ExpectKeyColon(p, "has_capability_digest")) { err=p.err; return false; }
    { p.SkipWs(); if (p.pos < p.s.size() && p.s[p.pos]=='t') { plan.has_capability_digest=true; p.pos+=4; } else { plan.has_capability_digest=false; p.pos+=5; } }
    if (!p.Expect('}')) { err=p.err; return false; }
    err.clear();
    return true;
}

/*-------------------------------------------------------------------------
 * Byte (BYTEAOID) wrappers with oversize rejection.
 *-----------------------------------------------------------------------*/
inline bool
SerializePlanToBytes(const PgDaftPlan &plan, std::vector<uint8_t> &out, std::string &err)
{
    std::string json = SerializePlanToJson(plan);
    if (json.size() > kPgDaftPlanMaxBytes)
    {
        err = "PgDaftPlan payload " + std::to_string(json.size()) +
              " bytes exceeds limit " + std::to_string(kPgDaftPlanMaxBytes);
        out.clear();
        return false;
    }
    out.assign(json.begin(), json.end());
    err.clear();
    return true;
}

inline bool
DeserializePlanFromBytes(const std::vector<uint8_t> &in, PgDaftPlan &plan, std::string &err)
{
    if (in.size() > kPgDaftPlanMaxBytes)
    {
        err = "PgDaftPlan payload " + std::to_string(in.size()) +
              " bytes exceeds limit " + std::to_string(kPgDaftPlanMaxBytes);
        return false;
    }
    std::string json(in.begin(), in.end());
    // Canonical v1 payloads are opaque self-contained plan data at the PG
    // node-copy boundary. Validate the checksum and preserve the exact bytes;
    // semantic graph validation is performed by the Bridge and final
    // validator. No executor handle, datum, pointer, or catalog object is
    // reconstructed here.
    const std::string prefix = "\"payload_sha256\":\"";
    size_t key = json.find(prefix);
    if (json.find("\"capability_registry_sha256\"") != std::string::npos &&
        key != std::string::npos)
    {
        size_t value = key + prefix.size();
        size_t end = json.find('"', value);
        if (end == std::string::npos || end - value != 64 ||
            end + 1 >= json.size() || json[end + 1] != ',')
        {
            err = "PgDaftPlan payload_sha256 has invalid canonical encoding";
            return false;
        }
        std::string claimed = json.substr(value, 64);
        std::string body = json;
        body.erase(key, (end + 2) - key);
        if (PgDaftSha256Hex(body) != claimed)
        {
            err = "PgDaftPlan payload_sha256 mismatch";
            return false;
        }
        plan = PgDaftPlan{};
        plan.payload_sha256 = claimed;
        plan.wire_json = json;
        err.clear();
        return true;
    }
    return DeserializePlanFromJson(json, plan, err);
}

} // namespace daft
} // namespace pg_orca

#endif // PG_ORCA_PG_DAFT_PLAN_H
