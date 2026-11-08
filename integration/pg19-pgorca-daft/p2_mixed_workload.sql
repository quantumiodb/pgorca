\set workload random(1, 4)
\set low_key random(0, 700)
\set high_key :low_key + 200
\set limit_rows random(64, 512)

SET application_name = 'pgd_p2_mixed';
BEGIN;
SET LOCAL pg_orca.enable_orca = on;
SET LOCAL pg_orca.daft_min_rows = 0;
SET LOCAL pg_orca.daft_min_cost = 0;
SET LOCAL pg_orca.cost_model = 'pg';
SET LOCAL pg_orca.daft_cost_calibration_profile = 'test_prefer_daft_v1';
SET LOCAL pg_orca.daft_trace = off;
SET LOCAL work_mem = '4MB';
SET LOCAL hash_mem_multiplier = 2.0;
SET LOCAL effective_cache_size = '4GB';

\if :workload = 1
SET LOCAL pg_orca.daft_enable = 'off';
SELECT count(*)::bigint AS pg_a,
       coalesce(sum(payload), 0)::bigint AS pg_b,
       0::bigint AS pg_c
FROM (
    SELECT payload
    FROM pgd_p2_mixed_l
    WHERE k BETWEEN :low_key AND :high_key
    ORDER BY lid
    LIMIT :limit_rows
) AS filtered_rows \gset
SET LOCAL pg_orca.daft_enable = 'force';
SET LOCAL pg_orca.daft_prefer_plan = on;
SELECT count(*)::bigint AS daft_a,
       coalesce(sum(payload), 0)::bigint AS daft_b,
       0::bigint AS daft_c
FROM (
    SELECT payload
    FROM pgd_p2_mixed_l
    WHERE k BETWEEN :low_key AND :high_key
    ORDER BY lid
    LIMIT :limit_rows
) AS filtered_rows \gset
\elif :workload = 2
SET LOCAL pg_orca.daft_enable = 'off';
SELECT count(*)::bigint AS pg_a,
       coalesce(sum(l.payload::bigint), 0)::bigint AS pg_b,
       coalesce(sum(r.payload::bigint), 0)::bigint AS pg_c
FROM pgd_p2_mixed_l AS l
JOIN pgd_p2_mixed_r AS r ON l.k = r.k
WHERE l.k BETWEEN :low_key AND :high_key \gset
SET LOCAL pg_orca.daft_enable = 'force';
SET LOCAL pg_orca.daft_prefer_plan = on;
SELECT count(*)::bigint AS daft_a,
       coalesce(sum(l.payload::bigint), 0)::bigint AS daft_b,
       coalesce(sum(r.payload::bigint), 0)::bigint AS daft_c
FROM pgd_p2_mixed_l AS l
JOIN pgd_p2_mixed_r AS r ON l.k = r.k
WHERE l.k BETWEEN :low_key AND :high_key \gset
\elif :workload = 3
SET LOCAL pg_orca.daft_enable = 'off';
SELECT count(*)::bigint AS pg_a,
       coalesce(sum(group_rows), 0)::bigint AS pg_b,
       coalesce(sum(group_total), 0)::bigint AS pg_c
FROM (
    SELECT k, count(*)::bigint AS group_rows,
           sum(payload)::bigint AS group_total
    FROM pgd_p2_mixed_l
    WHERE k IS NOT NULL AND k BETWEEN :low_key AND :high_key
    GROUP BY k
) AS grouped_rows \gset
SET LOCAL pg_orca.daft_enable = 'force';
SET LOCAL pg_orca.daft_prefer_plan = on;
SELECT count(*)::bigint AS daft_a,
       coalesce(sum(group_rows), 0)::bigint AS daft_b,
       coalesce(sum(group_total), 0)::bigint AS daft_c
FROM (
    SELECT k, count(*)::bigint AS group_rows,
           sum(payload)::bigint AS group_total
    FROM pgd_p2_mixed_l
    WHERE k IS NOT NULL AND k BETWEEN :low_key AND :high_key
    GROUP BY k
) AS grouped_rows \gset
\else
SET LOCAL pg_orca.daft_enable = 'off';
SELECT count(*)::bigint AS pg_a,
       coalesce(sum(payload), 0)::bigint AS pg_b,
       coalesce(sum(k), 0)::bigint AS pg_c
FROM (
    SELECT k, payload
    FROM pgd_p2_mixed_l
    WHERE k IS NOT NULL
    ORDER BY k DESC NULLS LAST, lid ASC
    LIMIT :limit_rows OFFSET 17
) AS top_rows \gset
SET LOCAL pg_orca.daft_enable = 'force';
SET LOCAL pg_orca.daft_prefer_plan = on;
SELECT count(*)::bigint AS daft_a,
       coalesce(sum(payload), 0)::bigint AS daft_b,
       coalesce(sum(k), 0)::bigint AS daft_c
FROM (
    SELECT k, payload
    FROM pgd_p2_mixed_l
    WHERE k IS NOT NULL
    ORDER BY k DESC NULLS LAST, lid ASC
    LIMIT :limit_rows OFFSET 17
) AS top_rows \gset
\endif

SELECT pgd_p2_assert_equal(
    :workload,
    :pg_a::bigint, :pg_b::bigint, :pg_c::bigint,
    :daft_a::bigint, :daft_b::bigint, :daft_c::bigint);
COMMIT;
