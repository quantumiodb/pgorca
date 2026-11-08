-- =====================================================================
-- daft_fallback.sql  (P0-008-F)
-- With daft_enable=off the planner must produce a normal PG plan and never
-- patch the final tree.  Auto/no-profile and single-retry policy are covered
-- by daft_explain and the final-validator selftest respectively.
-- =====================================================================

SET pg_orca.enable_orca = on;
SET pg_orca.daft_enable = 'off';

CREATE TABLE daft_fb_t(a int);
INSERT INTO daft_fb_t VALUES (1),(2),(3),(4),(5);

SELECT count(*) AS cnt, min(a) AS mn, max(a) AS mx FROM daft_fb_t;
SELECT a FROM daft_fb_t WHERE a >= 3 ORDER BY a;
-- end of daft_fallback
