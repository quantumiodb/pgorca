# TODO

## ORCA 支持 pg_statistic_ext (Extended Statistics)

### 现状

ORCA 已实现 functional dependencies (`d`) 和 ndistinct (`f`)。  
MCV lists (`m`) 和 expression statistics (`e`) 未实现。

| 统计类型 | 读取 | 使用 | 状态 |
|----------|------|------|------|
| Functional Dependencies (`d`) | ✅ | ✅ `CExtendedStatsProcessor` | 已完成 |
| N-Distinct (`f`) | ✅ | ✅ `CStatisticsUtils` GROUP BY 估算 | 已完成 |
| MCV Lists (`m`) | ❌ info 注册但数据未加载 | ❌ | 未实现 |
| Expression Stats (`e`) | ❌ 显式跳过 (`CTranslatorRelcacheToDXL.cpp:485`) | ❌ | 未实现 |

### Phase 1 — MCV Lists

解决纯列组合的选择率估算偏差（如 `WHERE a=1 AND b=2`）。

1. **gpdbwrappers**: 添加 `GetMVMCVList(OID)` 调用 PG `statext_mcv_load()`
2. **CDXLExtStats**: 添加 MCV 数据成员（多列值组合 + frequency + base_frequency）
3. **RetrieveExtStats**: 加载 `STATS_EXT_MCV` 数据
4. **CExtendedStatsProcessor**: 移植 PG 的 `mcv_clauselist_selectivity` + `mcv_combine_selectivities`
5. **CFilterStatsProcessor**: 在 dependency 之前先调用 MCV 估算

### Phase 2 — Expression Statistics

解决表达式谓词估算（如 `WHERE (CASE a WHEN 1 THEN true ELSE false END) AND b=2`）。

1. **RetrieveExtStatsInfo**: 去掉 `EstatExpr` 的 `continue` 跳过，解析表达式列表
2. **gpdbwrappers**: 添加加载 `stxdexpr`（pg_statistic 数组）的函数
3. **表达式匹配**: 将查询中的表达式与统计对象中的表达式做 `equal()` 匹配
   - 难点：ORCA 已将 Query 翻译成 DXL，原始 PG 表达式不直接可用
   - 方案：在翻译前（PG 侧）做匹配并标记，或保存原始表达式用于后续比较
4. **统计集成**: 匹配成功后将表达式统计作为虚拟列的直方图/MCV 使用

### 关联测试失败

- `stats_ext.out`: `check_estimated_rows` 估算偏差 (需要 Phase 2)


## Current Status (Phase 3)
- [ ] partitioned tables
- [ ] Parallel query support
- [ ] Bitmap scan cost model
- [ ] Doubly-correlated EXISTS/NOT EXISTS: ORCA generates O(N³) nested SubPlan
      when inner subquery references outer-outer column (skip-level correlation).
      Fix: detect m_fHasSkipLevelCorrelations in FRemoveExistentialSubquery and
      fall back to standard_planner. Cloudberry works around this by explicitly
      disabling the optimizer for such queries in tests.