# 代价模型剩余工作 (Cost Model TODO)

基于 `PLAN_COST_MODEL.md` 的完整规划，Phase 2 (optimizer_core) 侧已全部完成。  
以下是 Phase 1 (pg_bridge) 侧及集成测试的剩余工作。

---

## 1. pg_bridge 读取 MCV 和直方图

**对应章节**: PLAN_COST_MODEL.md §4.2  
**文件**: `pg_bridge/src/inbound/from_expr.rs` — `read_column_stats()`

当前 `read_column_stats()` 只读了 `stanullfrac` 和 `stadistinct`，新增的 `histogram_bounds` 和 `most_common_vals` 字段始终为 `None`。

### 需要做的事

- [ ] 解析 `pg_statistic` 中的 `stakindN`（N=1..5）数组，定位 `STATISTIC_KIND_MCV` 和 `STATISTIC_KIND_HISTOGRAM` 所在的槽位
- [ ] 读取 `stanumbersN`（`float4` 数组）获取 MCV 的频率值，组装为 `Vec<McvEntry>`
- [ ] 读取 `stavaluesN`（Datum 数组）获取 MCV 的值和直方图边界值
  - 需要根据列的类型 OID 将 Datum 转换为 `ConstValue`（Int32/Int64/Float64/Text 等）
- [ ] 将结果填入 `ColumnStats.most_common_vals` 和 `ColumnStats.histogram_bounds`
- [ ] 同时读取 `correlation`（`stanumbersN` 中 stakind=CORRELATION 的槽位），当前硬编码为 0.0

### 注意事项

- `pg_statistic` 的数组是 PG 内部 Datum 格式，需要用 `pg_sys::SysCacheGetAttr` + `pg_sys::deconstruct_array` 来解码
- 需要处理 `stainherit` 参数（`SearchSysCache3` 的第三个 key）
- 解码失败时应 graceful fallback 为 `None`，不应 panic

---

## 2. 注册自定义 GUC（阻尼因子）

**对应章节**: PLAN_COST_MODEL.md §4.1  
**文件**: `pg_bridge/src/lib.rs`

当前 `CostModel` 的三个阻尼因子使用默认值 0.75，DBA 无法调优。

### 需要做的事

- [ ] 在 `pg_bridge/src/lib.rs` 中使用 pgrx 的 `GucRegistry::define_float_guc` 注册：
  - `orca.damping_factor_filter`（默认 0.75）
  - `orca.damping_factor_join`（默认 0.75）
  - `orca.damping_factor_groupby`（默认 0.75）
- [ ] 在 `from_expr.rs` 构建 `CostModel` 时读取这些 GUC 的值，替代 `..CostModel::default()`

---

## 3. 端到端集成测试

**对应章节**: PLAN_COST_MODEL.md §7.5  
**文件**: `pg_bridge/tests/integration.rs`

当前新增的测试均为纯 Rust 单元测试（手工构造 MCV/直方图）。需要验证完整链路。

### 需要做的事

- [ ] 新增集成测试用例，验证 PG 统计信息 → pg_bridge 抓取 → optimizer_core 使用 的完整链路：
  1. 建表并插入有倾斜分布的数据（例如某列 80% 的值为同一个值）
  2. 执行 `ANALYZE` 让 PG 生成统计信息
  3. 执行带等值谓词的查询，验证优化器利用了 MCV 信息
  4. 执行带范围谓词的查询，验证优化器利用了直方图信息
- [ ] 验证多条件 AND 的阻尼效果：对比两个等价查询的代价估算是否合理
- [ ] 验证自定义 GUC 生效：`SET orca.damping_factor_filter = 1.0` 后应退化为朴素乘积
