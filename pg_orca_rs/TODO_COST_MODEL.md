# 代价模型剩余工作 (Cost Model TODO)

基于 `PLAN_COST_MODEL.md` 的完整规划，Phase 2 (optimizer_core) 和 Phase 1 (pg_bridge) 核心实现已完成。  
以下是剩余的集成测试工作。

---

## ~~1. pg_bridge 读取 MCV 和直方图~~ ✅ 已完成

`read_column_stats()` 已扩展为读取完整 pg_statistic 信息：
- 扫描 `stakind1..5` 定位 MCV、HISTOGRAM、CORRELATION 槽位
- 通过 `deconstruct_array_builtin` 解码 `stanumbersN`（float4[]）和 `stavaluesN`（Datum[]）
- 使用 `datum_to_const_value()` 按列类型 OID 转换为 `ConstValue`
- 填充 `ColumnStats.most_common_vals`、`histogram_bounds`、`correlation`

---

## ~~2. 注册自定义 GUC（阻尼因子）~~ ✅ 已完成

在 `lib.rs` 中注册了三个 GUC：
- `orca.damping_factor_filter`（默认 0.75）
- `orca.damping_factor_join`（默认 0.75）
- `orca.damping_factor_groupby`（默认 0.75）

`from_expr.rs` 构建 `CostModel` 时已读取这些 GUC 值。

---

## ~~3. 端到端集成测试~~ ✅ 已完成

在 `pg_bridge/tests/integration.rs` 中新增三个集成测试：

- `cost_model_mcv_equality` — 倾斜表（80% val=1），验证 MCV 高频值的行数估算远大于罕见值
- `cost_model_histogram_range` — 均匀分布表，验证 `id < 1000`（10%）vs `id < 5000`（50%）的行数估算差异
- `cost_model_damping_guc` — 双列 AND 谓词，验证 `SET orca.damping_factor_filter = 0.75` 比 `1.0` 估算更多行

运行：`cargo test -p pg_bridge --test integration -- --test-threads=1`  
（需要先 `cargo pgrx install --release` 安装 pg_bridge.so）
