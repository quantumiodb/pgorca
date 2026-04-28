# HashJoin DPE 实现状态

## 当前状态：ORCA 优化阶段崩溃

实现基于 `docs/hashjoin-dpe-design.md`，已完成代码编写，但运行时 ORCA 优化器在处理包含分区表的 HashJoin 查询时崩溃（postgres backend SIGFAULT）。

复现命令：
```sql
SET pg_orca.enable_orca = on;
EXPLAIN SELECT * FROM prt1 JOIN t2 ON prt1.a = t2.a;
```

---

## 已完成的代码改动

| 文件 | 改动内容 |
|------|----------|
| `libgpopt/src/xforms/CXformDynamicGet2DynamicTableScan.cpp` | `Exfp()` 从 `ExfpNone` 改为 `ExfpHigh`，启用 xform |
| `gpopt/config/CConfigParamMapping.cpp` | `enable_dynamic_tablescan` 从 `false` 改为 `true` |
| `libgpopt/src/operators/CPhysicalHashJoin.cpp` | 实现 `PppsRequiredForJoins()` DPE 逻辑：对 outer 的 partition consumer + join 谓词 → 在 inner child 上 require `EpptPropagator` |
| `gpopt/translate/CTranslatorDXLToPlStmt.cpp` | `TranslateDXLPartSelector` non-Append 分支 → 生成 `PartitionSelectorCS` CustomScan；`TranslateDXLDynTblScan` → 生成 `DynamicTableScanCS` CustomScan |
| `compat/executor/dyn_scan.c` / `.h` | 两个 CustomScan 节点的完整执行逻辑（新增文件） |
| `pg_orca.cpp` | `_PG_init` 中调用 `RegisterDynScanCustomScanMethods()` |
| `CMakeLists.txt` | 添加 `compat/executor/dyn_scan.c` |

### 临时 debug 代码（待清理）

以下文件含有 `fopen("/tmp/dpe_debug.log", ...)` 的 fprintf 调试代码，修复后需删除：
- `libgpopt/src/engine/CEngine.cpp` — Optimize() / RecursiveOptimize() 入口
- `libgpopt/src/search/CJobGroupImplementation.cpp` — FScheduleGroupExpressions()
- `libgpopt/src/search/CJobGroupExpressionImplementation.cpp` — ScheduleApplicableTransformations()
- `pg_orca.cpp` — _PG_init()

---

## 已排除的问题

### 1. Xform 未触发 ✅ 已排除

最初怀疑 `CXformDynamicGet2DynamicTableScan` 被 search stage 过滤掉。通过 debug trace 证明 **xform 正常触发**：

```
op_eopid=38 candidates=3 after_impl=2 stage_size=150 after_stage=2
```

- eopid=38 = `CLogicalDynamicGet`
- candidates=3 表示 `PxfsCandidates()` 返回 3 个候选 xform
- after_impl=2 表示经过 `∩ PxfsImplementation()` 后剩 2 个（`ExfDynamicGet2DynamicTableScan` 和 `ExfDynamicGet2AppendTableScan`）
- after_stage=2 表示 `∩ PxfsCurrentStage()` 没有额外过滤

### 2. Search stage 过滤实现 xform ✅ 已排除

`PdrgpssDefault()` 创建的默认 search stage 使用 `PxfsExploration()`，其中包含 ~150 个 xform（包括 implementation xform），`stage_size=150` 证实了这一点。

### 3. shared_preload_libraries 导致代码未更新 ✅ 已解决

`postgresql.conf` 中配置了 `shared_preload_libraries = 'pg_orca'`，.so 在 server 启动时加载。后续 `LOAD 'pg_orca'` 是 no-op。**必须 `pg_ctl restart` 才能加载新代码**。

---

## 当前问题：优化阶段崩溃

### 现象

debug log 输出以下内容后 backend 崩溃（server closed the connection unexpectedly）：

```
[CEngine::Optimize] called
[FScheduleGroupExpressions] pgexpr=0x931793aa8
[FScheduleGroupExpressions] pgexpr=0x9317916b0
[FScheduleGroupExpressions] pgexpr=0x931792c38
op_eopid=38 candidates=3 after_impl=2 stage_size=150 after_stage=2   ← DynamicGet
[FScheduleGroupExpressions] pgexpr=0x9317aced0
[FScheduleGroupExpressions] pgexpr=0x931790678
op_eopid=0  candidates=1 after_impl=1 stage_size=150 after_stage=1   ← LogicalGet(t2)
[FScheduleGroupExpressions] pgexpr=0x931dae738
op_eopid=11 candidates=11 after_impl=1 stage_size=150 after_stage=1  ← InnerJoin
op_eopid=11 candidates=11 after_impl=1 stage_size=150 after_stage=1  ← InnerJoin(另一个)
op_eopid=12 candidates=6 after_impl=0 stage_size=150 after_stage=0   ← NAryJoin(正常，无实现xform)
[FScheduleGroupExpressions] pgexpr=0x931db29b0
```

注意：`[CEngine::RecursiveOptimize]` 的 trace **未出现**，说明崩溃发生在 implementation 阶段完成之后、RecursiveOptimize 开始之前，即 `CEngine::Implement()` 的某个后续步骤中。

### 可能的崩溃原因

根据 ORCA 优化流程，implementation 之后的步骤是 **property derivation / enforcement**。崩溃最可能发生在以下位置：

#### 假设 A：`CPhysicalDynamicTableScan` 的 property 方法

`CPhysicalDynamicTableScan` 是一个存在于 vendored ORCA 代码中的物理算子。它的 `PppsDerive()` 会产生 `EpptConsumer`，依赖上层 join 提供匹配的 `EpptPropagator`。可能的问题：
- `PppsDerive()` 中引用了不存在的 partition info
- 某些 property 方法（如 `PcrsRequired`, `PosDerive`）对 DynamicTableScan 有特殊处理，在单节点模式下可能触发未预期的代码路径

#### 假设 B：`CPhysicalHashJoin::PppsRequiredForJoins()` 新代码

新增的 DPE propagation 逻辑可能有问题：
- `exprhdl.DerivePartitionInfo(0)` 可能在某些 group expression 上返回 null
- `PexprExtractPredicatesOnPartKeys()` 可能在某些谓词结构上崩溃
- `pps_result->Insert()` 的参数可能不正确（如 `rel_mdid` 的引用计数问题）
- `InsertAllowedConsumers()` 的使用方式可能不正确

#### 假设 C：AppendEnforcers / PartitionSelector 插入

ORCA 的 enforcer 机制在看到 inner child 需要 `EpptPropagator` 时，会尝试插入 `CPhysicalPartitionSelector`。这个过程中可能：
- PartitionSelector 的构造依赖了某些在单节点模式下不存在的信息
- enforcer 和 property resolution 之间的交互导致无限递归或空指针

### 调试困难

- **无 core dump**：macOS 默认不生成 core dump（`/cores/` 为空）
- **LLDB 不可用**：因为 OBJECT library 编译方式，LLDB 报大量 "debug map object file changed" 错误，无法有效设置断点
- **无 PG log**：logging_collector 关闭，stderr 输出未被捕获

---

## 下一步排查计划

### 1. 获取崩溃堆栈（最优先）

方案 A — 启用 core dump：
```bash
ulimit -c unlimited
sudo sysctl kern.corefile=/tmp/core.%P
# 重启 postgres，复现崩溃，用 lldb /path/to/postgres /tmp/core.XXX 分析
```

方案 B — 启用 PG logging 捕获 backtrace：
```
# postgresql.conf
logging_collector = on
log_directory = '/tmp/pg_log'
```

方案 C — 在关键路径添加更多 fprintf trace 缩小范围：
- `CEngine::Implement()` 结束后
- `CJobGroupOptimization` 中
- `CPhysicalDynamicTableScan::PppsDerive()`
- `CPhysicalHashJoin::PppsRequiredForJoins()` 各分支

### 2. 修复崩溃后的验证

1. `EXPLAIN` 输出应显示：
   ```
   HashJoin
     -> Custom Scan (DynamicTableScanCS) on prt1
     -> Hash
          -> Custom Scan (PartitionSelectorCS)
               -> Seq Scan on t2
   ```

2. `EXPLAIN ANALYZE` 应显示实际只扫描了匹配的分区

3. 不含 join 的查询（`SELECT * FROM prt1 WHERE a > 10`）应 fallback 到 AppendTableScan（DTS 被 property enforcement 淘汰）

### 3. 清理

- 删除所有 `/tmp/dpe_debug.log` 相关的 fprintf 调试代码
- 确认 NLJ DPE 路径仍然工作正常

---

## 环境信息

- PostgreSQL data dir: `/tmp/pgd3`
- Port: `15434`
- `shared_preload_libraries = 'pg_orca'`（需 restart 加载新代码）
- Build: `cd build && ninja -j$(nproc) && ninja install && pg_ctl -D /tmp/pgd3 restart`
- psql: `/Users/jianghua/code/postgresql/src/bin/psql/psql -p 15434 -d postgres`
