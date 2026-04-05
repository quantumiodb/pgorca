# Property Framework 细化设计文档 (v0.3)

## 1. 背景与目标
在 `pg_orca_rs` v0.2 引入了 Task-based Scheduler 后，搜索框架的骨架已经彻底就绪。下一步的重点是补全完整的 **Property Framework (属性框架)**，包括逻辑层面的等价类与函数依赖，以及物理层面的排序 (Ordering) 要求传递、Enforcer (Sort) 自动插入。

我们的目标是：
1. **逻辑属性增强 (Logical Properties)**: 引入等价类 (Equivalence Classes) 和函数依赖 (Functional Dependencies)，提升代价估算准确性，并协助物理排序键的化简。
2. **物理属性传递 (Physical Properties)**: 实现 `RequiredProperties` (自顶向下传递) 和 `DeliveredProperties` (自底向上推导)。
3. **Task-based Scheduler 适配**: 在 `scheduler.rs` 的状态机中加入对 Enforcer (Sort 算子) 的惩罚代价值计算与记录。

---

## 2. 逻辑属性扩展：等价类与函数依赖

在 `optimizer_core/src/properties/logical.rs` 中，需要增强现有的 `LogicalProperties`：

```rust
use std::collections::HashSet;
use crate::ir::types::ColumnId;

#[derive(Debug, Clone)]
pub struct LogicalProperties {
    pub output_columns: Vec<ColumnId>,
    pub row_count: f64,
    pub table_ids: Vec<TableId>,
    pub not_null_columns: Vec<ColumnId>,
    
    // 新增：函数依赖 (Functional Dependencies)
    // 例如，主键 id 决定了表里的其他所有列 (id -> name, age)
    pub fd_keys: Vec<Vec<ColumnId>>, 
    
    // 新增：等价类 (Equivalence Classes)
    // 由 A = B AND B = C 谓词推导而来，[{A, B, C}]
    pub equivalence_classes: Vec<HashSet<ColumnId>>, 
    
    pub avg_width: f64,
}
```

### 2.1 物理属性化简 (Property Resolution)
有了等价类和函数依赖，我们在比较排序要求时就能做到非常智能。
例如，用户要求 `ORDER BY A, B`：
- 如果已知 `A = C` (等价类)，那么底层物理算子提供的 `ORDER BY C, B` 也能完美满足要求。
- 如果已知 `A` 是主键 (函数依赖 `A -> B`)，那么 `ORDER BY A` 的效果等同于 `ORDER BY A, B`，底层算子只需提供 `A` 的排序即可。

在 `DeliveredProperties` 的 `satisfies` 逻辑中，需传入当前的 `LogicalProperties` 辅助判断：

```rust
impl DeliveredProperties {
    pub fn satisfies(
        &self, 
        required: &RequiredProperties, 
        logical_props: &LogicalProperties
    ) -> bool {
        // 利用 logical_props.equivalence_classes 判断等价列
        // 利用 logical_props.fd_keys 处理排序后缀的截断
        // ... (省略具体化简逻辑)
        unimplemented!()
    }
}
```

---

## 3. 物理算子属性接口 (Operator Traits)

我们需要为所有物理算子实现一个标准的 Trait，以支持属性的上下双向流动。
在 `optimizer_core/src/ir/physical.rs` 或新模块 `properties/traits.rs` 中定义：

```rust
pub trait PhysicalPropertyProvider {
    /// 1. 推导当前算子对第 i 个子节点的物理要求 (Required Properties)
    /// req_props: 当前节点上方（父节点）传递下来的要求
    fn derive_child_required(
        &self,
        child_idx: usize,
        req_props: &RequiredProperties,
    ) -> RequiredProperties;

    /// 2. 推导当前算子能对外提供的物理属性 (Delivered Properties)
    /// children_delivered: 各个子节点实际提供的属性
    fn derive_delivered(
        &self,
        children_delivered: &[DeliveredProperties],
    ) -> DeliveredProperties;
}
```

**实现举例**:
* **`MergeJoin`**:
  * `derive_child_required`: 返回其自身 `merge_keys` 的排序要求（左子节点用左 Key，右子节点用右 Key），完全忽略父节点的 `req_props`。
  * `derive_delivered`: 声明自己提供了 `merge_keys` 的排序。
* **`Limit`**:
  * `derive_child_required`: 直接将父节点的排序要求透传给唯一的子节点。
  * `derive_delivered`: 直接透传子节点提供的排序。
* **`HashJoin`**:
  * `derive_child_required`: 返回空 (不需要子节点排序)。
  * `derive_delivered`: 破坏子节点排序，返回空。

---

## 4. Task-based Scheduler 改造适配

我们不再使用旧版的递归模式，而是在 v0.2 的状态机中插入属性推导和 Enforcer 机制。
主要修改 `optimizer_core/src/search/scheduler.rs` 中的 `execute_optimize_expr` 函数：

```rust
// 在 OptimizeExprState::WaitingForChild 完成所有子节点代价计算后的处理阶段：

// 1. 获取子节点实际 Deliver 的属性
let children_delivered: Vec<DeliveredProperties> = child_gids.iter().map(|gid| {
    // 从子 group 的 winner 中读取其 delivered props
    memo.get_group(*gid).winners.get(&child_req_key).unwrap().delivered_props.clone()
}).collect();

// 2. 计算当前算子对外 Deliver 的属性
let delivered = phys_op.derive_delivered(&children_delivered);

// 3. 检查是否满足父 Group 传递进来的要求
let logical_props = memo.get_group(group_id).logical_props.get().unwrap();
let mut needs_enforcer = false;
let mut final_delivered = delivered.clone();

if !delivered.satisfies(&required, logical_props) {
    // 需要插入 Enforcer (Sort 算子)
    needs_enforcer = true;
    let enforcer_cost = calculate_sort_enforcer_cost(&required, logical_props, ...);
    accumulated_cost += enforcer_cost;
    
    if accumulated_cost > upper_bound {
        return; // 剪枝：加上 Sort 的代价后超出了预算
    }
    // 插入 Sort 后，满足了全部要求
    final_delivered = DeliveredProperties { ordering: required.ordering.clone() };
}

// 4. 将结果写入 Winner
if accumulated_cost < upper_bound {
    // ... 更新 current_best ...
    memo.get_group_mut(group_id).winners.insert(
        RequiredPropsKey::from(&required),
        Winner {
            expr_id,
            cost: Cost { total: accumulated_cost, ... },
            delivered_props: final_delivered,
            needs_enforcer, // 必须在 Winner 结构体中新增此字段！
        },
    );
}
```

---

## 5. Phase 3 物理计划提取 (Plan Extraction)

由于我们的 Enforcer 只是加了代价值，并没有真正在 Memo 里凭空插入一个 Group，所以在 Phase 3 `plan/extract.rs` 中组装最终物理树时，需要动态把这个 `Sort` 节点补回来。

```rust
pub fn extract_plan(memo: &Memo, group_id: GroupId, required: &RequiredProperties) -> PhysicalPlan {
    let winner = memo.get_group(group_id).winners.get(&RequiredPropsKey::from(required)).unwrap();
    let expr = memo.get_expr(winner.expr_id);
    let phys_op = ... // as PhysicalOp
    
    // 递归提取子节点
    let mut children = Vec::new();
    for (i, child_gid) in expr.children.iter().enumerate() {
        let child_req = phys_op.derive_child_required(i, required);
        children.push(extract_plan(memo, *child_gid, &child_req));
    }
    
    let mut plan = PhysicalPlan {
        op: phys_op.clone(),
        children,
        // ...
    };
    
    // 核心修改：如果 Winner 标记了需要 Enforcer，包装一层 Sort
    if winner.needs_enforcer {
        plan = PhysicalPlan {
            op: PhysicalOp::Sort { sort_keys: required.ordering.clone() },
            children: vec![plan],
            // ... 重算包裹后的信息
        };
    }
    
    plan
}
```

## 6. 实施优先级
1. **基础 Trait**: 先定义 `derive_child_required` 和 `derive_delivered`，并在几个核心算子 (`SeqScan`, `IndexScan`, `Sort`, `HashJoin`, `MergeJoin`) 上实现。
2. **Scheduler 接入**: 在 `scheduler.rs` 引入 `needs_enforcer` 逻辑和最基本的代价惩罚。修改 `Winner` 定义。
3. **计划装配**: 修改 `extract_plan`。并编写一个纯 Rust 单元测试，强制传入一个 `ORDER BY` 要求，断言生成的 `PhysicalPlan` 树顶端包含了一个 `Sort` 节点。
4. **高级推导 (后续里程碑)**: 最后再慢慢实现基于等价类和函数依赖的属性化简逻辑 (`satisfies`)。