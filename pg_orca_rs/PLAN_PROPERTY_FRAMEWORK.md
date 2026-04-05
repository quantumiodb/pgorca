# Property Framework 补全计划 (Physical Properties & Enforcer)

## 1. 背景与目标 (Background & Objective)
当前的 `pg_orca_rs` 中，Property Framework 的逻辑属性 (Logical Properties) 推导机制已经基本成型（尽管里面的基数估算公式仍需完善），但物理属性 (Physical Properties，即 Required/Delivered Properties) 的传递、校验和 Enforcer 插入机制严重缺失。

根据 `DESIGN.md` 第 8 节的规划，我们需要在搜索引擎 (`engine.rs`) 和属性模块 (`properties/`) 中补齐缺失的排序 (Ordering) 属性处理逻辑，使得优化器能够：
1.  正确理解 SQL 语句顶层的 `ORDER BY` 需求。
2.  在探索物理算子（如 `MergeJoin`、`Agg{Sorted}`）时，向其子节点传递正确的排序要求。
3.  自动识别子节点的输出是否满足排序要求，如果不满足，则自动插入 `Sort` 算子 (Enforcer) 并计算其代价。

## 2. 核心数据结构对齐 (Data Structures)

### 2.1 完善 `RequiredProperties` 和 `DeliveredProperties`
目前的定义仅包含 `ordering: Vec<SortKey>`，这是正确的起点。我们需要为其添加辅助方法，以便进行属性比较。

在 `optimizer_core/src/properties/required.rs` 和 `delivered.rs` 中：

```rust
impl DeliveredProperties {
    /// 检查当前提供的属性是否满足要求的属性
    pub fn satisfies(&self, required: &RequiredProperties) -> bool {
        if required.ordering.is_empty() {
            return true;
        }
        // 简单的前缀匹配：提供的排序键前缀必须完全等于要求的排序键
        if self.ordering.len() < required.ordering.len() {
            return false;
        }
        for (i, req_key) in required.ordering.iter().enumerate() {
            if self.ordering[i] != *req_key {
                return false;
            }
        }
        true
    }
}
```

## 3. 物理算子属性接口 (Operator Traits)

我们需要为 `PhysicalOp` 抽象出三个核心能力，对应 `DESIGN.md` 中的 8.5 和 8.6 节：

在 `optimizer_core/src/ir/physical.rs` 或一个新的 traits 模块中定义：

```rust
pub trait PhysicalPropertyProvider {
    /// 推导当前算子对第 i 个子节点的要求 (Required Properties)
    /// req_props: 当前节点上方（父节点）传递下来的要求
    fn derive_child_required(
        &self,
        child_idx: usize,
        req_props: &RequiredProperties,
    ) -> RequiredProperties;

    /// 当前算子在给定子节点的 Delivered Properties 下，自己能提供什么 (Delivered Properties)
    fn derive_delivered(
        &self,
        children_delivered: &[DeliveredProperties],
    ) -> DeliveredProperties;
}
```

### 3.1 `derive_child_required` 实现
根据 `DESIGN.md` 表 8.5 实现：
*   **`MergeJoin`**: 第 0 个子节点要求 `ordering(merge_keys_left)`，第 1 个子节点要求 `ordering(merge_keys_right)`。忽略父节点传下来的要求。
*   **`Agg{Sorted}`**: 子节点要求 `ordering(group_by_cols)`。
*   **`Sort`**: 子节点要求空（不需要排序），它会自己解决。
*   **`HashJoin` / `NestLoop` / `SeqScan` 等**: 原封不动地传递父节点的 `req_props`，或者直接返回空（如果不透传排序）。

### 3.2 `derive_delivered` 实现
根据 `DESIGN.md` 表 8.6 实现：
*   **`Sort`**: 无论子节点提供什么，自己总是提供自身的 `sort_keys`。
*   **`IndexScan` (BTree)**: 总是提供索引的前导列排序。
*   **`MergeJoin`**: 提供 `merge_keys` 的排序。
*   **`Limit` / `Unique`**: 原样透传子节点 (第 0 个) 提供的排序。
*   **`HashJoin` / `Agg{Hashed}` 等**: 破坏排序，返回 `DeliveredProperties::none()`。

## 4. 搜索引擎改造 (`engine.rs`)

这是整个补全计划中最关键的部分。修改 `optimize_group` 的第 5 步（计算物理算子代价并选择 Winner）。

```rust
// 在 explore 和 implement 之后，遍历该 Group 内的所有 Physical Expressions
let mut best_cost = upper_bound;
let mut best_winner = None;

for physical_expr in &group.physical_exprs {
    let mut accumulated_cost = local_cost(physical_expr.op, logical_props);
    let mut children_delivered = Vec::new();
    let mut is_feasible = true;

    // 1. 递归优化子节点，并获取子节点的 Delivered Properties
    for (i, child_gid) in physical_expr.children.iter().enumerate() {
        // 推导对当前子节点的要求
        let child_req = physical_expr.op.derive_child_required(i, required);
        
        let remaining_budget = best_cost - accumulated_cost;
        if remaining_budget <= 0.0 {
            is_feasible = false;
            break; // Branch and bound 剪枝
        }

        // 递归优化子节点
        if let Some(child_winner) = optimize_group(*child_gid, &child_req, remaining_budget, ...) {
            accumulated_cost += child_winner.cost.total;
            children_delivered.push(child_winner.delivered_props.clone());
        } else {
            is_feasible = false; // 子节点无法满足要求（或代价超标）
            break;
        }
    }

    if !is_feasible {
        continue;
    }

    // 2. 推导当前物理算子能提供的属性
    let delivered = physical_expr.op.derive_delivered(&children_delivered);

    // 3. 检查是否满足父节点的要求 (Enforcer 逻辑)
    let final_delivered;
    if !delivered.satisfies(required) {
        // 需要插入 Enforcer (目前主要是 Sort)
        let enforcer_cost = calculate_sort_enforcer_cost(required, logical_props, ...);
        accumulated_cost += enforcer_cost;
        
        if accumulated_cost > best_cost {
            continue; // 加上 Enforcer 后代价超标
        }
        // 加上 Sort 后，现在满足了要求
        final_delivered = DeliveredProperties { ordering: required.ordering.clone() };
    } else {
        final_delivered = delivered;
    }

    // 4. 更新当前 Group 的 Winner
    if accumulated_cost < best_cost {
        best_cost = accumulated_cost;
        best_winner = Some(Winner {
            expr_id: physical_expr.id,
            cost: Cost { startup: ..., total: best_cost },
            delivered_props: final_delivered,
            // 如果插入了 Enforcer，需要在此处记录一个标记，以便 Phase 3 提取计划时能实际插入 Sort 节点
            needs_enforcer: !delivered.satisfies(required), 
        });
    }
}
```

## 5. 计划提取阶段改造 (`plan/extract.rs`)

由于搜索引擎不会真正在 Memo 里凭空插入一个 `Sort` 物理表达式，它只是在代价上加上了 Enforcer，并在 `Winner` 中记录了 `needs_enforcer = true`。

因此，在 Phase 3 提取最终物理计划（`PhysicalPlan` 树）时，需要处理这个标记：

```rust
pub fn extract_plan(memo: &Memo, group_id: GroupId, required: &RequiredProperties) -> PhysicalPlan {
    let winner = memo.get_group(group_id).winners.get(required).unwrap();
    let expr = memo.get_expr(winner.expr_id);
    
    // 递归提取子节点
    let mut children = Vec::new();
    for (i, child_gid) in expr.children.iter().enumerate() {
        let child_req = expr.op.derive_child_required(i, required);
        children.push(extract_plan(memo, *child_gid, &child_req));
    }
    
    let mut plan = PhysicalPlan {
        op: expr.op.clone(),
        children,
        // ... 其他属性
    };
    
    // 如果 Winner 标记了需要 Enforcer，在这里显式包裹一层 Sort
    if winner.needs_enforcer {
        plan = PhysicalPlan {
            op: PhysicalOp::Sort { sort_keys: required.ordering.clone() },
            children: vec![plan],
            // ... 重新计算包裹后的 cost 和其他属性
        };
    }
    
    plan
}
```

## 6. 实施路线图

1.  **Phase 1: 基础设施 (Traits & Satisfies)**
    *   在 `properties/delivered.rs` 中实现 `satisfies`。
    *   在 `ir/physical.rs` (或新建模块) 中为各个物理算子实现 `derive_child_required` 和 `derive_delivered`。
2.  **Phase 2: 搜索引擎集成 (Engine)**
    *   修改 `engine.rs` 中的计算 Winner 的循环。
    *   实现一个简单的 `calculate_sort_enforcer_cost` 函数（复用现有的 `Sort` 代价模型）。
    *   扩展 `Winner` 结构体，增加 `needs_enforcer` 标志。
3.  **Phase 3: 计划提取 (Extract)**
    *   修改 `plan/extract.rs`，处理 `needs_enforcer`，动态组装 `PhysicalOp::Sort`。
4.  **Phase 4: 测试验证 (Tests)**
    *   编写纯 Rust 单元测试：构造一个要求排序的 `optimize_group` 调用，验证它是否正确地优先选择了 `IndexScan`（如果索引满足排序），或者回退到 `SeqScan + Sort` (Enforcer)。