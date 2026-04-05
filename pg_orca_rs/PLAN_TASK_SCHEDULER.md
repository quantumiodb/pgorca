# Task-based Scheduler 架构设计 (v0.2)

## 1. 背景与动机
在 v0.1 版本中，搜索引擎（`engine.rs`）采用的是直接的递归调用 (`optimize_group` 调用 `optimize_expr`，再递归调用子 `optimize_group`)。
虽然实现简单且符合直觉，但纯递归模型存在几个严重的局限性：
1. **栈溢出风险 (Stack Overflow)**：对于极其复杂的查询（如包含大量 Join 和冗长逻辑链的 TPC-DS 查询），深层递归极易击穿操作系统默认的线程栈大小。
2. **缺乏调度灵活性**：无法在运行中途随时暂停或恢复。也难以根据代价或启发式规则动态调整优先级（例如：优先 Explore 某些看似高回报的 Group）。
3. **阻碍并行化演进**：递归的状态强绑定在操作系统的调用栈上，很难平滑过渡到未来的多线程（如使用 Rayon）并行搜索架构。

为此，在 v0.2 版本中，我们引入 **显式任务栈 (Explicit Task Stack)** 机制。借鉴 `optd` 和 `GPORCA` 的成熟设计，将控制流倒置（Inversion of Control），用显式的状态机/事件循环取代深度递归。

## 2. 核心数据结构：任务枚举 (Task Enum)

我们不再使用互相调用的函数链，而是定义一组离散的 `Task` 枚举。调度器维护一个栈（或优先队列），按需压入和弹出任务。

```rust
// optimizer_core/src/search/task.rs

use crate::ir::types::{GroupId, ExprId, RuleId};
use crate::properties::required::RequiredProperties;

/// 优化器任务类型
#[derive(Debug, Clone)]
pub enum Task {
    /// 优化整个 Group：寻找满足 req_props 的最低代价物理计划
    OptimizeGroup {
        group_id: GroupId,
        req_props: RequiredProperties,
        upper_bound: f64,
        state: OptimizeGroupState, // 追踪任务执行阶段
    },
    
    /// 探索 Group：触发该 Group 内所有逻辑表达式的探索
    ExploreGroup {
        group_id: GroupId,
        state: ExploreGroupState,
    },
    
    /// 探索表达式：要求先探索其所有的子 Group，然后触发逻辑变换规则 (xform)
    ExploreExpr {
        expr_id: ExprId,
        state: ExploreExprState,
    },
    
    /// 实现 Group：触发对该 Group 内所有逻辑表达式的物理实现
    ImplementGroup {
        group_id: GroupId,
        state: ImplementGroupState,
    },
    
    /// 实现表达式：应用物理实现规则 (impl rules)
    ImplementExpr {
        expr_id: ExprId,
    },
    
    /// 优化物理表达式：计算自身代价，推导对子节点的要求，下发 OptimizeGroup 给各个子节点
    OptimizeExpr {
        expr_id: ExprId,
        req_props: RequiredProperties,
        upper_bound: f64,
        state: OptimizeExprState,
    },
    
    /// 应用特定规则：对指定表达式执行给定的规则，并将生成的新表达式插入 Memo
    ApplyRule {
        rule_id: RuleId, 
        expr_id: ExprId,
        is_explore: bool, // true: xform rule, false: impl rule
    },
}
```

## 3. 状态驱动 (Stateful Tasks) 解决依赖问题

在递归模型中，"等待子任务完成" 是自动的（当前函数阻塞）。在显式任务栈中，由于所有任务都在同一个循环中执行，我们需要一种机制让父任务能在子任务完成后继续执行。
做法是：**为复杂任务引入状态 (State)，执行后修改状态并重新压栈。**

以 `OptimizeGroup` 为例：

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptimizeGroupState {
    /// 初始状态：刚进入优化，需要触发 Explore 和 Implement
    Init,          
    /// 正在优化表达式阶段：Memo 中已生成所有 physical exprs，开始计算代价
    OptimizingExprs, 
}
```

执行逻辑流转：
1. `Task::OptimizeGroup(Init)` 被弹出。
2. 调度器将其状态改为 `OptimizingExprs`，然后**重新压入栈中**。
3. 调度器紧接着将 `ImplementGroup` 和 `ExploreGroup` 压入栈顶。
4. 由于栈是 LIFO（后进先出），`ExploreGroup` 会先被执行，不断裂变出新规则。接着 `ImplementGroup` 执行。
5. 当 Explore 和 Implement 的所有子任务都弹空后，栈顶再次露出 `OptimizeGroup(OptimizingExprs)`。此时，该 Group 内的所有候选物理表达式都已就绪。
6. `OptimizeGroup(OptimizingExprs)` 遍历所有的 Physical Exprs，为它们创建 `OptimizeExpr` 任务并压栈，自己则功成身退。

## 4. 执行引擎：事件循环 (Task Scheduler)

搜索引擎的主体将变为一个极其紧凑的 `while` 循环。

```rust
// optimizer_core/src/search/scheduler.rs

pub struct Scheduler {
    task_stack: Vec<Task>,
}

impl Scheduler {
    pub fn new() -> Self {
        Self { task_stack: Vec::new() }
    }

    pub fn schedule(&mut self, task: Task) {
        self.task_stack.push(task);
    }

    pub fn run(&mut self, memo: &mut Memo, catalog: &CatalogSnapshot) -> Result<(), OptimizerError> {
        while let Some(task) = self.task_stack.pop() {
            // 在事件循环顶层统一处理超时、中断信号
            if self.is_timeout() {
                return Err(OptimizerError::Timeout);
            }
            
            match task {
                Task::OptimizeGroup { group_id, req_props, upper_bound, state } => {
                    self.execute_optimize_group(group_id, req_props, upper_bound, state, memo);
                }
                Task::ExploreGroup { group_id, state } => {
                    self.execute_explore_group(group_id, state, memo);
                }
                // ... 分发给对应的处理函数
                Task::ApplyRule { rule_id, expr_id, is_explore } => {
                    self.execute_apply_rule(rule_id, expr_id, is_explore, memo);
                }
                _ => unimplemented!(),
            }
        }
        Ok(())
    }
}
```

## 5. 优势总结

1. **绝对的栈安全 (Stack Safety)**：不管查询语法树多深，Rust 层面永远只有一个 `while` 循环在跑，调用栈深度固定为常数级。所有的上下文保存在堆 (Heap) 上分配的 `Vec<Task>` 中，彻底消除 Stack Overflow 隐患。
2. **清晰的调试链路**：整个搜索过程被展平为一维线性的 Task 序列。遇到 bug 时，只需打印出当前 `task_stack` 的内容，就能清晰看到优化器 "正打算做什么" 以及 "正在等什么"。
3. **支持提前终止与细粒度剪枝**：如果在某个时刻 `upper_bound` 预算已经耗尽，我们可以直接清理掉栈中相关的待执行任务，而不需要在每层递归中传递和检查返回值。
4. **迈向并发计算的基石**：一旦我们将 `Vec<Task>` 替换为无锁并发队列（如 `crossbeam-deque`），再启动一组 Worker 线程去抢任务，这个设计就能顺理成章地平滑升级为真正的并行 Cascades 优化器（完全对标 GPORCA）。

## 6. 深度解析：依赖处理与并发演进 (参考 TensorFlow 设计)

### 6.1 依赖处理的进化路径
在 v0.2 版本中，我们通过 **LIFO (后进先出) 栈序** 模拟了递归依赖：父任务先改状态压栈，子任务再压栈覆盖其上。父任务被“唤醒”时，子任务的结果已写入 Memo。

但在并发模式下（v0.4+），LIFO 将失效。我们将借鉴 **TensorFlow Executor** 和 **GPORCA** 的核心设计：

*   **依赖计数器 (Pending Dependencies)**: 每个 `Task` 维护一个 `AtomicUsize`。父任务在派生子任务时，初始化计数器。
*   **反向通知 (Signal & Wakeup)**: 子任务执行完毕后，调用 `parent.decrement_pending()`。
*   **零等候调度**: 只有计数器归零的任务才会进入 **Ready Queue (就绪队列)**。这彻底解决了“父线程死等子线程”的问题，极大提升了多核利用率。

### 6.2 负载均衡 (Work Stealing)
得益于任务的原子化，未来可以引入类似 `Rayon` 或 `Tokio` 的工作窃取算法。不同 CPU 核心可以并行探索 Memo 图的不同分支，通过这种数据流驱动 (Dataflow-driven) 的方式，`pg_orca_rs` 将具备处理超大规模复杂查询（如几百张表的自动 Join Reorder）的潜力。

## 7. 实施路径 (Implementation Path)

1. **Step 1: 定义 Tasks 与 States**: 在 `search/task.rs` 中定义好完整的 Enum 和状态流转标识。
2. **Step 2: 构建 Scheduler**: 创建 `Scheduler` 结构体，替换掉现有的递归 `optimize_group` 函数。
3. **Step 3: 移植逻辑**: 将原先递归中 `for` 循环和规则调用的逻辑，拆解塞入各个 `execute_xxx` 的状态机分支中。
4. **Step 4: 测试回归**: 运行 `tests/` 目录下现有的纯 Rust unit tests。Task 机制的引入改变的是执行次序和存储结构，不应该改变任何代价和生成计划的结果。
