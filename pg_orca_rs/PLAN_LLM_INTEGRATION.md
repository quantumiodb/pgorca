# LLM 与查询优化器 (pg_orca_rs) 深度结合设计文档

## 1. 核心理念 (Vision)
在数据库领域，传统优化器（如 PostgreSQL 的 Bottom-Up 动态规划和我们实现的 GPORCA Cascades）主要依赖**硬编码的启发式规则 (Heuristics)** 和 **基于统计信息 (pg_statistic) 的数学代价模型**。

LLM（大语言模型）及其衍生的深度学习技术的引入，并不是要替换掉严谨的 `Search Engine`，而是作为**“智能外脑”**，在优化器无法精确计算的领域提供高阶指导。

得益于我们在 v0.2 版本中引入的 **Task-based Scheduler** 和原本的 **Phase 1-3 隔离架构**，`pg_orca_rs` 已经具备了无缝接入 LLM 的绝佳土壤。

---

## 2. 四大结合场景 (Use Cases)

### 场景 A: 复杂谓词的基数估算 (NL-Cardinality Estimation)
* **痛点**: 对于 `LIKE '%keyword%'`、复杂的正则表达式，或者多列之间的隐含语义关联（如 `WHERE city = 'Seattle' AND state = 'Washington'`），传统的直方图和 MCV 完全失效，优化器只能猜一个魔法数字（比如默认 1/3）。
* **结合方式**: 
  * 在 **Phase 1 (Inbound)** 阶段，如果检测到复杂的文本或非等值表达式，可以通过轻量级本地 LLM (或 Embedding 模型) 获取语义层面的选择率 (Selectivity) 预测。
  * 将预测结果缓存并写入 `CatalogSnapshot`，然后传递给无状态的 `optimizer_core`。

### 场景 B: 智能查询重写 (Semantic Query Rewrite)
* **痛点**: 开发者写出的复杂 SQL（深层嵌套子查询、冗余的 LEFT JOIN、无意义的 DISTINCT）常常让优化器搜索空间爆炸。
* **结合方式**: 
  * 在进入 Cascades 搜索前的 `simplify_pass` 阶段。
  * 将原始查询转化为标准树结构发给 LLM 代理。LLM 利用其丰富的 SQL 模式识别能力，直接识别出反模式 (Anti-patterns) 并返回一个逻辑等价但在物理层面上更容易执行的简化 AST。

### 场景 C: 搜索空间引导 (LLM-Guided Task Scheduler)
* **痛点**: Cascades 优化器在面对超多表 Join（>10 张表）时，虽然有 Branch-and-bound 剪枝，但穷举空间仍然过于庞大，极易触发 `TIMEOUT` 导致退化。
* **结合方式**: 
  * 我们刚刚重构的 `Scheduler` 拥有一个 `task_stack`。目前它是单纯的后进先出 (LIFO)。
  * 我们可以将它升级为 **Priority Queue (优先队列)**。利用在 LLM 上微调出来的策略网络 (Policy Network) 或直接询问 LLM，为栈里的 `OptimizeGroup` 或 `ExploreGroup` 任务进行**启发式打分 (Scoring)**。
  * LLM 预测哪条探索路径（比如优先把表 A 和表 B Join 起来）最有希望迅速降低 `upper_bound`，优化器就优先执行哪个任务，这能将搜索速度提升数个数量级。

### 场景 D: 交互式调优与执行计划解释 (Interactive EXPLAIN & Advisor)
* **痛点**: 传统的 `EXPLAIN` 输出的是冷冰冰的算子树和 cost，DBA 很难直观理解“为什么优化器选了这个又慢又蠢的计划”。
* **结合方式**: 
  * 利用 **Phase 3 (Outbound)** 的 Hook。
  * 优化器不仅输出最终胜出的 `PhysicalPlan`，还可以把完整的 `Memo` 图（记录了哪些计划被淘汰、因为缺少排序被淘汰、或者因为哪一步的 cost 突然变大被淘汰）输出给 LLM。
  * LLM 自动分析出人类可读的报告，例如：*"优化器之所以选择全表扫描而不是索引，是因为第 3 步基数估算认为这会返回 90% 的数据。如果您想走 IndexScan，建议您在 `orders.status` 上建立一个索引，或者调小 `random_page_cost` 参数。"*

---

## 3. 架构设计：如何安全、高效地接入？

LLM 的响应时间（百毫秒至秒级）与优化器（微秒至毫秒级）存在巨大鸿沟。因此，不能在核心优化路径中做同步的 HTTP 等待。

### 3.1 异步状态机调度 (Async State Machine in Scheduler)
得益于 v0.2 的任务栈机制，如果某个任务需要向 LLM 询取代价或重写建议：
1. 调度器发出一个非阻塞请求给本机的 `LLM Agent`（如基于 Ollama 的微服务）。
2. 将当前的 `Task` 状态切换为 `WaitingForLLMResponse` 并重新压栈。
3. 调度器继续执行栈里其他不依赖 LLM 的优化任务（这在多分支探索中是完全可行的）。
4. 当 LLM Agent 返回结果时，该 Task 被唤醒并利用 LLM 提供的智慧继续进行决策。

### 3.2 离线微调模型 (Offline Model Tuning)
针对**场景 C (搜索引导)** 和 **场景 A (基数估计)**，每次都调用 GPT-4 这种大模型是不现实的。
* **流程**: 我们可以让 PG 在后台异步记录下大量执行过的慢查询、它们的真实执行代价 (Actual Cost)、以及最终的 Memo 结构。
* **微调**: 利用这些数据，离线微调一个小型的 LLM 或图神经网络 (GNN)。然后将其编译为可以在本机毫秒级推理的模型，由 `optimizer_core` 在运行时调用。

## 4. 实施里程碑计划 (Roadmap)

- **Phase 1: 概念验证 (The AI EXPLAINer)**
  - 在 `pg_bridge` 的 `ExplainOneQuery_hook` 中挂载调用接口。当用户执行 `EXPLAIN (FORMAT AI) SELECT ...` 时，收集 Catalog、AST 和计划给 LLM，验证模型对执行计划的理解能力。
- **Phase 2: 基数预测拦截 (The AI Estimator)**
  - 针对正则表达式 `~` 和 `LIKE`，在 `estimate_selectivity_v2` 中引入一个缓存机制。未命中缓存时触发 LLM 辅助估算。
- **Phase 3: 启发式优先队列 (The Learned Optimizer)**
  - 将 `Scheduler` 的 `Vec<Task>` 升级为基于 LLM/ML 模型打分的优先队列，真正实现 AI 驱动的查询搜索空间剪枝。