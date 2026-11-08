# PG19 + pg_orca + Daft 集成交付物

## 唯一权威目录

本目录是 PG19 + pg_orca + Daft 集成文档的唯一维护和交付位置：

```text
/home/qiumc/data/code/cpp/公共文档/多模优化与计算
```

原目录 `deliverables/pg19_pgorca_daft` 已在 2026-08-10 完成归并。归并前按文件名和内部链接规范化后进行了逐字节比较，四组文件完全一致，没有仅存在于旧目录的正文、规则、接口或任务。映射关系如下：

| 原文件 | 当前唯一文件 |
|---|---|
| `README.md` | `01_PG19_ORCA_Daft集成文档说明.md` |
| `ARCHITECTURE_WALKTHROUGH.md` | `02_PG19_ORCA_Daft架构与执行流程详解.md` |
| `PG19_PGORCA_DAFT_INTEGRATION_PLAN.md` | `03_PG19_ORCA_Daft开发集成方案.md` |
| `IMPLEMENTATION_TASKS.yaml` | `04_PG19_ORCA_Daft实施任务清单.yaml` |

今后只修改本目录中的 `01-07` 文件和本目录下的 `执行包`，不再创建或人工同步第二套正文。文档间引用必须使用本目录的中文编号文件名；`04` 的 `plan` 必须保持指向 `03_PG19_ORCA_Daft开发集成方案.md`。`05` 是依据 `04` 生成的领导汇报视图，没有旧目录对应文件；`06/07/执行包` 是新增的大模型实施层，也没有旧目录对应文件。父任务状态、依赖和验收发生变化时先修改 `04`，再同步 `05`；原子任务定义修改 `07`，运行状态和证据只通过执行包维护。

## 新增大模型实施文件完整清单

`06`、`07` 与 `执行包` 共同构成可交给编程大模型的实施层。下表是当前固定文件的完整登记；任务执行后在 `evidence/`、`evidence/logs/` 下产生的按任务命名证据和日志属于运行数据，不逐个登记在 01。

| 类别 | 文件 | 作用 |
|---|---|---|
| 实施正文 | [06_PG19_ORCA_Daft大模型开发执行手册.md](06_PG19_ORCA_Daft大模型开发执行手册.md) | 规定领取、实施、验证、证据、阻塞和人工闸门流程 |
| 机器任务 | [07_PG19_ORCA_Daft原子任务清单.json](07_PG19_ORCA_Daft原子任务清单.json) | 67 个原子任务的依赖、路径边界、工期、交付物和验收命令，以及覆盖全部任务的设计、源码和测试阅读路由 |
| 执行包入口 | [执行包/README.md](执行包/README.md) | 说明执行包结构和最小使用方式 |
| 模型交接 | [执行包/MODEL_HANDOFF_PROMPT.md](执行包/MODEL_HANDOFF_PROMPT.md) | 交给编程大模型的唯一启动入口，负责串联全部文档、任务和证据流程 |
| 任务状态 | [执行包/state/task_state.json](执行包/state/task_state.json) | 记录领取、进行、阻塞和完成状态，只能通过 `taskctl.py` 改动 |
| 证据规范 | [执行包/evidence/README.md](执行包/evidence/README.md) | 说明命令日志、SHA-256、验收和脱敏证据要求 |
| 证据模板 | [执行包/evidence/TEMPLATE.json](执行包/evidence/TEMPLATE.json) | 新原子任务证据文件的固定字段模板 |
| 人工决策 | [执行包/decisions/DECISION_LOG.md](执行包/decisions/DECISION_LOG.md) | 记录 ABI/IR 冻结、阶段准入和可选能力的 `APPROVED DEC` |
| 能力合同 | [执行包/specs/pg_daft_capabilities.initial.yaml](执行包/specs/pg_daft_capabilities.initial.yaml) | 冻结类型、节点、32 个 OptimizerRule、非规则物理决策和 AI 函数的初始关闭状态 |
| IR 合同 | [执行包/specs/pg_daft_plan_v1.schema.json](执行包/specs/pg_daft_plan_v1.schema.json) | `PgDaftPlan v1` 的 JSON Schema |
| 性能闸门 | [执行包/specs/performance_gate.initial.json](执行包/specs/performance_gate.initial.json) | P1 正确性、性能对照、退化上限和成本标定初始提案 |
| Bridge ABI | [执行包/include/daft_pg_bridge_v1.reference.h](执行包/include/daft_pg_bridge_v1.reference.h) | C/C++ 与 Rust bridge 共享的 ABI v1 参考头文件 |
| Arrow ABI | [执行包/include/arrow/c/abi.h](执行包/include/arrow/c/abi.h) | Arrow C Data Interface 结构声明，使 bridge header 可独立编译 |
| Golden 索引 | [执行包/golden/manifest.json](执行包/golden/manifest.json) | 登记正反 IR fixture、校验范围和稳定拒绝码 |
| 合法 Golden | [执行包/golden/valid/project_limit_v1.json](执行包/golden/valid/project_limit_v1.json) | Project + Limit 的 schema、图结构和 canonical roundtrip 样例 |
| 非法 Golden | [duplicate_key_v1.json](执行包/golden/invalid/duplicate_key_v1.json)、[dangling_node_v1.json](执行包/golden/invalid/dangling_node_v1.json)、[cycle_v1.json](执行包/golden/invalid/cycle_v1.json)、[unknown_field_v1.json](执行包/golden/invalid/unknown_field_v1.json)、[unknown_required_capability_v1.json](执行包/golden/invalid/unknown_required_capability_v1.json)、[missing_required_capability_v1.json](执行包/golden/invalid/missing_required_capability_v1.json)、[dangling_source_v1.json](执行包/golden/invalid/dangling_source_v1.json)、[orphan_node_v1.json](执行包/golden/invalid/orphan_node_v1.json)、[output_schema_mismatch_v1.json](执行包/golden/invalid/output_schema_mismatch_v1.json) | 固定重复键、未知字段、悬空 ID、环、未知或缺失必需能力、source 映射、孤立节点和输出 schema 失配的拒绝行为 |
| 超大 IR 配方 | [执行包/golden/recipes/oversized_plan_v1.json](执行包/golden/recipes/oversized_plan_v1.json) | 生成超过 payload 上限输入的负例配方 |
| 包自检 | [执行包/scripts/validate_package.py](执行包/scripts/validate_package.py) | 校验文档、任务 DAG、工期、schema、golden、ABI、能力表和模板 |
| 环境预检 | [执行包/scripts/preflight.sh](执行包/scripts/preflight.sh) | 检查源码 SHA、工作树、PG 基线和 Daft 工具链 |
| 状态工具 | [执行包/scripts/taskctl.py](执行包/scripts/taskctl.py) | 实施领取、命令记录、证据验证、人工 DEC 校验和完成解锁 |
| 模板说明 | [执行包/templates/README.md](执行包/templates/README.md) | 说明参考脚本何时复制为正式集成脚本 |
| 干净构建模板 | [build_clean.sh.reference](执行包/templates/integration/build_clean.sh.reference) | 从空目录构建 PG19、Daft bridge 和 pg_orca |
| 临时集群模板 | [cluster.sh.reference](执行包/templates/integration/cluster.sh.reference) | 安全初始化、启停和清理专用 PG 测试集群 |
| 阶段闸门模板 | [run_gate.sh.reference](执行包/templates/integration/run_gate.sh.reference) | 统一 P0、P1、P2 正确性、性能、故障和稳定性入口 |
| 日志脱敏模板 | [redact_log.py.reference](执行包/templates/integration/redact_log.py.reference) | 检测 token、密码、签名 URL、AWS key 和私钥特征 |

## 阅读顺序

下面是人工理解整套方案时的推荐顺序。交给实现模型时不要把 02～07 全文一次性拼进提示词；只交付唯一入口 `MODEL_HANDOFF_PROMPT.md`，由它先读核心合同，再由 `taskctl.py show <TASK_ID>` 输出当前任务的 `reading_route`，按路由精读相关章节和源码。

人工按以下顺序阅读：

1. [架构导读](02_PG19_ORCA_Daft架构与执行流程详解.md)：先判断什么时候需要统一 CBO、为什么选择 ORCA 集成式 CBO 和嵌入式 Daft，再理解 Daft 规则纳管、PG/Daft 执行边界、同进程数据通道和完整混合计划。
2. [实施规格](03_PG19_ORCA_Daft开发集成方案.md)：把 02 的选型结论视为已冻结约束，再查阅规则注册表、ORCA physical alternatives/cost model、ABI/IR、内存、文件级设计与验收门槛；03 不重复维护选型论证。
3. [父任务清单](04_PG19_ORCA_Daft实施任务清单.yaml)：20 个带依赖、实施动作和硬验收条件的管理级任务；其中 AI、Daft-owned external scan 和 sidecar 是默认关闭的后续可选任务。
4. [大模型执行手册](06_PG19_ORCA_Daft大模型开发执行手册.md)：规定任务领取、允许改动范围、状态机、证据、停止条件和人工闸门。
5. [原子任务清单](07_PG19_ORCA_Daft原子任务清单.json)：67 个机器可校验工作单元，给出精确依赖、交付物、验证命令和停止条件；`reading_routes` 为每个父任务登记设计章节、冻结源码锚点和测试入口，`taskctl show` 会把它解析到每个原子任务。
6. [执行包入口](执行包/README.md)：包含启动提示词、任务状态、证据、IR/ABI/能力表模板、golden cases、预检和状态工具。
7. [Daft 开发约束](../../daft/AGENTS.md)：Daft 仓库本地开发约束。
8. [领导汇报](05_PG19_ORCA_Daft领导汇报.md)：把 04 的阶段、状态、关键路径、风险、审批项和验收口径转换为管理视图；不替代开发规格或原子任务事实。

## 跨文档统一口径

后续修改 02-07 和执行包时必须保持下表一致；若正文与本表冲突，以正文中更严格的安全边界为准，并同时修正本表，不能长期保留两种说法。

本文档组中的“核心 P0-P2”指实施规格的 P0、P1 和生产硬化 P2（机器任务到 `P2-001`），不包含延期的 `P2-002` AI 路线，也不包含 `P3-001/P3-002`。

| 主题 | 统一结论 |
|---|---|
| 当前状态 | 只有源码冻结、PG19/pg_orca 构建运行基线和实施设计；`DaftExec`、Rust bridge、外部直读和 AI 接入都尚未实现 |
| 为什么用 ORCA | PG19 本身是成熟 CBO，固定 Daft 路径或固定 Daft-only AI 函数并不要求 ORCA；本项目因为要把代价相关 Daft 规则、传统算子、AI 位置、外部扫描和行列边界统一搜索，且不深改 PG core，当前复用仓库已有 ORCA |
| 候选生成 | 只枚举结果正确、当前真实存在且能满足查询要求的实现；普通算子可以比较 PG/Daft，Daft-only AI 不得虚构 PG 对照实现 |
| 计划形成 | 引擎、Join 顺序和算子位置在 ORCA 选优时决定；翻译器只把胜出树中相邻 Daft 节点编码为 `DaftExec`，最终计划检查不得重新下推或换引擎 |
| 核心数据源 | P0-P2 只启用 PG-owned source；PG heap/index 必须由 PG executor 按 MVCC/RLS/权限语义扫描，再按批转成 Arrow |
| 外部对象 | Daft 直读 S3/Parquet 是 `P3-002` 的独立可选能力，只能针对显式登记的 external relation，并与等价 PG ForeignScan 作为两个有成本的实现比较；它不受 PG MVCC 直接保护 |
| 进程关系 | P0-P2 的 Daft bridge/Swordfish 与当前 PG backend 同进程，Rust worker 不得调用 PG API；`P3-001` sidecar 才是独立跨进程模式 |
| AI 与多模 | `P2-002` 是延期能力；当前 Python `UDFProject`、VLLM、GPU/model actor 均为 `BLOCKED`，不能把 Daft 源码中已有 API 写成 PG 集成已支持 |
| 回退与缓存 | 规划阶段可以禁用 Daft 后重新做一次完整 ORCA 优化；开始执行后不得透明换计划。digest 只能检测 cached plan 失配，不会自动触发 PG 重规划；失效机制完成前，执行前发现失配必须报错 |

当前已完成的是源码拉取、深度代码勘察、PG19/pg_orca 编译与运行基线，以及实施设计；尚未实现 Daft bridge，也尚未为本集成修改三个上游 source tree。

冻结源码：

- PostgreSQL 19beta2：`7873db5369b967dd53984df34fd800003410d04f`
- pg_orca：`55339728f8987ff351d3e88e033a0dc3be78465f`
- Daft：`b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a`

首个可执行父任务是 `P0-001` 或 `P0-002`；实现模型实际领取的首批原子任务是 `P0-001-A`（复现 Xerces 干净构建失败）或 `P0-002-A`（盘点工具链、取得人工 DEC，并只在批准范围内完成实际配置）。任何模型都不得从 P1/P2 倒序开始。
