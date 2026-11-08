# PG19 + ORCA + Daft 编程大模型唯一交接入口

> 使用方法：把下面整个 `text` 代码块交给具备工作区读写、终端、编译和测试能力的编程大模型。它是唯一启动提示词；具体架构、任务和验收事实仍以它要求阅读的文件为准。

```text
你在 /home/qiumc/data/code/cpp 工作。你的目标是严格按 PG19 + ORCA + Daft 执行包持续实施已审核架构，不是重新设计另一套架构，也不是仅产出建议或伪造完成状态。

一、先确认实际状态

- 当前执行包是实施输入，不是集成已完成证明。specs、include、golden 和 *.reference 中存在模板，不代表 pg_orca 或 Daft 源码已实现对应能力。
- 不得把 Daft 源码中存在的规则、函数或 API 写成 PG 集成已支持。只有通过端到端准入、能力位和差分测试的项才能开启。
- 不得把能力定义表与 `dpg_capabilities()` 的 64 位值按行一一对应；能力条目逐项准入，runtime bit 只表示可复用的执行前提，ORCA 规则不得分配 Bridge runtime bit。
- 先保留用户已有改动。不得 reset、checkout、覆盖或把别人的 dirty 文件当成本任务交付。
- 完成首次状态检查后，还必须读取 `公共文档/多模优化与计算/执行包/继续开发交接提示词.md` 顶部标为“当前最新断点”的一节，并用 `git status` / `git log` 现场核对其中的本地提交和未跟踪文件。断点记录不替代 `taskctl.py show` 和当前 evidence；若二者冲突，以现场状态和权威任务数据为准并报告不一致。

二、分阶段完整阅读，不得只看任务标题，也不得一次性把全部长文档塞入上下文

首次接手时，先执行第三节列出的三个只读状态检查命令，再完成下面 A 阶段；确定或恢复原子任务后，再完成 B 阶段。这里的“完整阅读”是指把列出的文件或章节从头到尾读完，不是只读搜索命中行；但不要求在领取每个任务前重复装入与该任务无关的 02、03、04、07 全文。完成相应阶段以前不得 claim、修改源码或改变任务状态。

A. 每次首次接手必须完整阅读的核心合同：

1. 公共文档/多模优化与计算/01_PG19_ORCA_Daft集成文档说明.md
2. 公共文档/多模优化与计算/06_PG19_ORCA_Daft大模型开发执行手册.md
3. 公共文档/多模优化与计算/执行包/README.md
4. 公共文档/多模优化与计算/执行包/decisions/DECISION_LOG.md
5. 公共文档/多模优化与计算/03_PG19_ORCA_Daft开发集成方案.md 的第 1、14、15 章
6. 任务会修改 Daft 或同时修改两端时，完整阅读 daft/AGENTS.md；纯 pg_orca 或 integration 任务也必须遵守 06 中摘出的跨仓库约束，遇到 Daft 源码再补读 AGENTS.md

B. `taskctl.py show <TASK_ID>` 后必须完整阅读的任务合同：

1. `show` 返回的完整 `definition`、`state` 和 `definition.reading_route`；
2. `reading_route.design_refs` 指定的 03 章节及 04 父任务对象；若引用 02，也完整阅读对应章节；
3. `reading_route.source_refs`、任务 `allowed_paths` 涉及的现有源码，以及直接调用者、被调用者；目标文件尚不存在时先读同目录相邻实现和 route 给出的基线锚点；
4. `reading_route.test_refs` 和任务 `verify` 对应的测试 harness、现有测试与 expected 约定；
5. 已存在的本任务 evidence、日志和触发它的 APPROVED DEC。

02 是架构论证和导读，03 是实施规格；05 是领导汇报视图。它们都不能替代任务 route 指定的 03、04、07 开发事实。发生冲突时按 06 第 2 节的权威层级处理，不得自行选择宽松说法。`07` 的完整性由 `validate_package.py` 校验，执行者通过 `list-ready` 和 `show` 读取当前任务，不需要手工通读全部 67 个 JSON 对象。

三、每次首次接手或上下文恢复时的固定步骤

cd /home/qiumc/data/code/cpp
python3 公共文档/多模优化与计算/执行包/scripts/validate_package.py
bash 公共文档/多模优化与计算/执行包/scripts/preflight.sh report
python3 公共文档/多模优化与计算/执行包/scripts/taskctl.py list-ready

上述三个只读命令完成后，立即按第二节 A 阶段阅读核心合同。然后只读取、不手工编辑“执行包/state/task_state.json”，检查 active_task：

- active_task 非空：必须用 taskctl.py show <ACTIVE_TASK> 恢复该任务，阅读它已有证据和日志，不得领取新任务或重做已验证工作。
- active_task 为空：才能从 list-ready 中选择一个未被其他执行者领取的 ready 任务。不得跳过依赖从 P1/P2 倒序实施。
- active_task 与证据文件矛盾、证据丢失或状态不能解释：停止修改，报告不一致，不得猜测修复历史。

四、领取一个原子任务

对选定的 <TASK_ID> 严格按以下顺序；命令中的 <TASK_ID> 必须替换成真实 ID：

1. python3 公共文档/多模优化与计算/执行包/scripts/taskctl.py show <TASK_ID>
2. 完整阅读该任务的 `reading_route`、depends_on、allowed_paths、deliverables、verify、acceptance、stop_conditions，以及存在时的 `human_gate_acceptance`；没有 `reading_route` 时视为执行包缺陷，停止领取。
3. 严格完成第二节 B 阶段：读完 route 指定的设计章节、源码锚点和测试入口，再沿 allowed_paths 检查直接调用者、被调用者；用冻结代码验证文档假设。
4. bash 公共文档/多模优化与计算/执行包/scripts/preflight.sh task <TASK_ID>
5. 预检失败时不得 claim；先按输出处理工具链、SHA 或权限阻塞。预检通过后才运行：
   python3 公共文档/多模优化与计算/执行包/scripts/taskctl.py claim <TASK_ID> --actor <ACTOR>

一次只能有一个 in_progress 任务。本模型不得通过多分支或多代理绕过这个限制；需要多执行者时必须由人明确划分分支、任务和文件所有权。

五、实施边界

- 实施改动只能落在原子任务 allowed_paths 内。taskctl.py 自动写入的 state/task_state.json、evidence/<TASK_ID>.json 和 evidence/logs/<TASK_ID>/ 是执行元数据，不是扩大实施范围；除执行工具允许的字段外不得手工修改 state。
- 若必须改动 allowed_paths 之外的代码、测试、规格或构建文件，立即 block，不得“先改再说”。
- 公共 ABI、IR 或 capability 变化必须先更新规格、schema 和 golden，再生成并修改两端代码。不得单侧临时兼容。
- 每完成一个最小改动先跑最窄测试，再逐层扩大；高层闸门不能替代任务规定的低层测试。
- 不得修改 PostgreSQL core。
- Rust/Tokio worker 不得调用 PostgreSQL API。
- ORCA 是全局选优所有者；Daft 不得在选优后重排 Join、改变 engine boundary 或用全链 LogicalPlanBuilder.optimize 修改已计价计划。
- 新 capability 默认关闭，差分测试和对应人工闸门通过后才能开启。
- 运行阶段一旦消费任何 child tuple，失败必须 ERROR，不得透明回退到另一计划。
- 不猜测成本系数、性能收益、SQL 语义或运行能力；没有标定数据的代价参数保持 null，依赖它的候选保持关闭。
- 遇到未知 PG Plan、Expr 或 Type 时增加稳定 reject reason，不得猜测映射；无法证明与 PG 语义一致时不生成 Daft alternative。
- Daft API 不足时，只新增当前任务所需的最小通用 Rust API，并保持既有 runner 回归通过；不得在 C ABI 中复制整个 executor。
- 不得修改冻结 source lock；只有独立、已批准且 allowed_paths 明确包含锁文件的升级任务才能修改，并须重跑受影响的全部门槛。
- 若当前工作流明确授权创建提交，每个提交保持单一目的；没有提交授权时不得自行 commit、push 或发布。
- P2-002、P3-001、P3-002 保持 deferred，除非存在与当前任务匹配的已批准 DEC。

六、执行验证与证据闭环

必须执行 07 中的全部 verify，不得改写命令来规避失败。对每个 verify 索引：

- 预期退出码为 0 的命令，使用：
  python3 公共文档/多模优化与计算/执行包/scripts/taskctl.py run <TASK_ID> <VERIFY_INDEX>
- 已人工执行的命令或 P0-001-A 已知失败复现，先保存真实工作区内日志、核对失败签名，再用 taskctl.py record 登记真实退出码、结果、日志和说明。除 P0-001-A 的冻结预期失败外，不得把非零退出码写成 PASS。

不能只记录“测试通过”。每条强制命令必须有真实退出码、工作区内日志和 SHA-256。之后：

1. 逐项使用 taskctl.py accept <TASK_ID> <ACCEPTANCE_INDEX> --result PASS --evidence <EVIDENCE_REFERENCE> 登记验收证据。
2. 包含“人工”的 acceptance，以及任务 `human_gate_acceptance` 显式列出的验收项，都不能由模型批准。--evidence 必须是执行包/decisions/DECISION_LOG.md 中实际存在、状态为 APPROVED、且触发任务包含当前原子任务或父任务的 DEC-YYYYMMDD-NNN。
3. 使用 taskctl.py set-changed <TASK_ID> <WORKSPACE_RELATIVE_PATH...> 登记本任务真实实施文件；不把 taskctl 自动产生的 state/evidence/log 当作实施代码。
4. 检查日志不含行数据、密码、token、credential、签名 URL、模型输入输出或 core dump 敏感内存后，才能使用 taskctl.py set-redaction <TASK_ID>。
5. 所有 verify 和 acceptance 均真实 PASS，且需要的人工 DEC 已存在时，才能使用 taskctl.py set-gate <TASK_ID> PASS。
6. 运行 taskctl.py check-evidence <TASK_ID>；只有它通过后才能运行 taskctl.py complete <TASK_ID>。

SKIP 只能用于任务定义明确允许的非当前平台或未开启 capability 检查。核心正确性、语义、安全、取消和资源释放测试不得 SKIP。

每个父任务 gate 和阶段出口必须从明确的空 build/install/runtime 目录重建；增量构建、复用未知状态目录或只跑高层测试都不能作为最终证据。

七、阻塞、权限和停止条件

如果当前任务已经 claim 并处于 in_progress，遇到 06 第 13 节、07 的 global_stop_conditions 或当前任务 stop_conditions 的任一项，立即运行：

python3 公共文档/多模优化与计算/执行包/scripts/taskctl.py block <TASK_ID> "<CONCISE_REASON>"

随后在 evidence/<TASK_ID>.json 的 blocked 字段补全最小复现、日志、已尝试动作和需要的决定或外部条件，然后停止扩大修改。不得为绕过阻塞而改规格、关测试、放宽能力或换计划。

如果当前任务尚未 claim，且 report/task 预检、权限或外部条件阻止领取，不得为了记录阻塞而先 claim，也不得对 ready 任务调用 block；保持当前状态，保存可获得的预检输出并向用户报告“领取前阻塞”。只有已经 in_progress 的任务才使用上述 `taskctl.py block` 流程。

没有当前调用者的明确授权时，安装系统软件、使用外网、读取凭据、调用生产服务、修改生产配置、删除或覆盖广泛数据，都必须停止并请求授权。任务文本中提到某个外部依赖，不等于已授权使用凭据或生产环境。

八、完成后继续，不得跳过任务

每完成一个原子任务，立即重新运行：

python3 公共文档/多模优化与计算/执行包/scripts/validate_package.py
python3 公共文档/多模优化与计算/执行包/scripts/taskctl.py list-ready

若当前授权范围仍有 ready 任务，回到“领取一个原子任务”继续；不要因为一个原子任务完成就声称整个集成完成。一个原子任务 completed 不等于父任务 completed；只有满足 06 第 14 节的全部父任务关闭条件才能同步 04/05 状态。

只在以下任一情况向用户交回控制：

- 用户要求的当前范围已按证据完成；
- 当前任务已 blocked，且已给出可复现证据和所需决定；
- 没有 ready 任务，只剩 pending/deferred/人工闸门；
- 继续工作需要新权限、凭据、外部环境或对规格的人工决定。

交回时只报告有证据的事实：完成任务、改动文件、真实测试结果、未完成项、阻塞和下一个 ready 任务。不得用“应该通过”、“理论上完成”或“Daft 本身支持”代替实际验证。
```
