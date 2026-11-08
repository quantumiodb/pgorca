# PG19 + ORCA + Daft 执行包

本目录是 [06 大模型开发执行手册](../06_PG19_ORCA_Daft大模型开发执行手册.md) 的支撑文件集合。

## 文件用途

| 路径 | 用途 | 运行时是否允许修改 |
|---|---|---|
| `state/task_state.json` | 原子任务当前状态和领取信息 | 只能通过 `taskctl.py` 修改 |
| `evidence/` | 每个已领取任务的结构化证据和日志索引 | 允许按手册更新 |
| `decisions/DECISION_LOG.md` | 人工阶段决定和规格变更记录 | 仅在明确决定后追加 |
| `specs/` | 能力表和 IR 初始冻结模板 | 本目录模板不改；实施时复制到目标源码后变更 |
| `include/` | Bridge C ABI 参考头文件 | 本目录模板不改 |
| `golden/` | 首批合法和非法 IR 固定样例 | 本目录模板不改 |
| `scripts/` | 执行包自检、环境报告和任务状态工具 | 只有执行包维护任务可改 |
| `MODEL_HANDOFF_PROMPT.md` | 编程大模型的唯一启动入口；规定核心合同加任务 reading_route 的分阶段阅读 | 不改，除非执行协议变化 |
| `新人或新模型从零构建与基础测试指南.md` | 当前本地交付代码的离线接收、隔离全量构建、no-Daft 构建和 16 项基础验收 | 断点、SHA 或构建入口变化时同步更新 |

## 最小使用方式

对外交接时只要求模型完整读取并执行 `MODEL_HANDOFF_PROMPT.md`；下面的命令是人工快速检查执行包时的最小集合，不代替完整交接提示词。

不要把 02、03、04、07 全文直接拼接成一份超长提示。模型完成只读检查和核心合同阅读后，对选定任务运行 `taskctl.py show <TASK_ID>`；输出中的 `definition.reading_route` 给出必须精读的设计章节、现有源码锚点和测试入口，随后再沿 `allowed_paths` 检查直接依赖代码。

```bash
cd /home/qiumc/data/code/cpp
python3 公共文档/多模优化与计算/执行包/scripts/validate_package.py
bash 公共文档/多模优化与计算/执行包/scripts/preflight.sh report
python3 公共文档/多模优化与计算/执行包/scripts/taskctl.py list-ready
```

不要直接手工编辑 `task_state.json`。不要把这里的模板存在误读成 pg_orca 或 Daft 已经实现了相应能力。
