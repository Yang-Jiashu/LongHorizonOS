<div align="center">

<img src="assets/brand/banner.svg" alt="LongHorizonOS" width="100%">

# LongHorizonOS

### 长时程 Agent 的操作系统

**Harness 让 Agent 一直跑。<br/>LongHorizonOS 让它别白跑。**

<br/>

<!-- Badges Row 1: Project Identity -->
[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-Apache--2.0-D22128?style=for-the-badge)](LICENSE)
[![Status](https://img.shields.io/badge/status-research%20alpha-yellow?style=for-the-badge)](docs/releases/v0.1.0.md)

<!-- Badges Row 2: GitHub Metrics -->
[![GitHub Stars](https://img.shields.io/github/stars/Yang-Jiashu/LongHorizonOS?style=for-the-badge&logo=github&color=gold)](https://github.com/Yang-Jiashu/LongHorizonOS/stargazers)
[![GitHub Forks](https://img.shields.io/github/forks/Yang-Jiashu/LongHorizonOS?style=for-the-badge&logo=github&color=forestgreen)](https://github.com/Yang-Jiashu/LongHorizonOS/network/members)
[![GitHub Issues](https://img.shields.io/github/issues/Yang-Jiashu/LongHorizonOS?style=for-the-badge&logo=github&color=orange)](https://github.com/Yang-Jiashu/LongHorizonOS/issues)
[![Last Commit](https://img.shields.io/github/last-commit/Yang-Jiashu/LongHorizonOS?style=for-the-badge&logo=github&color=blueviolet)](https://github.com/Yang-Jiashu/LongHorizonOS/commits/main)

<!-- Badges Row 3: Tech & Eval -->
[![Benchmark](https://img.shields.io/badge/LHTB-46%20%E4%B8%AA%E4%BB%BB%E5%8A%A1%E9%85%8D%E5%AF%B9-4A7DBF?style=for-the-badge)](#%EF%B8%8F-%E8%AF%84%E6%B5%8B46-%E4%B8%AA%E4%BB%BB%E5%8A%A1%E7%9A%84-longhorizonbenchmarklhtb%E9%85%8D%E5%AF%B9%E5%8F%8C%E8%87%82)
[![Top Language](https://img.shields.io/github/languages/top/Yang-Jiashu/LongHorizonOS?style=for-the-badge&color=3776AB)](https://github.com/Yang-Jiashu/LongHorizonOS)
[![Code Size](https://img.shields.io/github/languages/code-size/Yang-Jiashu/LongHorizonOS?style=for-the-badge&color=teal)](https://github.com/Yang-Jiashu/LongHorizonOS)
[![Contributors](https://img.shields.io/github/contributors/Yang-Jiashu/LongHorizonOS?style=for-the-badge&color=crimson)](https://github.com/Yang-Jiashu/LongHorizonOS/graphs/contributors)

<br/>

[English](README.md) | 简体中文

</div>

---

## 📑 目录

- [你早就见过的那种死法](#你早就见过的那种死法)
- [思路：外骨骼，不是移植手术](#思路外骨骼不是移植手术)
- [✨ 核心机制](#-核心机制)
- [🏗️ 本版本已实现](#️-本版本已实现)
- [📊 评测：46 个任务的 LongHorizonBenchmark（LHTB）配对双臂](#%EF%B8%8F-%E8%AF%84%E6%B5%8B46-%E4%B8%AA%E4%BB%BB%E5%8A%A1%E7%9A%84-longhorizonbenchmarklhtb%E9%85%8D%E5%AF%B9%E5%8F%8C%E8%87%82)
- [为什么不能只靠更用力写-prompt--更大上下文--原生压缩](#为什么不能只靠更用力写-prompt--更大上下文--原生压缩)
- [🚀 快速开始](#-快速开始)
- [🗺️ 路线图](#️-路线图)
- [📚 文档](#-文档)
- [🧑‍💻 开发](#-开发)
- [贡献-安全-许可证](#贡献--安全--许可证)
- [致谢](#致谢)

---

## 你早就见过的那种死法

长时程 Agent 不会崩溃。它们干更糟的事：**一直跑下去**。

以下是我们评测车队里的真实轨迹（接入 OS 层之前）：

> **`apex-openroad`** —— 3.5 小时、158 次切片续跑。每次续跑都重读全部对话，到最后**单次模型调用要付 183,000 token**。烧掉 45.9M token，reward 从 0.292 掉到 0。没人拦它，甚至没有任何组件察觉到异常。

> **`super-mario`** —— 120M token。Agent 连第一关都没过去。但它全程都在"工作"。

> **`audio-visual`** —— 90 分钟的会话里 **54% 的时间片什么都没干**：零工具调用、零进展，而 verifier 每片都在问"做完了吗"，harness 每片都在答"继续"。

每个长时程 Agent 系统里都有这样的会话。它们不是边缘 case，而是吃掉预算的长尾。Harness 治不了它们，因为**正在跑的就是 harness 自己**——你需要一个在它外面的层。

---

## 思路：外骨骼，不是移植手术

LongHorizonOS 包住现有的 Agent harness，把每次运行当成**操作系统下的一个进程**：

<p align="center">
<img src="assets/eval/lhos-architecture.svg" alt="LongHorizonOS 控制面：观测、决策、干预，harness 不变" width="860">
</p>

模型不变。Harness 不变。任务不变。唯一的区别是**有人在看表**。

---

## ✨ 核心机制

OS 类比是故意的。所有闸门都是 fail-closed：观测有歧义就*继续跑*。

<div align="center">

| | OS 概念 | LongHorizonOS 实现 |
|---|---|---|
| ⏱️ | **时间片** | 有界执行切片——失控的运行最多再漂一个片就会被干预 |
| 🔄 | **上下文切换** | 带压缩语义 handoff 的重启——目标、证据、产物 URI 带走；100K token 的会话全文不带 |
| 💡 | **投资而非水位警报** | 重启必须是*挣来的*——handoff 带得动状态、且上一次重启确实回了本，才允许发生 |
| 🧨 | **OOM Killer** | 限速安全阀——单调用 cache ≥ 4 倍软线必然触发重启，但永远不会振荡 |
| 🐕 | **看门狗** | 止损停机——连续 8M token 燃烧且零写入、零测试、verifier 冻结 → 停。任何生命迹象都会重置计时 |
| 📑 | **页表** | 版本化进度图（Goal / Artifact / Evidence / 有效性）。中途变更只让受影响子图失效。已验证的工作绝不重做 |

</div>

reward 红线是结构性的，不是口号。

---

## 🏗️ 本版本已实现

工件核心是一个崩溃一致、内容寻址的存储，外面包一层能力模型：

<div align="center">

| 能力 | 含义 |
|---|---|
| 📝 **Process / Action / Journal** | 每次变更都是记录在案、原子的事件；进程只能通过显式派生的 action 行动 |
| 🔐 **Capability / Lease / Signal** | 资源访问走能力授权，所有权基于租约，控制流走类型化信号 |
| 💥 **Crash Recovery** | 对 SIGKILL 有韧性、恰好一次语义；回放日志即可重建崩溃前的精确状态 |
| 🗃️ **Versioned Artifact FS** | 工件不可变、内容寻址、带版本；读写走原子写协议 |
| 🧱 **Namespace Isolation** | 每个进程命名空间相互隔离；跨命名空间访问需要显式能力 |
| ✅ **Version-checked Commits** | 用期望版本做乐观并发——陈旧写入会被拒绝而不是悄悄丢失 |
| 🔗 **Canonical URI Security** | 工件 URI 统一规范化，路径穿越在边界处被拒绝 |

</div>

### 尚未实现

- **分布式多智能体集群**——当前控制面是单机形态
- **通用信念修正**——进度图是版本化的，但尚未在一般情形下撤回先前结论

---

## 📊 评测：46 个任务的 LongHorizonBenchmark（LHTB）配对双臂

46 个 **LHTB**（**LongHorizonBenchmark**，长时程基准测试套件）任务（真实软件工程与数据科学负载，机器可验证的 verifier）每个跑**两遍**——同模型（`stepfun/step-3.7-flash`）、同任务二进制、同预算、同 verifier：

- **fresh** —— harness 裸跑
- **lhos** —— 同一次运行，置于 LongHorizonOS 控制之下

<div align="center">
<br/>

### ── 核心指标 ──

# <span style="font-size:2.4em;color:#2ea043">37 / 42</span>

### reward 持平或更优

<br/>

# <span style="font-size:2.4em;color:#4A7DBF">22 / 27 (81%)</span>

### 至少一臂有得分

<br/>

# <span style="font-size:2.4em;color:#8957e5">~2.4×</span>

### input token 节省

<br/>

# <span style="font-size:2.4em;color:#e6850a">0</span>

### 无需修改 harness

<br/>

<img src="assets/eval/lhtb-46-rewards.png" alt="46 个 LHTB 任务的逐任务 reward 对比（fresh vs lhos）" width="720">

<br/><br/>

<img src="assets/eval/lhtb-46-tokens.png" alt="逐任务 input token 对比（对数轴）" width="520">

</div>

**怎么看这两张图**：左图——每个任务两臂的 verifier reward；红色是我们如实披露的 5 个掉点；灰色是 3 个无有效 lhos 结果的任务。分母写清楚：46 个任务 − 3 个无有效结果 − 1 个无效配对（matpower，见 †）= 42；其中 15 个是 0–0 双零对（两臂都没做出来），统计上算持平但没有信息量——22/27 的比率已把它们剔除。右图——逐任务 input token（对数轴），对角线以下 = OS 层更省。`commit0` 是留在对角线上方的反例——我们选择披露而不是裁掉。

<details>
<summary><b>完整 46 任务配对结果表</b>（单 seed reward；±0.01 容差）</summary>

| # | 任务 | fresh | lhos | Δ | 备注 |
|---|---|---:|---:|---:|---|
| 1 | grammar-fuzz | 0.939 | 0.828 | -0.111 | **掉点** |
| 2 | poc-exploit | 0.892 | 0.892 | +0.000 | 持平 |
| 3 | spot | 0.855 | 0.909 | +0.054 | 提升 |
| 4 | spice | 0.606 | 0.636 | +0.030 | 提升 |
| 5 | foldseek | 0.333 | 0.333 | +0.000 | 持平 |
| 6 | alp | 0.300 | 0.200 | -0.100 | **掉点** |
| 7 | apex-openroad | 0.292 | 0.000 | -0.292 | **掉点** |
| 8 | great-expectations | 0.273 | 0.273 | -0.000 | 持平 |
| 9 | satellite | 0.200 | 0.200 | +0.000 | 持平 |
| 10 | su2 | 0.200 | 0.200 | +0.000 | 持平 |
| 11 | unison | 0.167 | 0.167 | -0.000 | 持平 |
| 12 | apexmgmt | 0.160 | 0.118 | -0.042 | **掉点** |
| 13 | modflow6 | 0.092 | 0.000 | -0.092 | **掉点** |
| 14 | climate | 0.084 | 0.086 | +0.002 | 持平 |
| 15 | audio-visual | 0.068 | 0.066 | -0.002 | 持平 |
| 16 | materials | 0.067 | 0.064 | -0.004 | 持平 |
| 17 | rush_hour | 0.040 | 0.040 | +0.000 | 持平 |
| 18 | scientific-figure | 0.040 | 0.040 | +0.000 | 持平 |
| 19 | robotics-slam | 0.029 | 0.029 | +0.000 | 持平 |
| 20 | microscopy | 0.017 | 0.121 | +0.104 | 提升 |
| 21 | apex-ib244 | 0.014 | 0.144 | +0.130 | 提升 |
| 22 | document-table | 0.012 | 0.012 | +0.000 | 持平 |
| 23 | nrel | 0.005 | 0.000 | -0.005 | 持平 |
| 24 | epa-swmm | 0.002 | 0.031 | +0.029 | 提升 |
| 25 | opensees | 0.001 | 0.000 | -0.001 | 持平 |
| 26 | matpower | 0.000 | 0.583 | +0.583 | 无效配对 † |
| 27 | generals-bot | 0.000 | 0.430 | +0.430 | 提升 |
| 28 | sudoku | 0.000 | 0.027 | +0.027 | 提升 |
| 29 | chess-mate | 0.000 | 0.000 | +0.000 | 持平 |
| 30 | nbody | 0.000 | 0.000 | +0.000 | 持平 |
| 31 | langchain | 0.000 | 0.000 | +0.000 | 持平 |
| 32 | 2048 | 0.000 | 0.000 | +0.000 | 持平 |
| 33 | apex-investment | 0.000 | 0.000 | +0.000 | 持平 |
| 34 | unknown-config | 0.000 | 0.000 | +0.000 | 持平 |
| 35 | tabular | 0.000 | 0.000 | +0.000 | 持平 |
| 36 | gdal | 0.000 | 0.000 | +0.000 | 持平 |
| 37 | snake_maze | 0.000 | 0.000 | +0.000 | 持平 |
| 38 | super-mario | 0.000 | 0.000 | +0.000 | 持平 |
| 39 | dicom | 0.000 | 0.000 | +0.000 | 持平 |
| 40 | epidemic | 0.000 | 0.000 | +0.000 | 持平 |
| 41 | commit0 | 0.000 | 0.000 | +0.000 | 持平 |
| 42 | sokoban | 0.000 | 0.000 | +0.000 | 持平 |
| 43 | duckdb | 0.000 | 0.000 | +0.000 | 持平 |
| 44 | apex-law433 | 0.452 | — | — | 无有效 lhos 结果 |
| 45 | vector-db | 0.300 | — | — | 无有效 lhos 结果 |
| 46 | riscv | 1.000 | — | — | 无有效 lhos 结果 |

† fresh 臂在该次配对中超时；该任务的历史 fresh 值为 1.0——此行应视为无效，而不是增益。

</details>

**诚实声明（因为这很重要）**：42 个有效任务里有 5 个在 OS 层下得分*更低*。每个的根因我们都查清了：部分是 restart 敏感型任务、两个长会话存在上下文膨胀限制。当前的策略 2.0（投资门控重启、限速安全阀、止损停机）就是针对它们设计的，研究日志会持续跟踪它们是否改善。如果没改善，这段话会一直写在这里。

---

## 为什么不能只靠[更用力写 prompt / 更大上下文 / 原生压缩]？

- **写 prompt** 是让 agent 自己管自己。而正在漂移的就是 agent 自己。
- **更大上下文**让"单次调用 183K token"的问题变得*更贵*，不是更便宜。
- **原生压缩**发生在会话内部，看不见预算、看不见 verifier 信号、看不见前三次重启有没有回本。OS 全看得见——因为它是唯一永远不被压缩的层。

---

## 🚀 快速开始

```bash
python -m pip install -e ".[dev]"

# 不需要 API key。建一个 Goal、中途崩溃、恢复、改一个上游文件——
# 看只有受影响的子图被重新验证：
lhos demo recovery-repair --json
```

---

## 🗺️ 路线图

<div align="center">

| 阶段 | 状态 | 内容 |
|---|---|---|
| **现在** | 🟢 已发布 | 研究 alpha · 单机 · DeepSeek Harness 集成 · 46 任务 **LongHorizonBenchmark（LHTB）** 配对评测 |
| **下一步** | 🟡 焦点 | Harness 无关的能力矩阵 · 会话内局部压缩 · 更丰富的策略遥测 |
| **真正的目标** | 🔭 远景 | 训练基座——续跑决策全部可记录、可回放、可按策略寻址。调度策略（乃至学习型模型）在 OS 内部针对真实长时程负载训练与评测 |

</div>

---

## 📚 文档

| | |
|---|---|
| **使用** | [用户与运维手册](docs/USER-OPERATOR-MANUAL.md) · [快速上手](docs/QUICKSTART.md) · [Python API](docs/sdk/PUBLIC-API.md) · [Harness 协议](docs/HARNESS-SESSION-PROTOCOL.md) |
| **原理** | [概念](docs/CONCEPTS.md) · [架构](docs/architecture/LONGHORIZONOS-CORE-V1.md) · [算力管理](docs/LONG-HORIZON-COMPUTE-MANAGEMENT.md) |
| **状态** | [发版说明](docs/releases/v0.1.0.md) · [实现状态](docs/IMPLEMENTATION-STATUS.md) · [路线图](docs/ROADMAP.md) · [更新日志](CHANGELOG.md) |

---

## 🧑‍💻 开发

```bash
python -m pytest -q -m "not slow"
python -m ruff check src tests && python -m ruff format --check src tests examples scripts
python -m mypy src/lhos
```

---

## 贡献 · 安全 · 许可证

[CONTRIBUTING.md](CONTRIBUTING.md) · [SECURITY.md](SECURITY.md) · [Apache-2.0](LICENSE)

---

## 致谢

**基准套件与 Harness 层。** 本版本 46 任务评测使用 **LHTB = LongHorizonBenchmark**（长时程 Agent 基准测试套件）。全部 46 个 Agent 会话基于 **DeepSeek Harness** 执行（见 `scripts/lhtb_dsh_harbor_agent.py` 中的 `LHTBDeepSeekHarnessAgent`）；配对实验桥接与切片级遥测闸门均以 DeepSeek Harness 为基线。LongHorizonOS 是在此 harness 与 benchmark 之上*做对照评测*，而非跑在其他任何 runtime 之上。

**模型与推理。** 全部实验由 [StepFun 构建者计划](https://platform.stepfun.com/builder-program) 赞助，提供模型 API 与推理额度（模型 `stepfun/step-3.7-flash`，凭证走 `STEPFUN_API_KEY` 环境变量）。

---

<div align="center">

**[⬆ 回到顶部](#longhorizonos)**

</div>
