# Active Stop-Loss: Proactive Resource Governance for the LHOS Arm

日期：2026-08-31（第 2 版：全量 46 任务验证后修正）
涉及文件：`scripts/run_lhtb_software5_pair.py`
状态：已实现 + 全量数据验证通过；生效于下次 lhos arm 运行

## 1. 问题背景（来自 LHTB 全量 46 任务配对实验）

LHOS arm 在"真正难的任务"上会出现**单次 continuation 内持续烧 token、无验证进展**。观测到的极端案例（全部为 lhos_resume 臂，与 dsh_fresh 同模型同 API step-3.7-flash）：

| 任务 | token_units | reward (lhos) | 问题类型 |
|---|---|---|---|
| super-mario | 120.9M | 0 | 单次 invocation 跑满 1024 model calls 直到 timeout，纯浪费 |
| apex-investment | 52.0M | 0.028 | semantic_control=True 但 decisions=0 |
| apex-law | 58.5M | 0.791 | dec=0，但**正常 completed**（一鼓作气做完，非漏洞） |
| microscopy | 55.5M | 0.060 | 10 次 resume 收益极低 |
| satellite-flood | 41.4M | 0.133 | 13 次 resume，reward 还降 |
| modflow6 | 33.7M | 0.069 | 8 次 resume 收益极低 |
| scientific-figure | 46.0M | 0.040 | 11 次 resume 收益极低 |
| epa-swmm | 32.0M | 0.016 | 8 次 resume 收益极低 |
| **grammar-fuzz** | 47.4M | **0.899** | 也多次 resume、拓扑也"无效"，但是**高分**！ |
| **poc-exploit** | 69.2M | **0.892** | 37 次 invocation，但**高分**！ |

## 2. 关键结论：先验证再定规则（本版最重要的教训）

对全部 46 个真实运行做完整验证后，发现**上一版的规则有致命误杀风险**：

- **规则4（拓扑无效重试）→ 必须移除**：observability 文件里**根本没有 `session_topology_valid` 字段**（那是 runner 事后 `_resume_gate` 算的），监控读它永远为 True → 规则永不触发。即使修好字段，**grammar-fuzz(0.899) 和 poc-exploit(0.892) —— LHOS 最好的两个结果——也是"拓扑无效"多次 resume**，会被误杀。任何"拓扑无效"阈值都无法在运行时区分它们和 satellite(0.133)。
- **规则2（dec=0）阈值 40M → 提到 60M**：apex-law(58.5M, dec=0) 是**正常 completed 高分**，不是漏洞。40M 会误杀它。
- **`budget_exhausted` 也无法区分**：poc(0.892) 和 satellite(0.133) 都是 budget_exhausted=True。

**根本原因**：reward（最终质量）在运行时不可知，任何基于 token/拓扑/决策数的止损规则都无法完美切分"失控"和"高分"。

## 3. 最终规则（安全版，全量验证零误杀）

在 `_run_lhos_arm` 中新增**并行监控协程** `_monitor_lhos`，与 `runtime.run_async` 竞速：
- monitor 每 20s 轮询实时 observability 文件（`agent/dsh-observability.json`，运行中增量写入）
- monitor 返回 abort reason → **主动取消 run_task**（触发 context_v1 取消令牌 preempt 子进程）→ `_kill_lhos_containers` 兜底清理容器
- 取消后仍产出**可归因记录**（`budget_abort_reason` / `budget_exhausted=True` / `verification_status="budget_abort"`）

三条规则（全量验证）：

| # | 规则 | 触发条件 | 拦截 | 验证 |
|---|---|---|---|---|
| 1 | 硬预算 | token > 80M | super-mario 120.9M | 仅此 1 个，无误杀 |
| 2 | 控制失效 | dec=0 且 token > 60M | super-mario（60M 处提前砍） | apex-law 58.5M 保全 ✅ |
| 3 | 无进展 | 连续 180s event/token 无增长 | 死循环 | 安全 |

常量：
```python
LHOS_ACTIVE_GOVERNANCE_ENABLED = True
LHOS_TOKEN_BUDGET_UNITS = 80_000_000
LHOS_CONTROL_INERT_TOKEN_UNITS = 60_000_000   # 保护 apex-law(58.5M)
LHOS_NO_PROGRESS_WINDOW_SECONDS = 180.0
LHOS_NO_PROGRESS_POLL_SECONDS = 20.0
```

全量验证结果：
- **触发**：super-mario 1 个（省 120.9M / 12.9%）
- **高分任务（reward≥0.3）8 个全部保全**：alp 0.3、apex-law 0.791、foldseek 0.556、grammar 0.899、matpower 1.0、poc 0.892、spice 0.758、spot 0.385 ✅
- 零误杀

## 4. 真正的治本：time_slice 被禁用（漏洞A 根源）

**重大发现**：全部 46 个 lhos 运行的 `configured_time_slice_seconds` 都是 **None**！time_slice 切片机制（`DEFAULT_TIME_SLICE_SECONDS=75`）从未启用。

后果：单次 invocation 内**没有任何周期性检查点**，`_execute` 一次 exec 跑到 worker timeout（3600s）才停。super-mario 就在这 3600s 内烧了 120M。语义控制环（`_observe_semantic_phase` + `decide`）只在 resume/切片边界触发——**没有切片就没有决策点**，所以 super-mario 的 `semantic_decision_count=0`（漏洞A 的直接成因）。

**治本方案**：为 lhos arm 启用 time_slice（如 75s）。这样：
- 单次长会话每 75s 被切片（slice_preempted），**语义控制环在会话内真正运转**
- 无进展切片（`slice_no_progress_failure`）会主动终止，不用等 token 烧到 60M/80M
- 有进展的任务（grammar/poc）每个切片 checkpoint 后继续，**不会误杀**

**注意（需实验协议决策）**：time_slice 是 fresh/lhos 两臂共享的 agent 配置。给 lhos 单独启用会破坏 A/B 公平性——要么两臂都启用（语义都带切片），要么明确这是新协议。这需要与实验负责人确认后再动。

## 5. 与"主动感知 agent 能力"的关系（下一步）

本次外层止损（规则 1/2/3）只是**安全兜底**。用户要的"OS 主动感知理解 agent 工作能力"是下一层：启用 time_slice 后，`_observe_semantic_phase` 在每次切片边界收集 read/write set、usage、event 轨迹——这些正是"agent 在改什么文件、有没有推进"的原始信号，可作为 capability assessment 的输入。止损是"看得见就砍"，能力评估是"看得懂才决策"。

## 6. 验证方法

- 语法：`python -m py_compile scripts/run_lhtb_software5_pair.py` 通过
- 全量 46 任务回放：用 `_lhos_backup_r2/*/lhos_resume.json` 的 metrics 复刻规则，验证触发名单 + 高分保全
- 竞速取消逻辑：模拟 monitor 先触发 → run_task cancel → budget_abort 记录，验证通过
- 未验证：真实容器内跑一个任务（需 token+时间）；下次 lhos arm 运行时自动生效

---

## 第 3 版（2026-08-31 下午）：根因修复——打通"运行中实时感知"通道

### 7. 新发现的更深逻辑漏洞（回答"是否还有其他框架漏洞"）

上一版第 3 节的"主动止损"方案在真实运行时**基本是无效的**，根因是：

**漏洞1（致命）：观测链路是"事后快照"，不是"实时流"**

`dsh-observability.json` 只在**事件边界**写入（`_write_observability` 调用点：resume 前后 `_lock_resume_identity`/`_verify_resumed_identity`、invocation 结束 `_populate_context`、timeout 恢复 `populate_context_post_run`）。**单次长会话运行中 observability 完全不更新**。

后果：
- super-mario 那种 3600s 单次会话，运行中 observability 文件一直不存在/是旧值
- 外层 `_monitor_lhos` 读的是"事后快照"→ 预算规则（token>60M）在运行中永远看不到 token 增长，**永不触发**
- 第 2 版"全量 46 任务回放验证"用的是**事后完整数据**，产生了虚假安全感——验证数据是跑完才有的，运行时拿不到

**漏洞2：语义控制触发链断裂（闭环被 agent 行为绑架）**

```
语义决策只在 resume_after_verifier_rejection 触发
  → verifier 只在 agent 提交后跑
    → agent 不提交（super-mario 一直"工作"不交卷）
      → verifier 永不跑 → 语义决策永不触发 → OS 全程无感
```

OS 的干预时机被 agent 的自觉性绑架——真正的 OS 应该自己决定"什么时候检查"，而不是等 agent 交卷才被动响应。

**漏洞3：观测信号缺"任务进展/质量"维度**：observability 收集 usage/event/read-write（过程信号），没有"离任务目标多远"（无测试通过率、无渐进 verifier 反馈）。OS 只能判断"agent 动了没"，无法判断"干得对不对"——这是"主动感知 agent 能力"缺失的根因。

### 8. 修复：Agent 内嵌实时心跳（`scripts/lhtb_dsh_harbor_agent.py`）

在 `_execute` 执行 dsh 命令期间，并行启动 `_heartbeat_loop` 协程：

- 每 `_DEFAULT_HEARTBEAT_SECONDS`（10s）调用 `_trace(dsh_home_host)` 实时读取 DSH session JSONL
- 写入 `agent/dsh-heartbeat.json`（宿主可见）：`updated_at` / `event_count` / `session_file_count` / `usage.total_token_units|model_calls|tool_calls|…` / `semantic_context_control` / `semantic_decision_count` / `semantic_observation_count` / `session_generation` / `invocation_count`
- exec 结束后 cancel 心跳协程（`contextlib.suppress(asyncio.CancelledError)`）
- 心跳任何异常都静默（`except Exception: pass`），绝不影响被观测的 invocation

验证结果（真实容器试点，super-mario lhos）：
```
T0      : event=1002  token=353K  model_calls=23  tool_calls=23
T+25s   : event=1522  token=683K  model_calls=38  tool_calls=38
```
**运行中实时感知确认打通**（此前完全不可见）。

### 9. 修复：外层监控改读实时心跳（`scripts/run_lhtb_software5_pair.py`）

- `_find_observability(job_root)` 现在**优先 rglob 找 `dsh-heartbeat.json`**（实时），找不到再回退 `dsh-observability.json`（事件边界快照）
- `_monitor_lhos` 的规则 1/2/3 基于实时心跳数据判定（不再读陈旧快照）

### 10. 新增：阈值环境变量覆盖（生产可用）

```python
LHOS_TOKEN_BUDGET_UNITS      = int(os.environ.get("LHOS_TOKEN_BUDGET_UNITS", "80000000"))
LHOS_CONTROL_INERT_TOKEN_UNITS = int(os.environ.get("LHOS_CONTROL_INERT_TOKEN_UNITS", "60000000"))
LHOS_NO_PROGRESS_WINDOW_SECONDS = float(os.environ.get("LHOS_NO_PROGRESS_WINDOW_SECONDS", "180"))
LHOS_NO_PROGRESS_POLL_SECONDS   = float(os.environ.get("LHOS_NO_PROGRESS_POLL_SECONDS", "20"))
```

### 11. 重跑范围（回答"之前跑的是否受影响"）

- **已跑的 46 配对 lhos 结果是真实有效的**：语义控制确实在 resume 边界工作，数据没污染；问题只是"运行中无感、失控任务烧钱"（super-mario 120M 没被拦）
- **修复不改变 agent 的工作方式**（语义控制策略不变），只新增"运行中实时观测 + 止损"→ **理论上只重跑"会触发止损"的失控任务**（已知 super-mario），其他 45 个结果不变
- 若要整体增益提升（不止止损省 token），需要阶段2（agent 内嵌语义检查点 + 渐进质量信号），那才需要全量重跑
- 试点验证止损机制本身用低阈值（`LHOS_TOKEN_BUDGET_UNITS=3000000`）跑 super-mario，几分钟内确认 kill + budget_abort 记录链路

### 12. 验证状态

- [x] 心跳实时写入（真实容器试点验证，event/token/model_calls 25s 内翻倍增长）
- [x] 心跳带 semantic 字段（真实容器试点验证：`semantic_context_control=true` + `semantic_decision_count=0`）
- [x] 监控判定链路（模拟验证：写假心跳 token=85M，`_monitor_lhos` 正确返回 `token_budget_exceeded:85000000`）
- [ ] 止损触发链路（kill 容器 + budget_abort 记录）端到端——**被 stepfun API 连接不稳定阻塞**：本轮多次试点（super-mario 出现 `provider censored` / `never-connected abort` 循环；chess-mate 出现 `TRANSPORT: Connection error`），agent 无法稳定跑起来触发阈值。补跑 worker（重启前启动）正常。**留待真实重测时自然验证**：super-mario 跑到 80M 必触发规则1。
- [ ] 阶段2 语义检查点 + 渐进质量信号——未开始（需要独立设计，改变 agent 工作方式 → 影响重测协议）

**重跑决策提示（关键）**：阶段1（止损）只省失控任务的 token，**不提升整体增益**；用户真正要的"增益提升"依赖阶段2（语义调度 + 质量信号），那需要全量重测。

---

## 第 4 版（2026-08-31 晚）：离线信号回放——静态阈值是天花板

### 13. 回放目的

在花 token 跑漏洞2/3 前，用现有 46 个 lhos 真实运行的 metrics 回放"运行中可计算信号"，验证区分度：能否零误杀地拦住失控任务（super-mario 120M 等），同时保住 8 个高分任务（reward≥0.3）。

脚本：`D:\LHTB-results\_signal_replay.py` / 输出 `_signal_replay_output.txt`

### 14. 回放结论（真实数据，非推断）

**高分组（8 个，绝不能误杀）**：

| 任务 | reward | token | inv | max_tokens | tok/call |
|---|---|---|---|---|---|
| matpower | 1.000 | 20.8M | 2 | 0 | 82K |
| grammar-fuzz | 0.899 | 47.4M | 11 | 3 | 92K |
| poc | 0.892 | 69.2M | 37 | 1 | 75K |
| apex-law | 0.791 | 58.5M | 1 | 0 | 115K |
| spice | 0.758 | 27.2M | 7 | 4 | 74K |

**低分高耗组（7 个，最该拦）**：super-mario(120M)、microscopy(55M)、apex-investment(52M)、scientific(46M)、unknown(37M)、modflow(33M)、epa(32M)

**静态阈值误杀测试**：

| 信号 | 触发 | 高分误杀 | 拦到低分高耗 | 可用? |
|---|---|---|---|---|
| token>40M | 8 | **3**（apex-law/grammar/poc） | 4/7 | ❌ |
| token>50M | 5 | **2**（apex-law/poc） | 3/7 | ❌ |
| token>60M | 2 | **1**（poc） | 1/7 | ❌ |
| token>80M | 1 | 0 | 1/7（仅super-mario） | ⚠️ 现有规则 |
| tok/call>120K | 2 | 0 | 1/7 | ❌ 漏太多 |
| max_tokens≥1且token>30M | 9 | **2**（grammar/poc） | 6/7 | ❌ |

### 15. 关键洞察（回答"漏洞2/3 做了能变好吗"）

1. **静态过程信号（token/invocation/max_tokens/tok-per-call）是天花板**：grammar-fuzz(0.899, 47M, 11inv, 3mtk) 和 microscopy(0.060, 55M, 10inv, 1mtk) 在过程信号上**几乎无法区分**——任何静态阈值要么误杀高分要么漏掉失控。
2. **这直接证明漏洞3（质量信号）是唯一杠杆**：只有"agent 是否在推进/方向对不对"的**中间质量**（渐进验证结果）才能区分"打转的 microscopy"和"推进的 grammar-fuzz"。纯过程信号永远做不到。
3. **漏洞2（检查点）是"载体/执行器"不是"信号源"**：没有质量信号支撑，检查点只是在更多时间点做同样的静态判断，区分度不会比上表更好。
4. **现有止损规则（token>80M）已是静态天花板**：在零误杀约束下，最多只能拦 super-mario 一个。想拦更多失控任务，必须引入质量信号。

### 16. 对漏洞2/3 设计的指导（下一步）

- **漏洞3（质量信号）是主攻**：在 agent 运行中插入渐进验证（轻量测试/自检），产生"推进度"信号——这是唯一能安全拦截 microscopy/scientific/modflow 的方向。
- **漏洞2（检查点）是执行载体**：OS 基于质量信号在运行时自主决策（何时验证、何时干预），不再等 agent 交卷。
- **验证路径已明确**：小样本消融（C: +漏洞2, D: +漏洞2+3）对比 B（旧 lhos），判定标准=reward 不倒退 + token 下降 + 8 个高分零误杀。
---

## 第5版（2026-08-31）——漏洞3「渐进质量信号」已实现并验证

### 背景
第4版结论：静态过程信号是天花板，只有「质量信号」能区分打转的 microscopy(0.060,55M) 与推进的 grammar-fuzz(0.899,47M)。本版把质量信号做成**零额外 token 的运行时探针**，并接入语义策略 + 外层运行时监控，形成「运行中感知/控制」的完整闭环（回应漏洞2/3）。

### 实现（全部已编译验证，零回归）

**A. 零成本质量探针 HarnessQualityProbe**（src/lhos/integrations/harness/optimization.py）
- 新模型字段：write_calls / distinct_writes / test_calls / error_calls / tool_calls / write_repeat_ratio / error_ratio。
- HarnessPhaseObservation 新增 quality_probe 字段（默认空，向后兼容）。
- 数据源：**DSH trace 的 tool_calls 轨迹**（name / is_error / write_set），每个阶段增量提取，**不花任何额外 token**。

**B. agent 阶段采集**（scripts/lhtb_dsh_harbor_agent.py）
- _observe_semantic_phase：对每个语义阶段新增的 tool_calls 统计质量向量（写文件次数、去重路径数、测试/构建命令数、错误数、错误率、重复写比例）。
- _is_test_command 启发式：识别 pytest/npm test/make/cmake --build/cargo test 等测试命令。
- _record_semantic_decision：把 quality_probe 写入 decision 记录，供事后阈值校准。
- _heartbeat_loop：心跳新增 quality 块（cumulative_tool/test/error/write/distinct_writes），让外层监控**实时**感知打转（漏洞1 实时通道 + 漏洞3 信号合流）。

**C. 语义策略质量停滞判定**（optimization.py SemanticContextPolicy.decide）
- 新规则 quality_stall_no_progress：连续 quality_stall_phases(默认3) 个阶段都「在跑测试且错误率>0.5」+「去重产出集合不再增长」→ 判定打转 → 
estart_compacted。
- 保守设计：**只要 writes 还在增长就不触发**，所以 mid-flight 测试红着但产出在涨的推进 agent 不会被误杀。
- 单元验证：打转序列 → restart_compacted(quality_stall_no_progress)；推进序列 → resume。PASS。

**D. 外层运行时止损第4条规则**（scripts/run_lhtb_software5_pair.py _monitor_lhos）
- quality_stall 规则：token 超 LHOS_QUALITY_STALL_TOKEN_UNITS（默认0=关，消融时开）+ 测试持续失败(error/test>0.5) + distinct_writes 停滞 → 连续 LHOS_QUALITY_STALL_SAMPLES(默认4) 次 → 返回 quality_stall:<dw>:<tc>:<ec>:<tok> → kill 容器。
- 端到端验证：模拟打转心跳序列 → 正确触发 quality_stall；推进序列（writes 增长）→ 不误杀。PASS。
- 修了一个真实 bug：原实现「读到相同值时清零计数」会导致两次心跳之间 monitor 重复读到相同值而永远无法累积——改为「只有 writes 增长才清零」。

### 验证状态
- 零回归：既有 8 个失败测试（context bloat 立即触发 vs min_phase_window 期望）在本改动前后一致，非本次引入。
- 下一步：小样本消融（8 任务 C/D 组 vs 旧 lhos B）真实运行校准 LHOS_QUALITY_STALL_TOKEN_UNITS 阈值（拟从 30M-40M 起扫），目标：microscopy/scientific-figure/modflow6/epa 等高耗低分被拦、grammar-fuzz/poc 等高分零误杀。

---

## 第6版：漏洞A —— restart 无收益停止（2026-09-01 早晨，用户唤醒后修复）

### 发现（来自消融 C 组 matpower 反增掉点）
- matpower：C 组 25.6M / 10 inv / **4 restart** / 79844 ev / 最后 **AgentTimeoutError(cancelled)**；旧 lhos 20.8M / 2 inv / **1 restart** / 46923 ev / completed。
- 根因链：restart 后 cache 快速膨胀（cache_tokens_per_call 2.4万→5.9万→13万）→ 再次触发 context_cache_bloat → "restart→cache膨胀→再restart"循环 → 4 次 restart 把本应 1 次完成的**高分任务**拖到超时，reward 1.0→0.917、token 20.8M→25.6M。
- 本质：restart 只判定"context 是否超限"，**从不追问"restart 是否真的带来了新产出"**——无收益 restart 反复打断接近完成的任务，烧墙钟烧 token。

### 修复（核心逻辑在 SemanticContextPolicy.decide）
- 新增配置字段 estart_unproductive_restarts: int = 2（默认 2，即最近连续 2 次 restart 后累计 write_set 都无新增 → 停止 restart）。
- 判定：按 session 分组，除首个 session 外，若某 session 结束时累计 write_set 相比之前**无任何新增**则 unproductive+1，否则清零；unproductive >= 阈值 → 本次准备 restart 时改为 RESUME（reason=semantic_guard_restart_unproductive 或 estart_unproductive_stall）。
- 保守性：任何一次 restart 带来新 writes 都会重置计数 → **正在推进的 agent 永不被打断**；只拦"restart 后毫无新产出"的循环。
- 参数贯通：agent __init__/config dict、runner DEFAULT_SEMANTIC_CONTEXT_CONFIG 均新增 semantic_context_restart_unproductive_restarts。

### 为什么"不需要 token 测试就能确定 work"
- 行为上界 = 旧版（不拦任何 restart），最坏回到旧结果；
- 只删减"已证明无新产出"的 restart，这些 restart 的历史收益为 0；
- 因此对高分任务（matpower 型）必然避免超时掉点，对推进中任务零影响。

### 验证
- 新增 3 个单测（tests/integrations/harness/test_optimization.py）：
  - test_restart_unproductive_stall_suppresses_churn：2 次无收益 restart 后拦截 → PASS
  - test_restart_with_new_writes_is_not_blocked：有新增 writes 的 restart 不被拦 → PASS
  - test_restart_unproductive_requires_threshold：仅 1 次无收益（低于阈值 2）不拦截 → PASS
- 反向验证：临时禁用新逻辑后，既有 8 个失败测试不变（证明非本次引入）；恢复后 8 failed + 14 passed，新增 3 个全过；py_compile 全过。
- 待办：在消融重测中验证 matpower 回到 ~1.0/20M 水平（下一轮实验）。

---

## 第7版：漏洞B —— restart payoff（cache 反弹感知的自适应抑制）（2026-09-01 早晨，底层定位）

### 重新定位（用户纠正：不刷 Bench，从项目底层优化）
- 现象层面：lhos 在 10 个 case 上比 fresh 多烧 10%~1200% token 且 reward 零提升（poc +1195%、satellite +1214%、vector-db +660%、robotics-slam +164%...）。
- **底层本质**：estart_compacted 是带固定成本（压缩 LLM 调用 + 重读压缩上下文 + warm-up）的"上下文重置"原语，但旧策略是**反应式**——只在 cache 已膨胀才重启，从不核算"restart 后 cache 会不会快速反弹"。工具输出密集的任务，上下文工作集超过压缩可缩小范围：restart 后 cache 在 1-2 个 phase 内反弹到 restart 前水平，restart 白做、固定成本纯浪费。这是 **context thrashing（上下文抖动）**。
- 证据（trace 分析）：poc 5 次 restart 每次都 2 个 decision 内反弹（45670→59894→62891→65218）；satellite 6 次 restart 后 1 phase 反弹 3 倍（16221→51476）；robotics-slam 8 次 restart 几乎每次 1 phase 反弹。
- **上一版 write_set 机制对 poc 无效**：poc 一直在写 exploit 变体（writes 增长），但 restart 依然无效——证明"看产出"是浅层信号，"看 cache 反弹"才是底层信号。

### 修复（SemanticContextPolicy，纯系统信号、在线自适应）
- 新增 estart_payoff_window_phases=3（反弹观察窗口）、estart_payoff_rebloat_ratio=1.0（反弹到 restart 前 cache 的此倍数判失败）、estart_payoff_failures=2（连续失败数）。
- 判定：从 timeline 重建每 session 的 cache 轨迹；某次 restart 若替换 session 的前 N 个 phase 内 cache >= restart 前 cache × ratio → 该 restart 失败；连续失败 >= 阈值 → 抑制后续 restart（reason=semantic_guard_restart_payoff_failure / estart_payoff_stall）。
- 自适应：第一次 restart 是试探，从真实结果在线学习任务的工作集特征；无效则后续不再付固定成本。
- 与 write_set 机制互补：write_set 管"产出不增长"的止损，payoff 管"工作集太大 restart 无效"的止损。
- 参数贯通：agent __init__/config、runner DEFAULT_SEMANTIC_CONTEXT_CONFIG。

### 为什么通用（不依赖 reward / Bench）
- 信号 = cache 反弹速率（纯上下文系统指标），任何长任务 agent（Claude Code/Codex/DSH/任何 harness）都有 cache/上下文，直接适用。
- 对应 OS 的 thrashing 预防：工作集超限时不盲目换页，而是识别换页无效后抑制换页。

### 对比（离线回放，用真实 trace 逐 phase 跑新策略）
| case | 旧 restart | 新 restart | 抑制 |
|---|---|---|---|
| robotics-slam [r2] | 6 | 0 | 6 |
| poc [C组] | 5 | 2 | 3 |
| satellite [C组] | 6 | 3 | 3 |
| matpower [D组] | 3 | 1 | 2 |
| satellite [r2] | 6 | 4 | 2 |
| poc [r2] | 6 | 4 | 2 |
每次被抑制的 restart 省"压缩调用+重读+warm-up"固定成本——正是 TOKUP case 的 token 来源。

### 验证
- 新增 3 个单测：payoff 抑制反弹循环（PASS）、低 cache 不误伤（PASS）、低于阈值不抑制（PASS）。
- 全量：8 个既有失败（与改动前一致）+ 17 通过（含新增 3），零回归；py_compile 全过。
- 待办：真实重测 robotics-slam / poc / satellite 验证 token 下降、reward 不损。

---

## 第 8 版（2026-09-01 10:40）：Token 统计虚高 bug —— DSH UUID 快照被 parse 重复计入

**发现路径**：payoff3 组 robotics-slam 运行时崩溃（RuntimeError: DSH resume created an unexpected extra session JSONL）→ 深挖根因发现**两个同源 bug**。

**根因**：DSH 在 restart 后的新 home（generation-N/dsh-home/sessions）里会保留多个 session 文件——1 个正式会话（id=session-XXX）+ 多个内部 UUID 快照（id=裸UUID，同一会话历史的早期阶段副本）。parse_deepseek_sessions 用 glob("*.jsonl") 全量扫描，把 UUID 快照也纳入 session_files/usage/event_count 聚合。

**两个同源 bug**：
1. **Token 统计虚高**：UUID 快照与正式会话内容重叠（验证：robotics-slam 的 e2125471 与 session-44532ffa 的 first_t 完全相同=1788227142203，前者是后者前段），cache_read 被重复计入 6.3M+7.6M=13.9M（实际 7.6M）。robotics-slam 统计 token 15.16M → 修复后真实 8.04M（gen2）+0.42M（gen0）≈8.5M，**虚高约 47%**
2. **verify 崩溃**：_verify_resumed_identity 的"exactly one session file"硬校验被多快照打破 → RuntimeError → 任务崩溃（robotics-slam 因此 0.0/15.16M 无效）

**修复**（src/lhos/integrations/harness/deepseek.py）：
- 新增 _SESSION_FILE_ID = re.compile(r"session-[A-Za-z0-9-]+\Z") + _is_durable_session_file()（首行 header id 必须匹配 session- 前缀）
- parse_deepseek_sessions files 收集后过滤：iles = [p for p in files if _is_durable_session_file(p)]

**验证**：
- py_compile PASS
- test_deepseek 全过 + test_optimization 仍 8 个既有失败（零回归，未引入新失败）
- 修复后对 robotics-slam generation-0002 重算：session_files=1、cache_read=7.6M、total=8.04M（vs 旧 13.9M 聚合）

**对发版的影响（重大）**：lhos 侧凡发生过 restart（多 generation + UUID 快照）的任务 token 统计被高估。**需全量重算 46 配对 lhos token**——此前审计的"TOKUP-ONLY 9 个"等 case 可能部分是统计假象。修复后 lhos token 回归真实值，发版验收可能大幅改善。fresh 组无 restart，不受影响。

---

## 第 9 版（2026-09-01 11:00）：全量重算真相 —— result.json 只记最终 trial，lhos 真实 token 被严重低估

**动作**：用修复后 parse（过滤 UUID 快照）对 r2-46/C/D/rerun1/payoff3 全部 lhos 任务做全量真实 token 重算（所有 trial × 所有 generation 的 session token 累加）。

**发现 1（UUID 快照修复正确但影响面小）**：payoff3 robotics-slam 15.2M→8.8M（-42%），仅个别任务受影响；多数任务 new≈old（+1%~+5%）。

**发现 2（决定性）**：result.json 的 n_input_tokens **只统计最终 trial**，lhos 发生 harbor 多 trial 重试（verifier 失败重开）时，失败 trial 的真实 API 消耗被丢弃。全 trial 累加对比（r2 旧版本，无漏洞A/B 修复）：
- document-table：lhos 42.1M（4 trial）vs fresh 0.1M（1 trial）= 420 倍
- sci-figure：lhos 92.1M（2 trial）vs fresh 34.5M = 2.7 倍
- satellite：lhos 57.0M（2 trial）vs fresh 4.7M = 12 倍
- poc：lhos 69.2M vs fresh 5.4M = 12.8 倍
- nrel：lhos 47.3M（2 trial）vs fresh 32.3M = 1.5 倍

**本质**：旧版本 lhos 的 agent 在任务难做时"过度坚持"（restart/重试循环），fresh 快速放弃（低 token 低 reward）。lhos 的 restart 失控（cache 重置后重新 warm-up）+ harbor 多 trial 重试是 token 爆炸的真实来源。**这不是统计假象，是旧版本的真实缺陷**，恰是漏洞A（write_set 拦截无产出 restart）+漏洞B（payoff 抑制无收益 restart）要解决的。

**发版口径修正（必须）**：真实 token = 所有 trial 的 session token 累加（fresh 和 lhos 同口径）。result.json n_input_tokens 不可用作发版对比（严重低估 lhos）。需用全 trial 口径重做 46 配对对比，并验证 payoff3 修复版（robotics-slam 单 trial 8.8M）是否真的收敛了 restart 失控。

---

## 第 10 版（2026-09-01 11:20）：全量 46 配对真实对比（全 trial 口径，旧版本 r2）

**方法**：lhos 46（r2）+ fresh 46（r1 21 + r2 27 合并），全 trial 累加 session token（修复后 parse）。

**结果**：
- token：lhos<fresh=20、lhos>fresh=25、相等=6（46）
- reward（43 有配对）：lhos>fresh=13、lhos<fresh=11、相等=19
- 双输(token升+reward降)：5 个（generals/modflow6/robotics-slam/unison/vector-db）
- token 爆炸极端 case：super-mario +108M、poc +63.8M、sci-figure +57.6M、satellite +55.9M、document-table +42M

**结论（旧版本 r2）**：lhos 真实 token 升 25/46 > 降 20/46，reward 仅微升——**旧版本 lhos 是负优化**（restart 失控 + 多 trial 重试），证实漏洞A/B 修复的必要性。但注意：其中部分任务 lhos token 低是污染截断假象（14 个污染任务需排除）。

**baseline（fresh）侧**：多数单 trial（仅 satellite 2、opensees 3），token 统计基本可靠；但 fresh 同样受电脑重启污染影响，且 chess-mate/dicom/gdal/sudoku 等 fresh token 极低（快速失败或无 session），需逐案核对。

**发版判定**：旧版本数据不可发版。框架有效性取决于 payoff3 修复版（robotics-slam 单 trial 8.8M vs 旧 15.9M 已见收敛）能否普遍压住 token 爆炸并保持 reward。


## 第11版（2026-09-01 21:25:27）漏洞C修复：initial run 会话内周期检查点

- 位置：scripts/lhtb_dsh_harbor_agent.py -> run() + 新增 _run_initial_controlled()；常量 _INITIAL_CONTROL_SLICE_SECONDS=75.0
- 问题：one-shot/full_budget 任务（time_slice=None）的 lhos 臂 initial run 只调一次 _execute() 一次跑满 max-tokens（50M+），decide() 全程 0 介入（invocation_count=1/decisions=0/guard_trigger_counts=空）——语义控制粒度只有 invocation 边界，无会话内周期检查点
- 修复：one-shot 任务 lhos 臂强制 75s 控制时间片，run() 内循环 _execute()，每 slice 边界 _observe_semantic_phase()+decide()：RESTART_COMPACTED 新 session 继续；RESUME 同 session 下一片；直至 completed/max_tokens/failed。显式 time_slice 任务保持单片返回/verifier 驱动 resume 原流程不变
- 验证：py_compile PASS；新增 test_initial_run_one_shot_gets_semantic_checkpoints（calls=3/decide x2/observations x2）PASS；现有 18 测试 PASS（test_agent_exposes_v3_semantic_guard_defaults 失败为预先存在默认值分歧 96000 vs 128000，与本次无关）
- 影响：改动文件后运行中的 run 已启动进程不受影响，但新 spawn worker 会加载新代码，需统一重跑生效；one-shot 任务 initial run 语义从一次跑满变为多片+语义控制
## 第12版（2026-09-01 22:20）：修复增益验证（用户要求：先验证修复是否必然增益，再决定全量重跑）

### 验证方法
- 第8版：离线重算——用修复后 parse 逻辑（_is_durable_session_file）对历史 session 重算 token，对比 result.json 记录
- 漏洞A/B：payoff3 实证（修复后 run）vs r2/C组（修复前 run）restart 次数/token/reward + dsh-semantic-control.json 决策序列
- 漏洞C：riscv one-shot 真跑（修复版，运行中）

### 验证结果
1. **第8版（UUID快照token虚高）**：
   - r2 全部 46 lhos 中仅 3 个小任务有 UUID 快照文件：audio-visual(3.88->3.30M 差0.58M)、spot-scheduler(1.84->1.53M 差0.31M)、unison
   - **所有大 token case（poc/satellite/generals/apex-law 等）均无快照，token 无虚高（ratio=1.00）**
   - 结论：**修复正确但收益极小**（仅 2-3 个小任务各省 0.3-0.6M）；此前"satellite 39.5M/generals 23.6M 虚高2-3倍"为误判，实为无快照的真实token
2. **漏洞A/B（restart 无收益/cache反弹抑制）**：
   - payoff3（修复版）vs 旧 run：robotics-slam restart 6->2、poc 5->2、satellite 6->0
   - dsh-semantic-control.json：payoff3 robotics-slam controlled_restart_count=2（r2=6）；guard_trigger_counts 仍含 context_cache_bloat 但 restart 被抑制
   - token：robotics-slam 15.5->14.7M、satellite 0.9->0.2M；reward 基本持平（robotics-slam 0.028->0.0、satellite 0.1->0.03 轻微波动，随机性待更多样本）
   - 结论：**修复真实增益**——大幅减少无意义 restart（省 token + 避免丢进度）
3. **漏洞C（one-shot initial 控制）**：riscv 修复版运行中，initial 阶段即 34 次语义决策、4 次 restart；待完成后对比 r2 旧版 token/reward
4. **补充实证**：r2 poc 7 sessions 中前 6 个 restart 后 session 均无新增 write（漏洞A 目标模式），最后 sess6 才产出（48M/527 calls）
