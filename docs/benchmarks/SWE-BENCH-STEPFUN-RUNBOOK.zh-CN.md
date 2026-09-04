# SWE-bench + StepFun 运行手册

## 当前已跑通的 case

```text
instance: pytest-dev__pytest-11143
repo: pytest-dev/pytest
base: 6995257cf470d2143ad1683824962de4071c0eb7
input: text only
model: step-3.7-flash
reasoning_effort: medium
harness: DeepSeek Harness 0.1.0-rc.8
```

StepFun 这个 key 应走 Step Plan 通道：

```text
https://api.stepfun.com/step_plan/v1
```

已实际验证：

```text
/step_plan/v1 + step-3.7-flash = HTTP 200
/v1           + step-3.7-flash = HTTP 402
```

本测评直接固定 `step-3.7-flash`，不使用 `step-router-v1`，避免两个 arm 被路由到
不同底模。主实验固定 `reasoning_effort=medium`；`high` 只用于后续小子集消融。
DSH rc.8 需要 Node 24；本机的 Node 20.19.0 不兼容。

## 1. 凭据

密钥只放当前 PowerShell 进程。推荐交互式读取，避免进入 shell history：

```powershell
$secureKey = Read-Host "STEPFUN_API_KEY" -AsSecureString
$env:STEPFUN_API_KEY = [System.Net.NetworkCredential]::new("", $secureKey).Password
```

不要把 key 写入 YAML、JSON、Markdown、命令行参数或 artifact。运行结束后：

```powershell
Remove-Item Env:STEPFUN_API_KEY
```

用户曾把 key 发到聊天中，应在完成实验后轮换。

## 2. 生成 patch

仓库内的 StepFun Cordis 配置：

```text
benchmarks/real_dsh_dynamic_coding/stepfun-3.7-pi-ai.cordis.patch.yml
```

先准备固定 commit 的干净 checkout 和独立 Python 环境：

```powershell
$source = "C:\Users\yangjiashu\Temp\swebench-pytest11143"
$venv = "C:\Users\yangjiashu\Temp\swebench-pytest11143-venv"

git clone https://github.com/pytest-dev/pytest.git $source
git -C $source checkout 6995257cf470d2143ad1683824962de4071c0eb7
python -m venv $venv
& "$venv\Scripts\python.exe" -m pip install -U pip
$env:SETUPTOOLS_SCM_PRETEND_VERSION_FOR_PYTEST = "8.0.0"
& "$venv\Scripts\python.exe" -m pip install -e "${source}[testing]"
Remove-Item Env:SETUPTOOLS_SCM_PRETEND_VERSION_FOR_PYTEST
```

然后运行仓库内的包装脚本。它会安全读取 key，依次跑 static/LHOS、导出
prediction，并生成 profiling：

```powershell
.\scripts\run_swebench_stepfun_pytest11143.ps1 `
  -SourceRepo $source `
  -EvalPython "$venv\Scripts\python.exe"
```

这个 runner 会：

1. 复制两个相同 base workspace；
2. 用同一 StepFun model 和同一 DSH executor 分别跑 static 与 LHOS；
3. Agent 完成后才注入公开 `test.patch`；
4. 记录 Agent 原始 source patch、provider usage 和 LHOS Goal 状态；
5. 用公开目标测试做 host-native verifier。

它是 paired smoke/proxy，不是官方 leaderboard evaluator。

## 3. 生成 profiling

```powershell
python scripts/profile_swe_case.py `
  artifacts\swe-stepfun37-pytest11143-YYYYMMDD\result.json `
  --model step-3.7-flash `
  --reasoning medium `
  --harness "DeepSeek Harness 0.1.0-rc.8"
```

输出：

```text
case-profile.json
CASE-PROFILE.zh-CN.md
```

profile 会拆分：

- uncached input、cache read、output；
- model call、tool call、各工具类型；
- tool execution、retry delay、model/context/provider waiting；
- OS 可归因部分与随机模型轨迹部分。

## 4. 官方 SWE-bench evaluator

prediction 必须是 JSON list，每个元素包含：

```json
[
  {
    "instance_id": "pytest-dev__pytest-11143",
    "model_patch": "<git diff>",
    "model_name_or_path": "step-3.7-flash-medium+dsh+longhorizonos"
  }
]
```

本次已生成：

```text
artifacts/swe-stepfun37-pytest11143-20260820/predictions.json
```

SWE-bench 5.0.2 单例命令：

```powershell
swebench eval "SWE-bench/SWE-bench_Lite" `
  -p artifacts\swe-stepfun37-pytest11143-20260820\predictions.json `
  --run-id stepfun37-pytest11143-20260820 `
  -i pytest-dev__pytest-11143 `
  -j 1 `
  -t 1800 `
  --report-dir artifacts\swe-stepfun37-pytest11143-20260820\official-report
```

官方 image：

```text
swebench/sweb.eval.x86_64.pytest-dev_1776_pytest-11143:latest
```

截至 2026-08-20，本机 Docker daemon 正常，但官方 image 从当前 registry mirror
拉取返回 403，direct registry 出现 EOF；本地构建也卡在 Miniconda 下载。因此当前
不能把 host-native 结果称为 canonical SWE-bench score。

StepFun 生成的 patch 已用替代 Linux 容器复核完整
`testing/test_assertrewrite.py`：

```text
alpine@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc
116 passed in 6.58s
```

这是 Dockerized Linux validation，不是官方固定 image digest 的 leaderboard 分数。

## 5. 当前 3-case Lite pilot 结果

```text
summary: artifacts/swe-lite-stepfun37-3case-20260820/summary.json
report:  artifacts/swe-lite-stepfun37-3case-20260820/SUMMARY.zh-CN.md
```

| Case | DSH static | DSH + LHOS |
|---|---:|---:|
| `pytest-dev__pytest-11143` | resolved | resolved |
| `sphinx-doc__sphinx-11445` | resolved | resolved |
| `pallets__flask-4992` | unresolved | unresolved |

```text
DSH static resolved: 2/3
DSH + LHOS resolved: 2/3
```

两个 pair-valid case 的观测总量：

| 指标 | Static | LHOS | LHOS-Static |
|---|---:|---:|---:|
| Token units | 1,338,031 | 1,480,354 | +10.6% |
| Model calls | 52 | 53 | +1 |
| Tool calls | 50 | 51 | +1 |
| Sum wall | 491.391 s | 684.062 s | +39.2% |

这组结果没有显示单 Task 加速。Sphinx 的 LHOS 独立轨迹明显更长，超过了 pytest
轨迹中的观测减少；Flask 两边都误解了公开 API contract，把接口实现成 `mode=`
而测试要求 `text=False`，verifier 正确拒绝了两个 patch。

这些 case 能证明：

```text
StepFun -> DeepSeek Harness -> LongHorizonOS -> independent verifier
```

链路已经打通，并且 resolved case 中 LHOS 持有最终 VERIFIED/closed 权限。但三个
case 都是单 READY Task，没有可复用分支或 selective repair，所以这里的 token/time
正负差都不能归因于 OS。要测 OS 加速，必须把多个公开 task 组成 dynamic episode，
在 requirement/artifact 变化后测 preserved Evidence、invalidation cone、
repair frontier 和 stale computation。

先前的 `step-3.5-flash-2603 + reasoning=low` 结果保留在：

```text
artifacts/swe-stepfun-pytest11143-20260820
```

它只属于 provider/DSH 接入 smoke，不进入统一模型的主实验表。

## 6. 从小到大的测评顺序

### Gate 0：最小统一配置，已完成

```text
1 个纯文本 case × 2 arms × 1 paired run
model = step-3.7-flash
reasoning_effort = medium
```

目的只是确认模型、Harness、workspace、patch export、verifier 和 profiling 全部打通。
本阶段不能估计加速，也不能衡量 OS selective scheduling。

### Gate 1：小规模稳定性

```text
3 个纯文本 SWE-bench case × 2 arms × 3 paired repetitions = 18 次 Agent run
```

两个 arm 固定完全相同的：

- model ID、endpoint、reasoning effort、max tokens；
- DSH 版本、prompt、tool set、permission mode；
- base commit、test patch、timeout、并发和 verifier；
- API key pool 与 rate-limit policy。

每个 repetition 交替执行顺序：

```text
repeat 1: static -> LHOS
repeat 2: LHOS -> static
repeat 3: static -> LHOS
```

这个阶段估计随机轨迹方差和 correctness，不把单 Task 差异归因于 OS。

### Gate 2：真正测 OS

```text
1 个公开仓库
3-5 个有依赖/独立分支的 task
1 次 requirement 或 artifact mutation
static restart vs LHOS selective repair
至少 5 个 paired repetitions
```

主指标：

- Time-to-Verified-Goal；
- provider token units；
- model/tool calls；
- preserved Evidence；
- invalidation cone 与 repair frontier；
- stale computation 和 Agent-slot occupancy；
- critical-path 与非 critical-path 节省。

只有这个阶段才能回答 LongHorizonOS 的资源调度是否真正减少了计算。

### Gate 3：thinking 消融

主表保持 `medium`。在 Gate 2 的一个 episode 上追加：

```text
reasoning_effort = high
2 arms × 3 paired repetitions
```

用来回答 OS 收益是否会随着单次推理成本上升而放大，不把两个 thinking 档混在同一主表。
