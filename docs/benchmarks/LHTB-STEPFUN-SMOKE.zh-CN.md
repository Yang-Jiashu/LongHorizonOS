# LHTB + StepFun 测试记录

## 当前结论

LHTB 的 Docker task/verifier 链路已经在本机验证成功；一个真实
StepFun Agent trial 也已经启动并完成，但在本地缩短的 900 秒预算内没有完成任务。

这不是 LongHorizonOS 的失败结果，也不是 LHTB 官方分数；它是外部 benchmark
接入和长时程计算规模的 smoke。

## 1. Docker/oracle smoke

任务：

```text
document-table-layout-reconstruction
```

结果：

```text
Docker image build: passed
healthcheck: passed
hidden oracle verifier: reward 1.0
```

Harbor job：

```text
C:\Users\yangjiashu\Temp\LHTB-one-local-jobs
```

## 2. 真实 StepFun agent

任务：

```text
langchain-version-migration
model: openai/step-3.7-flash
provider: https://api.stepfun.com/step_plan/v1
Harbor: 0.20.0
local agent budget: 900s
```

结果：

```text
reward: 0.0
status: AgentTimeoutError
```

Profiling：

| 指标 | 值 |
|---|---:|
| Agent steps | 27 |
| Terminal tool calls | 71 |
| Prompt tokens | 603,615 |
| Cached tokens | 560,768 |
| Completion tokens | 32,687 |
| Hidden verifier pass | no final submission |

Agent 在超时前已经完成依赖升级、API 调研和部分迁移，最后停在
`LegacyRouterLLM.bind_tools` 与 `BaseRetriever` 初始化修复阶段。

原始 Harbor job：

```text
C:\Users\yangjiashu\Temp\LHTB-stepfun-one-jobs3
```

本地 profile：

```text
artifacts/lhtb-stepfun37-langchain-migration-20260820/PROFILE.zh-CN.md
```

## 限制

1. 这次使用的是 stock Harbor 0.20.0，不是 LHTB bundled patched Harbor；
2. Windows Docker 无法执行任务声明的 `no-network`，本地副本临时允许网络；
3. Agent 预算从任务原始 5400 秒缩短到 900 秒；
4. 因此不能把 reward 0 当作模型或 LongHorizonOS 的最终能力结论；
5. 当前尚未把 LHTB trial 接入 LHOS；LHOS 的 causal OS 结果仍由
   `real_dsh_dynamic_coding` dynamic episode 提供。

## 下一步

将同一个 LHTB task 接入一个 Harbor-as-executor 的 LHOS adapter，先做
`full restart` 与 `checkpoint/resume` 对比，再在中途修改 Artifact 后测
`selective repair`。只有这个扩展才测到 LongHorizonOS，而不是单纯测 Agent
能否完成 LHTB 任务。
