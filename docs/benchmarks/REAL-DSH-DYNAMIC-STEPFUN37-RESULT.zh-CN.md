# StepFun 3.7 动态编码结果

## 结论

这是 LongHorizonOS 真正的 dynamic episode，而不是单任务 SWE-bench wrapper。
同一组四任务先完成 pricing contract v1，再把 contract 变为 v2：

```text
pricing_core -> pricing_api -> integration
audit -----------------------> integration
```

v2 只影响 `pricing_core`、`pricing_api`、`integration`。`audit` 的输入没有变化，
因此 LHOS 保留它的 Evidence，不创建 `audit-v2` computation。

## 配置

```text
provider: StepFun Step Plan
model: step-3.7-flash
reasoning_effort: medium
harness: DeepSeek Harness 0.1.0-rc.8
Node: 24.18.0
Agent slots: 2
max attempts: 2
public verifier: pytest
hidden verifier: external pytest
```

## Repair 结果

| 指标 | DSH static restart | DSH + LHOS | LHOS 变化 |
|---|---:|---:|---:|
| 执行 Task | 4 | 3 | -1 |
| Model calls | 28 | 18 | -35.7% |
| Tool calls | 31 | 22 | -29.0% |
| Provider token units | 334,053 | 185,631 | -44.4% |
| Repair wall-clock | 223.781 s | 109.859 s | 2.04x |

两侧最终都满足：

```text
public: 6/6
hidden: 4/4
under-invalidation: 0
over-invalidation: 0
LHOS Goal: closed
```

## 直接因果节省

LHOS 没有派发 `audit-v2`，因此确定避免：

```text
27,248 token units
3 model calls
4 tool calls
24.640 Agent-slot seconds
```

这部分可以直接归因于语义失效传播和 Evidence preservation。其余受影响任务的
token/time 差异来自两次独立模型轨迹，不能全部归因给调度器。

## 完整 episode

| 指标 | Static | LHOS |
|---|---:|---:|
| Initial + repair token units | 686,278 | 543,665 |
| Initial + repair model calls | 60 | 52 |
| Initial + repair tool calls | 69 | 60 |
| Total wall-clock | 344.938 s | 265.984 s |

完整流程观测约 `1.30x`，但论文主表应优先报告 repair 阶段，并把
`audit` 的避免成本作为 causal lower bound。

## 为什么这次体现了 OS

```text
Artifact/requirement version changes
        |
        v
VPG invalidation cone
        |
        +--> audit = PRESERVED
        |
        +--> pricing_core = STALE
                -> pricing_api = STALE
                -> integration = STALE
        |
        v
minimal repair frontier -> Scheduler -> Harness
```

普通 restart 的控制器不知道 `audit` 仍然有效，只能重跑四个任务。LHOS 根据
读集、写集、Artifact version 和 Evidence validity，只重新派发三个受影响任务，
最后仍由独立 verifier 决定是否 `VERIFIED/closed`。

## 限制

- 当前是 `n=1`；
- static 与 LHOS 的受影响任务是独立模型轨迹；
- 这是 recovered paired capture：static 先完成，LHOS 在 provider 451 误拦后
  使用中性 provider-facing 路径单独重跑；
- 当前不是官方 SWE-bench leaderboard 分数；
- 需要至少 5-10 组 AB/BA paired repetitions 才能做论文统计结论。

## Artifact

```text
artifacts/real-dsh-dynamic-stepfun37-medium-20260820/result.json
artifacts/real-dsh-dynamic-stepfun37-medium-20260820/BOSS-RESULT.zh-CN.md
artifacts/real-dsh-dynamic-stepfun37-medium-20260820/CASE-PROFILE.zh-CN.md
```
