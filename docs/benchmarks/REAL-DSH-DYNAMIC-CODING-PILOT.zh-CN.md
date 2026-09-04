# DeepSeek Harness + LongHorizonOS 真实动态编码 Pilot（历史 SenseNova 版本）

> 最新统一 StepFun 3.7 medium 结果见
> [REAL-DSH-DYNAMIC-STEPFUN37-RESULT.zh-CN.md](REAL-DSH-DYNAMIC-STEPFUN37-RESULT.zh-CN.md)。

## 结论

本实验首次让真实 DeepSeek Harness 执行代码阅读、文件修改和 pytest，
并在相同 Task DAG、Task prompt、模型、工具、并发度、重试上限和 verifier
下比较：

- `dsh_static_restart`：需求变化后重新执行全部 Task。
- `dsh_lhos`：需求变化后由 VPG 推导 STALE cone，只执行 repair frontier
  及其语义后继。

一组成对 pilot 中，两侧最终都通过：

- public tests：6/6；
- hidden tests：4/4；
- requirement/test 文件哈希保护；
- LongHorizonOS 初始和最终 Goal closure。

在 mutation 后的 repair 阶段，LongHorizonOS：

- provider token units 减少 **28.0%**；
- 模型调用减少 **30.3%**；
- repair wall-clock 加速 **1.59x**；
- 保留 `audit` 的有效 Evidence，没有重新调用 Harness；
- `under_invalidation = 0`；
- `over_invalidation = 0`。

这是有效的 `n=1` plumbing/pilot 结果，不是统计显著的论文结论。

逐 Task 的时间、token bucket、tool call、retry 和 causal savings bridge 见
[CASE-PROFILE.zh-CN.md](../../artifacts/real-dsh-dynamic-coding-20260819-pilot-eb0a8aaf/CASE-PROFILE.zh-CN.md)；
profile 字段定义见
[CASE-PROFILING-MANUAL.zh-CN.md](CASE-PROFILING-MANUAL.zh-CN.md)。

## 运行配置

| 配置 | 值 |
|---|---|
| DeepSeek Harness | `0.1.0-rc.8` |
| Node.js | `22.19.0` |
| Provider | SenseNova OpenAI-compatible endpoint |
| Model | `deepseek-v4-flash` |
| DSH adapter | `llm-pi-ai`, `openai-completions` |
| Reasoning | `low` |
| Agent slots | 2 |
| Task max attempts | 2 |
| Web tool | disabled |
| Session-title LLM | disabled |
| Final authority | public + external hidden pytest |

SenseNova 与 DSH 原生 `deepseek-official` adapter 的流式 tool-call delta
存在兼容差异，因此实验使用 DSH 官方提供的通用 `llm-pi-ai` seam。

## Workload

模型操作一个真实 Python package：

```text
pricing_core -> pricing_api ---+
                               +-> integration
audit -------------------------+
```

初始版本要求实现折扣计算、API wire shape、审计事件和集成服务。

随后 pricing contract 从 v1 演化到 v2：

```text
calculate_total(subtotal, discount_percent=0) -> Decimal
```

变为：

```text
calculate_total(
    subtotal,
    *,
    discount_percent=0,
    tax_rate,
    currency,
) -> PriceBreakdown
```

Oracle affected set：

```text
pricing_core
pricing_api
integration
```

Oracle preserved set：

```text
audit
```

## Correctness

LongHorizonOS 实际推导结果：

```text
affected = {pricing_core, pricing_api, integration}
preserved = {audit}
repair frontier = {pricing_core}
under-invalidation = {}
over-invalidation = {}
```

最终两侧均满足：

```text
public pytest PASS
AND hidden pytest PASS
AND protected inputs unchanged
AND final requirement version = 2
```

模型输出不能自行决定 Task 成功。DSH 退出 0 后，LongHorizonOS 仍通过独立
pytest verifier、ArtifactVersion 和 Evidence guardian 决定是否升级为
VERIFIED。

## 结果

### Repair 阶段

| 指标 | DSH static restart | DSH + LongHorizonOS | 差异 |
|---|---:|---:|---:|
| 执行 Task 数 | 4 | 3 | -25.0% |
| Model calls | 33 | 23 | -30.3% |
| Provider token units | 413,849 | 297,843 | -28.0% |
| Wall-clock | 224.359 s | 141.219 s | 1.59x |

减少的 provider token units：

```text
116,006
```

减少的 repair wall-clock：

```text
37.1%
```

### 完整初始 + repair

| 指标 | DSH static restart | DSH + LongHorizonOS | 差异 |
|---|---:|---:|---:|
| Provider token units | 809,644 | 599,676 | -25.9% |
| Wall-clock | 430.422 s | 284.844 s | 1.51x |

完整阶段减少：

```text
209,968 provider token units
```

初始阶段也出现模型调用差异，但两侧是不同的随机模型运行，不能将该差异归因
于 OS。论文 headline 应优先使用 mutation 后结构已知的 repair 指标，并通过
paired repetitions 控制模型方差。

Token units 定义为：

```text
uncached input
+ cache-read
+ cache-write
+ output
```

各 bucket 在 artifact 中分别保留。该指标不是美元成本；不同 token bucket
可能具有不同价格。

## OS 如何体现

该结果不是简单的“少发一个 prompt”。运行路径是：

```text
VPG READY
-> Scheduler admission
-> Attempt
-> Claim
-> Kernel Lease
-> real DeepSeek Harness process
-> pytest verifier
-> ArtifactVersion / Evidence
-> VERIFIED
```

Mutation 发生后：

```text
GraphVersion advances
-> pricing_core becomes STALE
-> invalidation propagates to pricing_api and integration
-> audit Evidence remains applicable
-> repair frontier contains only pricing_core
-> Scheduler dispatches only the affected cone
-> final hidden verifier closes the Goal
```

因此：

> DeepSeek Harness 负责运行 Agent；LongHorizonOS 决定哪些计算仍然有效、
> 哪些 Task 可以提交，以及变化后哪些计算值得重新执行。

## Artifact

有效结果：

```text
artifacts/real-dsh-dynamic-coding-20260819-pilot-eb0a8aaf/result-regraded.json
```

原始 DSH session event logs、每个 Attempt 的 provider usage、工具调用和
workspace 都保留在该 artifact 目录及 Attempt 记录所指向的隔离
`DSH_HOME` 中。

`result-regraded.json` 没有重新运行模型。它只重新执行了冻结 workspace 上的
外部 grader，原因是最初 hidden test 对 `subtotal` 是否规范化为两位小数做了
公开 contract 未定义的断言。该断言被删除后，两种实现都按公开规范接受。

## 不能由本实验推出

本实验还不能证明：

- 相对强 `DSH-resume/test-and-fix` baseline 的优势；
- DSH session 级 semantic preemption；
- 通用生产 workload 的稳定加速；
- 跨模型、跨 Harness 的 generality；
- “10 小时缩短到 3 小时”；
- 统计显著性。

当前 mutation 在初始 Goal 闭合后注入，因此主要证明：

```text
semantic memory
+ exact invalidation
+ verified-progress preservation
+ selective repair
+ fenced verification/commit
```

没有证明正在运行的 DSH session 能被原生 interrupt/rebase。

## 下一阶段

论文实验至少应补：

1. `DSH-resume`：保留 workspace/session，运行全量 tests，只修失败项。
2. `Oracle-selective`：使用预注册 affected set，作为理论上界。
3. 每个 arm 至少 10 个 paired repetitions，AB/BA 交替。
4. 报告 median、IQR、bootstrap 95% CI 和 paired test。
5. 加入 no-mutation 与 full-cone mutation，测控制面开销和负结果。
6. 通过 DSH Cordis/ACP control seam 增加 session cancel，测试真实
   semantic preemption 和 doomed tokens。
