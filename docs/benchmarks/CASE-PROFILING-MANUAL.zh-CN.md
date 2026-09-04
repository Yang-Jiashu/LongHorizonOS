# LongHorizonOS Case Profiling 工程说明书

## 目的

Benchmark 的最终报告不能只给一个：

```text
LHOS 比 baseline 快 1.5x
```

测评者需要看到：

1. 哪些 Task 被执行；
2. 哪些 Task 被保留、没有执行；
3. 每个 Task 的 model calls、tool calls、token buckets 和时间；
4. 关键路径在哪里；
5. 节省是 OS 直接造成的，还是模型随机轨迹/Provider retry 造成的。

因此每个 case 都应输出一个独立目录：

```text
artifacts/<case-id>/
├── result.json
├── case-profile.json
└── CASE-PROFILE.zh-CN.md
```

当前 dynamic case 的生成命令：

```powershell
python scripts/profile_real_dsh_dynamic_case.py `
  artifacts/real-dsh-dynamic-coding-20260819-pilot-eb0a8aaf/result-regraded.json
```

这条命令只读取已有 DSH session log 和 attempt artifact，不重新调用模型。

## Profile 的六个区域

### 1. Correctness

```text
public tests
hidden tests
protected input hashes
false VERIFIED
under-invalidation
over-invalidation
```

任何 correctness gate 失败时，性能数字只能作为 failure diagnostic，不能作为
加速结论。

### 2. Task DAG / Timeline

每个 Task 至少记录：

```text
task_id
dependency_ids
graph_version
attempt_id
claim_id
lease_id
session_id
status
start/end
```

图上必须显式标记：

```text
VERIFIED
STALE
PRESERVED
REPAIR FRONTIER
```

### 3. Token profile

不能只记录 `total_tokens`。至少拆成：

```text
uncached_input_tokens
cache_read_tokens
cache_write_tokens
output_tokens
total_token_units
```

因为一个 Task 被跳过时，减少的不只是 output；后续模型请求还会重复支付
增长后的 conversation prefix，通常体现在 `cache_read_tokens`。

### 4. Tool profile

按工具统计：

```text
read
glob
grep
str_replace_editor
edit
pwsh / bash
todo_write
```

同时记录：

```text
tool_duration_ms
tool_error_count
重复读取的文件
重复测试命令
```

### 5. Time profile

一个 Task 的时间拆成：

```text
session_span
retry_delay
tool_duration
model/context/network residual
process startup / outside-session
verifier
```

关键路径应单独计算，不能把并行 branch 的总耗时误当作 makespan。

### 6. Causal savings bridge

最终必须输出：

```text
observed baseline
 - directly skipped computation (OS causal)
 - affected-task trajectory/provider variance
 = observed LHOS
```

只有第一项能直接宣传为 OS 节省。第二项必须标成：

```text
model variance / workspace pre-state / provider retry
```

## 当前真实 DSH case 的结论

在 `real_dsh_dynamic_coding` 中：

```text
Static repair: 4 Tasks, 413,849 token units, 33 model calls, 46 tools
LHOS repair:   3 Tasks, 297,843 token units, 23 model calls, 37 tools
```

直接可归因的部分是被保留的 `audit`：

```text
26,766 token units
3 model calls
4 tool calls
13.875 Agent-slot seconds
1 targeted verifier
```

注意：`audit` 和 `pricing_core` 并行且 `audit` 先结束，因此本 case 中跳过
`audit` 没有直接缩短 critical path。它证明的是：

```text
semantic reuse
+ token/call/tool avoidance
+ slot occupancy reduction
+ stale-result authority
```

而不是一个完全可归因的 wall-clock speedup。

## README 应该放什么

README 只放：

1. 一个四任务示例；
2. 一张五行结果表；
3. 一句“不要让不必要的模型调用发生”；
4. 本说明书和 per-case profile 链接。

详细的 retry、tool、cache、critical path 和 causal bridge 只放在 case profile，
避免 README 变成工程日志。

## 下一轮实验要求

要把 wall-clock 也变成强 OS 证据：

1. 初始 v1 只运行一次；
2. 冻结同一个 workspace / ArtifactVersion / Evidence snapshot；
3. 所有 repair arm 从同一 snapshot clone；
4. 加入 `DSH-restart`、`DSH-resume`、`DSH+LHOS`、`Oracle-selective`；
5. 让被释放的 slot 立即服务另一个 READY critical Task；
6. 至少 5–10 组 AB/BA paired repetitions；
7. retry-inclusive 和 retry-exclusive 时间都报告。
