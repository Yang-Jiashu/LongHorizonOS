# Case Profiling：真实 DSH 动态编码任务

## 一句话结论

LongHorizonOS 真正做的事情是：pricing contract 变化后，保留仍然有效的
`audit` Evidence，不再向 DeepSeek Harness 派发 `audit-v2`。因此下面这部分
资源消耗根本没有发生：

- **26,766 token units**
- **3 次模型调用**
- **4 次工具调用**
- **13.875s Agent-slot 占用**

这是当前 profiling 中可以直接归因于 OS 的部分。

## 场景

```text
pricing_core -> pricing_api -> integration
audit -----------------------> integration
```

pricing contract 从 v1 变为 v2。`pricing_core / pricing_api / integration`
失效，`audit` 没有读取 pricing contract，因此仍然有效。

```text
Static restart: pricing_core + audit -> pricing_api -> integration
LHOS repair:    pricing_core         -> pricing_api -> integration
                                   audit = PRESERVED
```

## Repair 总览

| 指标 | DSH static | DSH + LHOS | 观测差异 |
|---|---:|---:|---:|
| 执行 Task | 4 | 3 | -1 |
| Model calls | 33 | 23 | -10 (30.3%) |
| Tool calls | 46 | 37 | -9 (19.6%) |
| Token units | 413,849 | 297,843 | -116,006 (28.0%) |
| Wall-clock | 224.359s | 141.219s | 1.59x |

双方最终都通过 6 个公开测试和 4 个隐藏测试；under/over-invalidation 均为 0。

## 每个 Task 花了多少

| Task | Static token / call / tool / time | LHOS token / call / tool / time |
|---|---:|---:|
| `audit` | 26,766 / 3 / 4 / 13.875s | **PRESERVED，未执行** |
| `integration` | 124,947 / 9 / 17 / 57.531s | 89,173 / 7 / 15 / 39.063s |
| `pricing_api` | 147,610 / 12 / 15 / 98.140s | 48,594 / 5 / 7 / 17.140s |
| `pricing_core` | 114,526 / 9 / 10 / 63.032s | 160,076 / 11 / 15 / 79.546s |

## Token 到底省在哪里

| Bucket | Static | LHOS | 观测减少 |
|---|---:|---:|---:|
| Uncached input | 39,857 | 34,110 | 5,747 |
| Cache read | 361,216 | 253,696 | 107,520 |
| Output | 12,776 | 10,037 | 2,739 |
| **Total** | **413,849** | **297,843** | **116,006** |

观测 token 差额中，cache-read 占 **92.7%**。
原因是 Agent 每多进行一轮模型—工具循环，就会把更长的会话前缀再次送回模型。
跳过一个完整 Task 会同时减少模型请求数、重复上下文和后续工具调用。

## Tool call 为什么少

| Tool | Static | LHOS | 差异 |
|---|---:|---:|---:|
| `glob` | 1 | 1 | +0 |
| `pwsh` | 9 | 7 | -2 |
| `read` | 24 | 23 | -1 |
| `str_replace_editor` | 7 | 6 | -1 |
| `todo_write` | 5 | 0 | -5 |

`audit-v2` 本身包含 3 次 read 和 1 次 pytest/Pwsh。LHOS 不派发该 Task，
这 4 次工具调用是确定没有发生的；其余工具差异来自两次独立 Agent 轨迹。

## 时间具体省在哪里

| Repair critical path | Static | LHOS |
|---|---:|---:|
| core -> api -> integration 的 DSH task compute | 218.703s | 135.749s |
| Wall - critical path（控制、启动、验证等） | 5.656s | 5.470s |

| DSH task time component (sum) | Static | LHOS |
|---|---:|---:|
| Retry backoff | 58.759s | 11.199s |
| Tool execution | 8.805s | 7.195s |
| Model/context/network residual | 151.942s | 109.669s |
| Process outside session | 13.072s | 7.686s |

`audit` 与 `pricing_core` 并行，而且更早结束，因此跳过 audit 在本 case 中
**没有直接缩短关键路径**。它直接节省的是 13.875s slot-time 和相应 token/call/tool。

观测到的 aggregate task-compute 差额是 96.829s：

- 13.875s：保留 audit，可归因于 OS；
- 82.954s：受影响 Task 的模型轨迹、pre-state 和 Provider retry 方差，不能归因。

两臂的 429 retry 也不同：

- Static：25 次，等待 58.759s；
- LHOS：7 次，等待 11.199s。

因此当前 `1.59x` 是 **observed end-to-end result**，不是完整的 causal scheduler speedup。

## Savings Bridge

```text
413,849 static repair token units
 - 26,766  preserved audit（OS causal）
 - 89,240  affected-task / pre-state / provider variance
 = 297,843 observed LHOS repair token units
```

## 测评人应该怎么看优势

LongHorizonOS 的直接优势不是“同一个模型调用便宜了”，而是：

1. 变化发生后，系统知道 `audit` 的 Evidence 仍然适用；
2. Scheduler 不为它创建新的 repair computation；
3. 因而不会产生对应的模型请求、上下文回放、文件读取和测试执行；
4. 旧版本或过期 ownership 的结果也不能提交为 VERIFIED。

下一轮要把 wall-clock 因果收益测得更清楚，需要从同一个冻结 v1 snapshot
clone 所有 repair arms，并让释放的 slot 能立即服务其他 READY critical work。

## 限制

- n=1；
- 两个 arm 的 affected-task Agent 轨迹不同；
- v1 pre-state 没有从同一冻结 snapshot clone；
- Provider 429 retry 数量不同；
- 所以只有 preserved task 的实测成本是当前可靠的 causal lower bound。
