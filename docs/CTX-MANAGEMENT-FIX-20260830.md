# Context 管理修复记录（2026-08-30）

## 问题诊断

LHTB 配对对比分析（n=13）发现 lhos_resume arm 每轮平均 token 比 dsh_fresh 高 +50.9%（中位 80,362 vs 53,264 tok/call）。

### 根因 1：context_cache_bloat 触发逻辑缺失（核心 bug）

optimization.py 的 SemanticContextPolicy.decide() 中，注释写了 "the ordinary context_bloat path owns the decision"，但代码里根本没有实现 context_bloat 的 guard_trigger。cache_tokens_per_call_threshold 只用于计算 context_score（展示用），不直接触发 compaction。实际任务每轮 cache token 高达 50k-120k（远超 24k 阈值），但因为没撞 max_tokens，guard 永远不触发。

### 根因 2：max_restarts=2 太保守

每个任务 controlled_restart_count 都是 2（撞上限）。16 轮的任务只有前 2 轮能 compaction，后面 14 轮全量 resume 滚雪球。

## 修复内容

1. optimization.py：新增 context_cache_bloat 触发；max_restarts 2->6；threshold 24000->16000；min_phases 3->2；cumulative 128000->96000
2. run_lhtb_software5_pair.py：同步 DEFAULT_SEMANTIC_CONTEXT_CONFIG
3. lhtb_dsh_harbor_agent.py：同步参数默认值

## 验证

单元测试通过：cache/call=50000 超阈值 -> RESTART_COMPACTED (context_cache_bloat)；8000 低于阈值 -> RESUME。三文件 py_compile 通过。

## 预期效果

更多轮次触发 compaction，每轮 context 从 50k-120k 降到 handoff 摘要级别，lhos_resume 的 tok/call 应大幅下降，tool_calls 节省转化为 token 节省。
