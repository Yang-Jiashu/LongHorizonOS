# LHTB 自然长跑的外部时间点快照

这个工具用于同一套 DSH/Harbor harness、关闭 `time_slice` 的长预算配对实验。它在累计 DSH active time 到达
`3600, 5400, 10800, 14400, 18000, 21600, 28800` 秒时保存 prepared config 声明的 agent workdir（通常是 `/app`，
个别任务是 `/workspace`），随后可用同一 task、镜像和 Harbor verifier 事后打分。

它不改两臂的 prompt、agent、continuation、feedback 或配置。为取得一致文件系统视图，外部 watcher 会短暂
`docker pause`，在冻结状态执行 `docker commit --no-pause`，随即 `docker unpause`。冻结时间仍会消耗 Harbor 的墙钟预算，
所以这是一项 extended-budget diagnostic，不是官方 leaderboard checkpoint。每个快照都记录冻结时长和 cutoff 迟到上界；
两臂使用相同机制仍不能消除工作区大小不同造成的差异性扰动。

后续 cutoff 的累计 active time 会扣除 watcher 自己造成的先前冻结时间；Harbor 的最终 24h 墙钟上限无法扣除，因此每次冻结
仍会减少模型真正可用的尾部时间。watcher 会持续到主 arm 终止，以便发现 Harbor infrastructure retry；trial identity 一旦变化，
该 arm 的所有快照都会 fail closed，而不会拼接两次 trial。

## 运行

先在与主实验相同、只通过进程环境持有 provider key 的终端启动 watcher。必须在模型运行前启动，否则已经错过的
cutoff 不会被伪装成有效快照。

```powershell
$env:PYTHONPATH=(Resolve-Path 'src').Path
python scripts/lhtb_external_checkpoint.py watch `
  --output D:\LHTB-results\lhtb-extended-natural-86400-20260823-r1 `
  --jobs-dir D:\LHTB-jobs\lhtb-extended-natural-86400-20260823-r1
```

另一个终端运行原配对 runner。watcher 不启动模型，也不修改 runner。它从挂载到 host 的原子
`agent/invocations/invocation-*/invocation.json` 累加已完成调用的 `elapsed_ms`，并对当前 running invocation 使用其
`started_at` 计时；interim verifier 间隔不算 DSH active time。

快照写入 `OUTPUT/external-checkpoints/`。文件内容进入 SHA-256 content-addressed blob store，相同的基线数据和未变化文件
只保存一次；每个时间点以 workspace manifest 原子发布。若 `/app` 或其子路径是 mount、Compose container 不能唯一对应
trial、镜像 ID 改变、credential 出现在工作区、或者快照类型不能安全重建，捕获会 fail closed。

## 事后打分

单个快照：

```powershell
python scripts/lhtb_external_checkpoint.py score `
  --snapshot D:\LHTB-results\lhtb-extended-natural-86400-20260823-r1\external-checkpoints\alp-paper-reproduction\dsh_fresh\active-00005400
```

所有主实验 arm 都终止后，可批量打分；工具在主实验仍运行时拒绝执行，以免 verifier 与模型争抢 Docker 资源：

```powershell
python scripts/lhtb_external_checkpoint.py score-all `
  --checkpoint-root D:\LHTB-results\lhtb-extended-natural-86400-20260823-r1\external-checkpoints `
  --max-concurrency 2
```

打分前会重新校验原 config、task tree、Docker image ID、Harbor commit/source 和所有 workspace blob。事后 restore agent 只在
隔离的 replay trial 中清空 `/app`、恢复快照并退出；`HB_CONTINUE_MODE=same_conversation` 下它使用非 completion termination
reason，使 Harbor 不运行 interim verifier，而只运行一次原 final verifier。provider key 会从 replay 进程环境移除。

当前 replay 范围严格是该任务的 agent workdir。它不会保存安装到系统目录的软件、进程内存、打开的文件描述符或 workdir
之外的修改；如果任务的 verifier 依赖这些状态，该时间点应判为不可复验，而不是把 workspace 分数解释成完整容器分数。
模型创建的链接也按不可信输入处理：越出 workdir 的链接、链接目录下的归档成员、设备文件和 FIFO 都会被拒绝。

## 解释结果

- `captured`：有一致、可复验 workspace；检查 `capture_lateness_upper_seconds` 和 `freeze_wall_seconds`。
- `not_reached`：该 arm 在 active time 到达 cutoff 前已终止，不能从一次长跑制造不存在的后续状态。
- `late_unavailable`：active time 到过 cutoff，但 watcher 未取得安全快照；不能 carry-forward 或用最终 workspace 冒充。
- `failed`：身份、完整性、安全或 Docker 操作连续失败；不能作为该 cutoff 的分数。

汇总比较时必须同时报告 reward 差、捕获迟到和累计冻结成本；不能只展示 reward 曲线，也不能把这套外部 checkpoint 分数标成
官方 leaderboard score。
