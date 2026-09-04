# SWE-bench Docker 验证记录

## Docker 主机状态

验证时间：2026-08-20

```text
Docker Desktop: 4.85.0
Docker Engine: 29.6.2
Context: desktop-linux
OS/Arch: linux/amd64
Kernel: WSL2
CPU: 12
Memory: 14.96 GiB
```

Windows 侧已经验证可以执行完整容器生命周期：

```text
docker run
docker inspect
docker exec
docker logs
docker cp
docker rm
```

因此 LongHorizonOS 的控制面可以运行在 Windows 主机，代码 workspace 和
evaluator 可以运行在 Linux 容器中。

## 公开任务

```text
instance: pytest-dev__pytest-11143
repo: pytest-dev/pytest
base: 6995257cf470d2143ad1683824962de4071c0eb7
```

两份 patch 来自同一个真实 DeepSeek Harness 运行：

```text
static.patch
lhos.patch
```

两份 patch 的源码修改相同，都是：

```diff
and isinstance(item.value.value, str)
```

公开 test patch 在 Agent 完成后才注入，Agent 没有看到 gold patch。

## Linux 容器复核

由于官方 SWE-bench 镜像在当前 registry mirror 上返回 403，且 GHCR 拉取
超时，本次使用本地可用的 `alpine:3.20` 作为 Linux 容器，并切换 Alpine
APK 源到可达镜像。这个结果是**容器化复核**，不是官方镜像哈希意义上的
canonical SWE-bench 分数。

Static patch：

```text
115 passed, 1 skipped in 14.63s
```

LongHorizonOS patch：

```text
115 passed, 1 skipped in 15.60s
```

这说明：

1. 两份真实 DSH patch 在 Linux 容器中均可通过完整
   `testing/test_assertrewrite.py`。
2. Windows host-native 结果中出现的 `sys.pycache_prefix` 路径失败是平台
   差异，而不是 patch 本身的功能错误。
3. LongHorizonOS 产生的 patch 没有破坏 Linux 目标仓库测试。

日志：

```text
artifacts/swebench-official-pytest11143-20260820/docker-static.log
artifacts/swebench-official-pytest11143-20260820/docker-lhos.log
```

单任务 profiling：

```text
artifacts/swebench-host-native-pytest11143-20260820/CASE-PROFILE.zh-CN.md
```

这组单任务没有 preserved branch，因此 token/time 差异只能作为执行轨迹
观察，不能当作 LongHorizonOS 的 selective-scheduling 收益。

## 运行架构

当前验证的结构是：

```text
Windows host
├── DeepSeek Harness
├── LongHorizonOS Scheduler / VPG / Claim / Lease
├── token/time/event collector
└── Docker CLI
    └── Linux evaluator container
        ├── repository workspace
        ├── Python dependencies
        └── pytest test runner
```

这比把所有组件都放进容器更接近 LongHorizonOS 的 OS 定位：

- LongHorizonOS 是外部 authoritative control plane；
- 容器是受控 execution/evaluation sandbox；
- evaluator 的测试结果回到 LongHorizonOS；
- 只有通过独立 verifier 和 ownership fence 才能成为 VERIFIED。

## 官方 evaluator 与本地复核的区别

官方 SWE-bench evaluator 还会固定：

- evaluator image；
- repository/environment setup；
- test command；
- patch application；
- container cleanup。

本记录中的 Alpine 容器只验证 Linux 隔离环境下的真实测试结果，不能直接
替代官方 leaderboard 分数。若需要 canonical score，应继续使用官方
SWE-bench Docker image，或使用官方云端 evaluator。

## 当前阻塞

官方任务镜像拉取遇到：

```text
Docker Hub mirror: 403 Forbidden
GHCR: pull timeout
```

Docker daemon 本身已经健康，阻塞在 registry/image distribution，不是
Windows Docker 权限或容器执行问题。

## 下一步

1. 固定官方 SWE-bench image digest 后重跑同一 instance。
2. 对 Flask-4992、Django-17087、Sphinx-11445 分别生成 static/LHOS patch。
3. 每个仓库至少运行 5 组 paired repetitions。
4. 将同仓库多个 issue 组成 dynamic episode，测量：
   - preserved Evidence；
   - invalidation cone；
   - repair frontier；
   - stale computation；
   - provider token units；
   - Time-to-Verified-Goal。
