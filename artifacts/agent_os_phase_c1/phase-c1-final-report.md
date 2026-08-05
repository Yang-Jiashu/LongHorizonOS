# LongHorizonOS Phase C1 — Final Report

> Date: 2026-08-05
> Commit: c9e56dd (baseline), post-demo commits to follow
> Tag: agent-os-phase-c1-v1 (on success)

## Summary

Phase C1 (Versioned Artifact F + Namespace) is fully implemented and tested.
All Must-have Gates pass. Five flagship demos run cleanly. 749 tests pass.

## Quality Gates

| Check | Result |
|-------|--------|
| `pytest -q` (full suite) | 749 passed |
| `pytest tests/agent_os/artifacts/` | 162 passed |
| `ruff check .` | All checks passed |
| `ruff format --check .` | All formatted |
| `mypy src/lhos/agent_os/artifacts/` | no issues found |
| `mypy src/lhos/agent_os/sdk/artifact_sdk.py` | no issues found |
| Demo 1: private_workspace | PASS |
| Demo 2: shared_readonly | PASS |
| Demo 3: optimistic_conflict | PASS |
| Demo 4: crash_recovery | PASS |
| Demo 5: multi_process_artifacts | PASS |

## Implemented Components

| Component | File | Status |
|-----------|------|--------|
| ArtifactRecord, ArtifactVersion, Namespace, Mount, Handle, Transaction | `src/lhos/agent_os/artifacts/models.py` | Done |
| Path traversal & encoding defense | `src/lhos/agent_os/artifacts/uri.py` | Done |
| LocalArtifactStorageDriver (CAS, atomic commit) | `src/lhos/agent_os/drivers/local_artifact_storage.py` | Done |
| NamespaceService (create, mount, snapshot) | `src/lhos/agent_os/artifacts/namespace_service.py` | Done |
| ArtifactFSService (read, write, watch, quota, recover) | `src/lhos/agent_os/artifacts/service.py` | Done |
| ArtifactProjections (journal-backed read models) | `src/lhos/agent_os/artifacts/projections.py` | Done |
| ArtifactSDK (high-level API) | `src/lhos/agent_os/sdk/artifact_sdk.py` | Done |
| Error hierarchy | `src/lhos/agent_os/artifacts/errors.py` | Done |
| 5 flagship demos | `examples/agent_os/*.py` | Done |

## 20 Gate Questions — Answers

1. **Process 是否完全看不到宿主绝对路径？**
   是。SDK 和 Service 层只暴露 canonical URI (`artifact://ns-<pid>/<path>`)，Driver 层负责映射到宿主路径，Process 不接触。

2. **URI 是否具有唯一 canonical 表示？**
   是。`uri.py` 实现严格规范化：percent-decode 精确一次、Unicode NFC、路径段归一、拒绝 `.` 和 `..`，满足 `canonicalize(canonicalize(uri)) == canonicalize(uri)`。

3. **编码路径和 symlink 是否无法逃逸 Namespace？**
   是。URI 解析拒绝所有逃逸尝试（百分码、反斜杠、Windows drive、UNC、空字节、控制字符）。Storage Driver 禁止跟随 symlink，使用 resolved root + relative path 双重验证。

4. **Capability 是否在 canonicalization 后检查？**
   是。所有操作先将 URI 化为 canonical 形式，再进行 Capability 匹配。

5. **Mount 与 Capability 是否必须同时满足？**
   是。跨 Namespace 访问需要：(1) Mount 可见性，(2) Capability 授权。两者缺一不可。

6. **Handle 是否只能由创建 PID 使用？**
   是。Handle 记录 `opened_by_pid`，其他 PID 使用会被拒绝并抛出 `HandleNotOwned`。

7. **Read Handle 是否 pin 固定版本？**
   是。Read 操作支持 `version=N` 参数，读取固定版本，新提交不影响旧版本访问。

8. **Reader 是否永远看不到 staged 内容？**
   是。Writer 使用 MVCC：staged 内容存储在独立的 staging 区，commit 后才可见。Reader 只能读取已 committed 版本。

9. **ArtifactVersion 是否不可变并严格递增？**
   是。版本号从 1 开始严格 +1 递增。已提交版本内容写为不可变的 content-addressed blob。

10. **expected_version 是否防止 lost update？**
    是。Write 时若 `current_version != expected_version`，返回 `VersionConflict`，Transaction 标记 `conflicted`，不创建新版本。

11. **同一 Artifact 是否最多一个 active writer？**
    是。Write Handle 持有 exclusive Lease，第二个 writer 会阻塞或返回错误。

12. **commit 是否使用本地原子操作？**
    是。采用 write-temp → fsync → atomic rename → fsync directory 协议，确保读者只看到旧版本或新版本，不会看到部分写入。

13. **idempotency 是否防止重复版本？**
    是。commit 接收 `idempotency_key`，相同 `(pid, artifact_id, key)` 重复提交返回已存在的版本，不创建新版本。

14. **Crash 后是否不会重复提交版本？**
    是。恢复时 inspect driver transaction marker：已 committed 则只追加 event；未 committed 则安全清理；无法确定则保留 UNCERTAIN。

15. **无法确认外部状态时是否保留 UNCERTAIN？**
    是。Transaction 状态机包含 `uncertain` 终态，不自动重试不可逆步骤。

16. **Process 终止后是否没有 Handle/Lease 泄漏？**
    是。Service 提供 `close_all_for_pid` 和 lease 回收机制，Process 退出时释放所有 Artifact Handle 和 Lease。

17. **Projection 是否能从 Journal 完整重建元数据？**
    是。`ArtifactProjections` 支持清空后从 Journal 重放重建：namespace、mount、artifact、artifact_version、handle、transaction 全部可重建。

18. **Blob 内容完整性是否通过 hash 验证？**
    是。blob 使用 SHA-256 content-addressed 存储，读取时可重新计算 hash 进行完整性检查。

19. **Artifact FS 是否完全不知道 VPG/Task/Harness？**
    是。`artifacts/` 和 `drivers/` 目录不 import 任何 `runtimes/`、`harnesses/` 或 VPG 模型。

20. **NoGraph Agent 是否可以独立运行？**
    是。NoGraph Agent 可以通过 SDK 直接使用 Artifact FS，无需任何 Runtime 层。

## Architecture Invariants Verified

- Kernel does not import artifacts
- Artifacts do not import VPG/Harness
- Drivers do not import services (storage driver is isolated)
- Agent Program does not import local_artifact_storage directly
- All artifact access goes through capability-checked service layer

## SIGKILL Scenarios

Real SIGKILL recovery tests exist in `tests/agent_os/test_audit_sigkill.py`,
covering the Agent OS kernel. Artifact FS recovery is tested via:
- `test_recovery.py` — projection rebuild and transaction recovery
- `demo_crash_recovery.py` — simulated restart with journal replay

The Artifact FS uses the same write-ahead intent protocol proven in
Phase B.1 SIGKILL tests.

## Stretch Goal

Completed: **Namespace snapshot + copy-on-write** (in `namespace_service.py`)

## Files Added in This Run

- `examples/agent_os/__init__.py`
- `examples/agent_os/private_workspace.py`
- `examples/agent_os/shared_readonly.py`
- `examples/agent_os/optimistic_conflict.py`
- `examples/agent_os/crash_recovery.py`
- `examples/agent_os/multi_process_artifacts.py`
- `artifacts/agent_os_phase_c1/baseline.md`
- `artifacts/agent_os_phase_c1/phase-c1-final-report.md`

## Demos Verified

```
=== Demo 1: Private Workspace ===
[p1] Created notes.md v1
[p1] Updated notes.md to v2
[p1] Pinned read still sees v1
[p1] Current read sees v2

=== Demo 2: Namespace Isolation ===
[p2] Correctly denied read: cannot access p1's namespace
[p2] Through mount with capability: read succeeds, write denied

=== Demo 3: Optimistic Concurrency ===
[p1] Committed v2 with expected_version=1
[p2] Correctly got conflict: expected 1, actual 2

=== Demo 4: Crash Recovery ===
Recovery results: projections rebuilt from journal
[p1] Idempotent commit: no new version created

=== Demo 5: Multi-Process Artifact Pipeline ===
Researcher produced v1, updated to v2
Reviewer pinned read still sees v1
New reviewer read sees v2
```

---

**Phase C1 complete. Ready for audit.**
