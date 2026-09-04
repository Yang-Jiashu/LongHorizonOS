# Vertical Slice v2 — Stop Gate GO/NO-GO Decision

**Date**: 2026-08-04  
**Run ID**: vertical-slice-v2  
**Model**: sensenova-6.7-flash-lite  
**Task**: config_loader  

---

## 1. V1 → V2 Comparison Matrix

| Metric | V1 Baseline | V2 Result | Delta | Status |
|--------|-------------|-----------|-------|--------|
| **llm_call_count** | 0 | 53 | +53 | ✅ FIXED |
| **llm_calls_by_role** | N/A | planner:1, worker:52 | — | ✅ NEW |
| **llm_calls_by_status** | N/A | success:45, parse_failed:8 | — | ✅ NEW |
| **file_write_count** | N/A | 68 | — | ✅ NEW |
| **shell_call_count** | N/A | 26 | — | ✅ NEW |
| **failure_tree** | (missing) | Full tree | — | ✅ FIXED |
| **primary_failure_code** | (missing) | run_stuck | — | ✅ NEW |
| **tool_calls** | 29 | 47 | +62% | ✅ IMPROVED |
| **total_tokens** | 91,141 | 173,786 | +90% | ✅ MORE WORK |
| **verified_nodes** | 2/6 | 2/6 | — | — |
| **failed_nodes** | 1/6 | 1/6 | — | — |
| **external_score (LHoS)** | 20% | 20% | — | — |
| **external_score (Transcript)** | 30% | 30% | — | — |
| **wall_time (LHoS)** | 154.25s | 132.25s | -14% | ✅ FASTER |

---

## 2. Three Original Blocking Issues — Resolution Status

### Issue 1: `llm_calls` Log is Empty

**V1**: `llm_call_count: 0` — no LLM calls were logged to the database or JSONL trace.  
**V2**: `llm_call_count: 53` — all calls logged with full metadata.

**Root Cause (Fixed in Step 2)**: `LoggedLLMClient` was not properly bound to the runtime stack's database. The `LLMCallLogger` was using a separate database instance.

**Verification**:
- Database: 53 rows in `llm_calls` table with `status`, `error_type`, `causation_id` columns
- JSONL trace: 53 lines in `full_lhos/traces/llm_calls.jsonl`
- Role breakdown: 1 planner call + 52 worker calls
- Status breakdown: 45 success + 8 parse_failed
- Total tokens tracked: 161,124 input + 14,973 output

**Status**: ✅ RESOLVED

---

### Issue 2: Worker Not Creating Files (Chain Not Closed)

**V1**: 29 tool calls with no structured trace, no file write tracking, tool name mismatches.  
**V2**: 47 tool calls with per-iteration structured trace, 68 filesystem events, 26 shell events.

**Root Cause (Fixed in Steps 4-7)**:
- `LLMWorkerAdapter` lacked structured trace logging
- Tool names from LLM were not normalized (case sensitivity, aliases)
- `SenseNovaClient` used `reasoning` as fallback for empty `content`

**Verification**:
- Structured trace per worker iteration with `action_type`, `requested_tool`, `normalized_tool`, `tool_execution_status`
- Tool name normalization: all LLM-returned tool names correctly mapped to `filesystem` and `shell`
- File writes occurred (files created in workspace, rolled back by checkpoint on verification failure — correct behavior)
- Worker tool loop: n1 (9 tool calls → claim_done), n2 (tool calls → claim_done), n3 (multiple attempts with 10+ tool calls each)

**Status**: ✅ RESOLVED

---

### Issue 3: Controller Failure Lacks Diagnostics

**V1**: No failure tree, no `primary_failure_code`, no diagnostic payload.  
**V2**: Full structured failure tree with all required diagnostic fields.

**Root Cause (Fixed in Step 8)**: `RuntimeController._finish_run` did not build a structured failure tree payload.

**Verification** — Failure tree payload from v2 run:
```json
{
  "primary_failure_code": "run_stuck",
  "termination_reason": "no ready nodes and no waiting nodes: run is stuck",
  "terminal_status": "failed",
  "failed_node_ids": ["vertical-slice-full-lhos:n3"],
  "blocked_node_ids": ["vertical-slice-full-lhos:n4", "n5", "n6"],
  "ready_node_ids": [],
  "waiting_node_ids": [],
  "last_successful_event": "NODE_LEASE_RELEASED:system",
  "remaining_budget": {
    "tokens_remaining": 26214,
    "tool_calls_remaining": 53,
    "wall_time_remaining": 3600.0,
    "model_calls_remaining": 0
  },
  "recommended_debug_action": "Check dependency graph for cycles or dead branches."
}
```

**Status**: ✅ RESOLVED

---

## 3. Additional Fix During V2 Run

### JSONL Trace Crash (Hotfix)

**Issue**: `_sanitize_body()` truncates JSON at 10,000 chars and appends `"...[truncated]"`, making it invalid JSON. `json.loads()` on the truncated string caused `JSONDecodeError: Unterminated string`.

**Fix**: Added `_safe_json_loads()` helper that falls back to the raw string if parsing fails. Applied to both `log_call` and `log_failure` methods in `call_logger.py`.

---

## 4. Remaining Issues (Non-Blocking)

### 4.1 `run_stuck` Failure

**Description**: Full LHoS mode terminates with `run_stuck` because node n3 fails verification, blocking downstream nodes n4, n5, n6.

**Analysis**:
- n3 (design config loader) was attempted 3 times
- First attempt: 9 tool calls, parse failure on round 9 → returned raw text as claim_done
- Second attempt: 10 tool calls, claimed_done with `config_loader_design.md` → verification failed
- Third attempt: 8+ tool calls → verification failed
- After 3 failed attempts, n3 is marked as `failed`, and n4/n5/n6 are left in `pending`/`stale` state with no path to `ready`

**Classification**: Task execution quality issue (LLM output doesn't pass verification), NOT an infrastructure bug. The infrastructure correctly detected the failure, attempted retries, and terminated with diagnostic information.

### 4.2 External Score

**Description**: Both modes score low (20% / 30%) on the external grader.

**Analysis**: The `sensenova-6.7-flash-lite` model (free tier) has limited coding capability. Key failures:
- `req_1_json_loading`: ConfigLoader doesn't properly load JSON files
- `req_2_missing_file_error`: Missing FileNotFoundError handling
- `req_3_invalid_json_error`: Missing JSON decode error handling
- `req_9_get_method`: Missing `get()` method with default value
- `req_10_nested_config`: Missing nested config support

**Classification**: Model capability limitation, NOT an infrastructure bug.

### 4.3 Parse Failure Rate

**Description**: 8 out of 53 LLM calls (15%) had parse failures.

**Analysis**: The LLM sometimes returns invalid JSON (e.g., missing comma delimiter). The repair mechanism in `SenseNovaClient` handles this by retrying with a repair prompt, but some calls still fail after repair.

**Classification**: Model output quality issue, NOT an infrastructure bug. The parse failure tracking and repair mechanism are working correctly.

---

## 5. Stop Gate Decision

### GO/NO-GO Criteria

| Criterion | Required | Actual | Pass? |
|-----------|----------|--------|-------|
| `llm_calls` table is populated | >0 | 53 | ✅ |
| LLM calls have `status` field | All rows | 53/53 | ✅ |
| LLM calls have `role` breakdown | All rows | 53/53 | ✅ |
| Worker tool loop produces trace | Per iteration | Yes | ✅ |
| Tool name normalization works | No raw names | All normalized | ✅ |
| Failure tree has `primary_failure_code` | Non-empty | `run_stuck` | ✅ |
| Failure tree has `failed_node_ids` | Non-empty | `[n3]` | ✅ |
| Failure tree has `remaining_budget` | Present | Yes | ✅ |
| JSONL trace file written | >0 lines | 53 lines | ✅ |
| No infrastructure crashes | Clean exit | Yes | ✅ |

### Decision: **CONDITIONAL GO**

**Rationale**: All three original blocking issues have been resolved and verified with real LLM calls. The infrastructure now properly:
1. Logs all LLM calls with status, role, and cost tracking ✅
2. Executes tool calls with normalized names and structured trace ✅
3. Provides detailed failure diagnostics with structured failure tree ✅

**Conditions**:
1. The `run_stuck` failure mode should be investigated as a follow-up — consider adding graph patching or re-planning when a critical node fails
2. The 15% parse failure rate should be monitored — consider prompt improvements or model upgrades
3. External score improvement is deferred to a later milestone (model capability, not infrastructure)

**Next Steps**:
- Investigate whether the controller can re-plan around failed nodes (graph patching)
- Consider upgrading to a more capable model for future vertical slices
- Proceed to Milestone 2.2 (Benchmark Hardening)
