"""Summarize a local LHTB/Harbor trial without copying sensitive logs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def build_profile(trial_dir: Path) -> dict[str, Any]:
    trial_dir = trial_dir.resolve()
    trajectory_path = trial_dir / "agent" / "trajectory.json"
    trajectory = _load(trajectory_path)
    steps = trajectory.get("steps", [])
    agent_steps = [step for step in steps if step.get("source") == "agent"]
    tool_calls = [call for step in agent_steps for call in (step.get("tool_calls") or [])]
    last_message = str(steps[-1].get("message", "")) if steps else ""
    return {
        "schema_version": "lhos-lhtb-trial-profile.v1",
        "benchmark": "Long-Horizon Terminal-Bench",
        "trial": trial_dir.name,
        "trial_dir": str(trial_dir),
        "model": trajectory.get("agent", {}).get("model_name"),
        "agent": trajectory.get("agent", {}).get("name"),
        "steps": len(steps),
        "agent_steps": len(agent_steps),
        "tool_calls": len(tool_calls),
        "final_metrics": trajectory.get("final_metrics", {}),
        "last_step": steps[-1].get("step_id") if steps else None,
        "last_agent_message": last_message[:2000],
        "result": "timeout_or_incomplete",
        "official_score": False,
        "limitations": [
            "Stock Harbor 0.20.0 was used instead of the bundled patched LHTB Harbor.",
            "Local Docker task copy allowed network because Windows Docker cannot enforce no-network.",
            "Agent timeout was reduced to 900 seconds from the task's 5400-second budget.",
            "This is a baseline smoke, not a LongHorizonOS comparison.",
        ],
    }


def render(profile: dict[str, Any]) -> str:
    metrics = profile["final_metrics"]
    return f"""# LHTB StepFun Trial Profile

## Result

```text
task: {profile["trial"]}
model: {profile["model"]}
agent: {profile["agent"]}
result: timeout_or_incomplete
official_score: false
```

The Docker image and task healthcheck passed. The agent then ran for the local
900-second cap but did not reach a passing hidden-verifier result.

| Metric | Value |
|---|---:|
| Agent steps | {profile["agent_steps"]} |
| Terminal tool calls | {profile["tool_calls"]} |
| Prompt tokens | {metrics.get("total_prompt_tokens", 0):,} |
| Cached tokens | {metrics.get("total_cached_tokens", 0):,} |
| Completion tokens | {metrics.get("total_completion_tokens", 0):,} |

## Interpretation

This is evidence that the LHTB container/Harbor/StepFun path is executable and
that the task creates substantial long-horizon compute. It is not a model
quality score and it does not measure LongHorizonOS yet.

The useful next experiment is to checkpoint this same task after a verified
milestone, mutate a requirement or artifact, and compare full restart,
resume/test-and-fix, and LHOS selective repair.

## Reproducibility limitations

{chr(10).join(f"- {item}" for item in profile["limitations"])}
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("trial_dir", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    profile = build_profile(args.trial_dir)
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "profile.json").write_text(
        json.dumps(profile, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (output_dir / "PROFILE.zh-CN.md").write_text(render(profile), encoding="utf-8")
    print(output_dir / "profile.json")
    print(output_dir / "PROFILE.zh-CN.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
