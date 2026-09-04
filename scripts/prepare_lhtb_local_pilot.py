"""Prepare a Windows-compatible 46-task LHTB copy without mutating upstream."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path


def _task_dirs(root: Path) -> list[Path]:
    return sorted(
        path for path in root.iterdir() if path.is_dir() and (path / "task.toml").is_file()
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    source = args.source.resolve()
    destination = args.destination.resolve()
    if destination.exists():
        raise SystemExit(f"destination already exists: {destination}")
    tasks = _task_dirs(source)
    if len(tasks) != 46:
        raise SystemExit(f"expected 46 LHTB tasks, found {len(tasks)}")
    shutil.copytree(source, destination)
    modified: list[str] = []
    hashes: dict[str, str] = {}
    for task in _task_dirs(destination):
        config = task / "task.toml"
        raw = config.read_text(encoding="utf-8")
        updated = raw.replace("allow_internet = false", "allow_internet = true")
        if updated != raw:
            config.write_text(updated, encoding="utf-8")
            modified.append(task.name)
        hashes[task.name] = hashlib.sha256(config.read_bytes()).hexdigest()
    manifest = {
        "schema_version": "lhos-lhtb-local-pilot.v1",
        "source": str(source),
        "destination": str(destination),
        "task_count": len(tasks),
        "modified_allow_internet_tasks": modified,
        "modified_count": len(modified),
        "task_toml_sha256": hashes,
        "official_score": False,
        "reason": (
            "Windows Docker/Harbor cannot enforce allow_internet=false; both "
            "comparison arms use this identical copied task set."
        ),
    }
    args.manifest.resolve().parent.mkdir(parents=True, exist_ok=True)
    args.manifest.resolve().write_text(
        json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(manifest, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
