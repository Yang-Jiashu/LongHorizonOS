"""Export official SWE-bench prediction files from a host-native pair result."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load_result(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _prediction(instance_id: str, model_name: str, model_patch: str) -> list[dict[str, str]]:
    if not model_patch.strip():
        raise ValueError(f"empty model patch for {model_name}")
    return [
        {
            "instance_id": instance_id,
            "model_name_or_path": model_name,
            "model_patch": model_patch,
        }
    ]


def export_predictions(
    result_path: Path,
    *,
    output_dir: Path,
    static_model_name: str,
    lhos_model_name: str,
) -> None:
    result = _load_result(result_path)
    instance_id = str(result["instance_id"])
    static_patch = str(result["static"]["evaluation"]["agent_patch"])
    lhos_patch = str(result["lhos"]["evaluation"]["agent_patch"])
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "static.patch").write_text(static_patch, encoding="utf-8")
    (output_dir / "lhos.patch").write_text(lhos_patch, encoding="utf-8")
    (output_dir / "predictions-static.json").write_text(
        json.dumps(
            _prediction(instance_id, static_model_name, static_patch),
            ensure_ascii=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (output_dir / "predictions.json").write_text(
        json.dumps(
            _prediction(instance_id, lhos_model_name, lhos_patch),
            ensure_ascii=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("result", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--static-model-name", default="dsh-static")
    parser.add_argument("--lhos-model-name", default="dsh-lhos")
    args = parser.parse_args()
    result_path = args.result.resolve()
    output_dir = (args.output_dir or result_path.parent).resolve()
    export_predictions(
        result_path,
        output_dir=output_dir,
        static_model_name=args.static_model_name,
        lhos_model_name=args.lhos_model_name,
    )
    print(output_dir / "predictions-static.json")
    print(output_dir / "predictions.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
