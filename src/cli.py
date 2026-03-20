from __future__ import annotations

import argparse
import json
import sys

from src.pipeline import run_pipeline



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="YouTube playlist dataset pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the ingestion pipeline")
    run_parser.add_argument("--config", required=True, help="Path to playlist YAML config")
    return parser



def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "run":
        try:
            result = run_pipeline(args.config)
        except ModuleNotFoundError as exc:
            parser.exit(
                status=1,
                message=(
                    f"Missing runtime dependency: {exc.name}. "
                    "Install project dependencies first, e.g. `pip install -e .[dev]`.\n"
                ),
            )
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
