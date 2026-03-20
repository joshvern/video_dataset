from __future__ import annotations

import argparse
import json

from src.pipeline import run_ingestion_pipeline, run_pipeline, run_processing_pipeline



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="YouTube playlist dataset pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Build processed outputs from existing raw artifacts")
    run_parser.add_argument("--config", required=True, help="Path to playlist YAML config")

    ingest_parser = subparsers.add_parser("ingest", help="Refresh raw playlist and transcript artifacts, then process outputs")
    ingest_parser.add_argument("--config", required=True, help="Path to playlist YAML config")

    process_parser = subparsers.add_parser("process", help="Build processed outputs from existing raw artifacts")
    process_parser.add_argument("--config", required=True, help="Path to playlist YAML config")
    return parser



def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command in {"run", "ingest", "process"}:
        try:
            if args.command == "ingest":
                result = run_ingestion_pipeline(args.config)
            elif args.command == "process":
                result = run_processing_pipeline(args.config)
            else:
                result = run_pipeline(args.config)
        except ModuleNotFoundError as exc:
            parser.exit(
                status=1,
                message=(
                    f"Missing runtime dependency: {exc.name}. "
                    "Install project dependencies first, e.g. `pip install -e .[dev]`.\n"
                ),
            )
        except FileNotFoundError as exc:
            parser.exit(status=1, message=f"{exc}\n")
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
