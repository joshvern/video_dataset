from __future__ import annotations

from pathlib import Path
from typing import Any

from src.models import NORMALIZED_BUCKETS, PlaylistConfig


class ConfigError(ValueError):
    """Raised when the YAML config is invalid."""


REQUIRED_ROOT_KEYS = {"playlist", "pipeline", "output"}


def _require(mapping: dict[str, Any], key: str, context: str) -> Any:
    if key not in mapping:
        raise ConfigError(f"Missing required key '{key}' in {context}.")
    return mapping[key]


def _path(value: str | Path, base_dir: Path) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path


def _coerce_scalar(value: str) -> Any:
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in {"null", "none"}:
        return None
    if value == "{}":
        return {}
    if value == "[]":
        return []
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_coerce_scalar(part.strip()) for part in inner.split(",")]
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def _simple_yaml_load(text: str) -> dict[str, Any]:
    lines = text.splitlines()
    root: dict[str, Any] = {}
    stack: list[tuple[int, Any]] = [(-1, root)]

    for idx, raw_line in enumerate(lines):
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        line = raw_line.strip()

        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]

        if line.startswith("- "):
            if not isinstance(parent, list):
                raise ConfigError("Invalid YAML structure: list item without a list parent.")
            parent.append(_coerce_scalar(line[2:].strip()))
            continue

        key, sep, remainder = line.partition(":")
        if not sep:
            raise ConfigError(f"Invalid YAML line: {raw_line}")
        key = key.strip()
        value = remainder.strip()

        if value == "":
            next_is_list = False
            for future_raw in lines[idx + 1 :]:
                if not future_raw.strip() or future_raw.lstrip().startswith("#"):
                    continue
                future_indent = len(future_raw) - len(future_raw.lstrip(" "))
                if future_indent <= indent:
                    break
                next_is_list = future_raw.strip().startswith("- ")
                break
            container: Any = [] if next_is_list else {}
            if isinstance(parent, dict):
                parent[key] = container
            else:
                raise ConfigError("Invalid YAML structure: nested mapping inside a list is unsupported.")
            stack.append((indent, container))
        else:
            if not isinstance(parent, dict):
                raise ConfigError("Invalid YAML structure: scalar under list is unsupported.")
            parent[key] = _coerce_scalar(value)

    return root


def _load_yaml(config_path: Path) -> dict[str, Any]:
    text = config_path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:
        return _simple_yaml_load(text)
    return yaml.safe_load(text) or {}


def load_config(path: str | Path) -> PlaylistConfig:
    config_path = Path(path).resolve()
    payload = _load_yaml(config_path)

    missing = REQUIRED_ROOT_KEYS - payload.keys()
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ConfigError(f"Missing required root sections: {missing_str}.")

    playlist = payload["playlist"]
    pipeline = payload["pipeline"]
    output = payload["output"]
    base_dir = config_path.parent.parent.parent if len(config_path.parents) >= 3 else config_path.parent

    normalized_buckets = pipeline.get("normalized_buckets", NORMALIZED_BUCKETS)
    if normalized_buckets != NORMALIZED_BUCKETS:
        raise ConfigError(
            "This pipeline currently requires the normalized buckets to be exactly: "
            + ", ".join(NORMALIZED_BUCKETS)
        )

    return PlaylistConfig(
        playlist_id=_require(playlist, "id", "playlist"),
        playlist_url=_require(playlist, "url", "playlist"),
        playlist_name=playlist.get("name", _require(playlist, "id", "playlist")),
        segmentation_strategy=_require(pipeline, "segmentation_strategy", "pipeline"),
        raw_playlist_path=_path(_require(output, "raw_playlist_path", "output"), base_dir),
        raw_transcript_dir=_path(_require(output, "raw_transcript_dir", "output"), base_dir),
        processed_long_path=_path(_require(output, "processed_long_path", "output"), base_dir),
        processed_wide_path=_path(_require(output, "processed_wide_path", "output"), base_dir),
        review_low_confidence_path=_path(_require(output, "review_low_confidence_path", "output"), base_dir),
        download_media=pipeline.get("download_media", False),
        media_output_dir=_path(pipeline["media_output_dir"], base_dir) if pipeline.get("media_output_dir") else None,
        write_parquet=output.get("write_parquet", False),
        long_parquet_path=_path(output["long_parquet_path"], base_dir) if output.get("long_parquet_path") else None,
        wide_parquet_path=_path(output["wide_parquet_path"], base_dir) if output.get("wide_parquet_path") else None,
        transcript_languages=pipeline.get("transcript_languages", ["en"]),
        manual_transcript_first=pipeline.get("manual_transcript_first", True),
        transcript_request_delay_seconds=float(pipeline.get("transcript_request_delay_seconds", 0.0) or 0.0),
        topic_overrides=pipeline.get("topic_overrides", {}),
        segmentation=pipeline.get("segmentation", {}),
        llm_topic_fallback=pipeline.get("llm_topic_fallback", {}),
        yt_dlp=pipeline.get("yt_dlp", {}),
    )
