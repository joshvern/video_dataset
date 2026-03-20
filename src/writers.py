from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from src.models import NORMALIZED_BUCKETS, LevelSegment, PlaylistConfig


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def build_long_rows(
    playlist_payload: dict[str, Any],
    transcript_payloads: dict[str, dict[str, Any]],
    segmentation_payloads: dict[str, list[LevelSegment]],
    topic_keys: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in playlist_payload.get("entries", []):
        video_id = entry["video_id"]
        transcript_payload = transcript_payloads.get(video_id, {})
        segments = segmentation_payloads.get(video_id, [])
        for segment in segments:
            rows.append(
                {
                    "playlist_id": playlist_payload.get("playlist_id"),
                    "playlist_name": playlist_payload.get("playlist_name"),
                    "video_id": video_id,
                    "playlist_index": entry.get("playlist_index"),
                    "video_title": entry.get("title"),
                    "video_url": entry.get("url"),
                    "topic_key": topic_keys.get(video_id, "unknown_topic"),
                    "level_key": segment.level_key,
                    "raw_label": segment.raw_label,
                    "segmentation_confidence": segment.confidence,
                    "transcript_language_code": transcript_payload.get("language_code"),
                    "transcript_is_generated": transcript_payload.get("is_generated"),
                    "transcript_error": transcript_payload.get("error"),
                    "segment_start_index": segment.start_index,
                    "segment_end_index": segment.end_index,
                    "segment_text": segment.content,
                    "segment_evidence": " | ".join(segment.evidence),
                    "raw_transcript_path": f"data/raw/transcripts/{video_id}.json",
                }
            )
    return rows


def build_wide_rows(long_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in long_rows:
        video_id = row["video_id"]
        bucket = grouped.setdefault(
            video_id,
            {
                "playlist_id": row["playlist_id"],
                "playlist_name": row["playlist_name"],
                "video_id": video_id,
                "playlist_index": row["playlist_index"],
                "video_title": row["video_title"],
                "video_url": row["video_url"],
                "topic_key": row["topic_key"],
                "transcript_language_code": row["transcript_language_code"],
                "transcript_is_generated": row["transcript_is_generated"],
                "transcript_error": row["transcript_error"],
                "raw_transcript_path": row["raw_transcript_path"],
            },
        )
        level_key = row["level_key"]
        bucket[f"{level_key}_text"] = row["segment_text"]
        bucket[f"{level_key}_confidence"] = row["segmentation_confidence"]
        bucket[f"{level_key}_evidence"] = row["segment_evidence"]

    for bucket in grouped.values():
        for level_key in NORMALIZED_BUCKETS:
            bucket.setdefault(f"{level_key}_text", "")
            bucket.setdefault(f"{level_key}_confidence", 0.0)
            bucket.setdefault(f"{level_key}_evidence", "")

    return sorted(grouped.values(), key=lambda row: (row.get("playlist_index") or 0, row["video_id"]))


def write_jsonl(rows: list[dict[str, Any]], path: Path) -> None:
    _ensure_parent(path)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    _ensure_parent(path)
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_optional_parquet(rows: list[dict[str, Any]], path: Path | None) -> None:
    if not rows or not path:
        return
    try:
        import pandas as pd
    except ImportError:
        return
    _ensure_parent(path)
    pd.DataFrame(rows).to_parquet(path, index=False)


def build_review_rows(long_rows: list[dict[str, Any]], threshold: float) -> list[dict[str, Any]]:
    return [row for row in long_rows if float(row.get("segmentation_confidence") or 0.0) < threshold]


def write_outputs(config: PlaylistConfig, long_rows: list[dict[str, Any]], wide_rows: list[dict[str, Any]], review_rows: list[dict[str, Any]]) -> None:
    write_jsonl(long_rows, config.processed_long_path)
    write_csv(wide_rows, config.processed_wide_path)
    write_jsonl(review_rows, config.review_low_confidence_path)
    if config.write_parquet:
        write_optional_parquet(long_rows, config.long_parquet_path)
        write_optional_parquet(wide_rows, config.wide_parquet_path)
