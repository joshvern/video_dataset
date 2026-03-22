from __future__ import annotations

from pathlib import Path
import time

from src.config import load_config
from src.download import download_playlist_media
from src.playlist_ingestion import ingest_playlist_metadata, load_playlist_metadata
from src.segmentation import get_strategy
from src.topic_extraction import extract_topic_key
from src.transcripts import fetch_transcript, load_transcript_result, write_transcript_result, _make_api
from src.writers import build_long_rows, build_review_rows, build_wide_rows, write_outputs


def _missing_transcript_payload(video_id: str) -> dict[str, object]:
    return {
        "video_id": video_id,
        "source": None,
        "language_code": None,
        "is_generated": None,
        "error": "raw_transcript_missing",
        "snippets": [],
    }


def _build_outputs(config, playlist_payload: dict, transcript_payloads: dict[str, dict]) -> dict[str, int | str]:
    segmentation_payloads = {}
    topic_keys: dict[str, str] = {}

    strategy = get_strategy(config.segmentation_strategy)
    for entry in playlist_payload.get("entries", []):
        video_id = entry["video_id"]
        transcript_payload = transcript_payloads.get(video_id, _missing_transcript_payload(video_id))
        segmentation_payloads[video_id] = strategy.segment(transcript_payload.get("snippets", []), entry)
        topic_keys[video_id] = extract_topic_key(entry.get("title", ""), config)

    long_rows = build_long_rows(playlist_payload, transcript_payloads, segmentation_payloads, topic_keys)
    wide_rows = build_wide_rows(long_rows)
    threshold = float(config.segmentation.get("low_confidence_threshold", 0.5))
    review_rows = build_review_rows(long_rows, threshold)
    write_outputs(config, long_rows, wide_rows, review_rows)

    return {
        "playlist_id": config.playlist_id,
        "videos": len(playlist_payload.get("entries", [])),
        "long_rows": len(long_rows),
        "wide_rows": len(wide_rows),
        "review_rows": len(review_rows),
    }


def _load_raw_transcript_payloads(entries: list[dict], raw_transcript_dir: Path) -> dict[str, dict]:
    transcript_payloads: dict[str, dict] = {}
    for entry in entries:
        video_id = entry["video_id"]
        transcript_path = raw_transcript_dir / f"{video_id}.json"
        if transcript_path.exists():
            transcript_payloads[video_id] = load_transcript_result(transcript_path)
        else:
            transcript_payloads[video_id] = _missing_transcript_payload(video_id)
    return transcript_payloads


def _delay_between_transcript_requests(delay_seconds: float, is_last_entry: bool) -> None:
    if delay_seconds > 0 and not is_last_entry:
        time.sleep(delay_seconds)


def run_ingestion_pipeline(config_path: str) -> dict[str, int | str]:
    config = load_config(config_path)
    playlist_payload = ingest_playlist_metadata(config)
    download_playlist_media(config)

    transcript_payloads: dict[str, dict] = {}
    entries = playlist_payload.get("entries", [])
    api = _make_api()
    for index, entry in enumerate(entries):
        video_id = entry["video_id"]
        transcript_result = fetch_transcript(video_id, config, api=api)
        write_transcript_result(transcript_result, config.raw_transcript_dir)
        transcript_payloads[video_id] = transcript_result.to_dict()
        _delay_between_transcript_requests(config.transcript_request_delay_seconds, index == len(entries) - 1)

    return _build_outputs(config, playlist_payload, transcript_payloads)


def run_processing_pipeline(config_path: str) -> dict[str, int | str]:
    config = load_config(config_path)
    if not config.raw_playlist_path.exists():
        raise FileNotFoundError(f"Raw playlist metadata not found: {config.raw_playlist_path}")

    playlist_payload = load_playlist_metadata(config.raw_playlist_path)
    transcript_payloads = _load_raw_transcript_payloads(playlist_payload.get("entries", []), config.raw_transcript_dir)
    return _build_outputs(config, playlist_payload, transcript_payloads)


def run_pipeline(config_path: str) -> dict[str, int | str]:
    return run_processing_pipeline(config_path)
