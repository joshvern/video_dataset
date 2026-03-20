from __future__ import annotations

from src.config import load_config
from src.download import download_playlist_media
from src.playlist_ingestion import ingest_playlist_metadata
from src.segmentation import get_strategy
from src.topic_extraction import extract_topic_key
from src.transcripts import fetch_transcript, write_transcript_result
from src.writers import build_long_rows, build_review_rows, build_wide_rows, write_outputs


def run_pipeline(config_path: str) -> dict[str, int | str]:
    config = load_config(config_path)
    playlist_payload = ingest_playlist_metadata(config)
    download_playlist_media(config)

    transcript_payloads: dict[str, dict] = {}
    segmentation_payloads = {}
    topic_keys: dict[str, str] = {}

    strategy = get_strategy(config.segmentation_strategy)
    for entry in playlist_payload.get("entries", []):
        video_id = entry["video_id"]
        transcript_result = fetch_transcript(video_id, config)
        write_transcript_result(transcript_result, config.raw_transcript_dir)
        transcript_payload = transcript_result.to_dict()
        transcript_payloads[video_id] = transcript_payload
        segmentation_payloads[video_id] = strategy.segment(transcript_payload["snippets"], entry)
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
