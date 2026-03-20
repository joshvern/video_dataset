import csv
import json
from types import SimpleNamespace

from src.pipeline import run_ingestion_pipeline, run_processing_pipeline


def _write_config(tmp_path, playlist_id: str = "demo"):
    config_path = tmp_path / "configs" / "playlists" / f"{playlist_id}.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        f"""playlist:
  id: {playlist_id}
  name: Demo Playlist
  url: https://example.com/playlist

pipeline:
  segmentation_strategy: generic
  normalized_buckets:
    - child
    - teen
    - college_student
    - grad_student
    - expert
  segmentation:
    low_confidence_threshold: 0.5

output:
  raw_playlist_path: data/raw/playlists/{playlist_id}.json
  raw_transcript_dir: data/raw/transcripts
  processed_long_path: data/processed/long/{playlist_id}.jsonl
  processed_wide_path: data/processed/wide/{playlist_id}.csv
  review_low_confidence_path: data/processed/review/{playlist_id}_low_confidence.jsonl
  write_parquet: false
""",
        encoding="utf-8",
    )
    return config_path


def test_run_processing_pipeline_builds_outputs_from_existing_raw_artifacts(tmp_path):
    config_path = _write_config(tmp_path)
    raw_playlist_path = tmp_path / "data" / "raw" / "playlists" / "demo.json"
    raw_playlist_path.parent.mkdir(parents=True, exist_ok=True)
    raw_playlist_path.write_text(
        json.dumps(
            {
                "playlist_id": "demo",
                "playlist_name": "Demo Playlist",
                "entries": [
                    {
                        "playlist_index": 1,
                        "video_id": "abc123",
                        "title": "Physics in 5 Levels",
                        "url": "https://youtu.be/abc123",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    transcript_path = tmp_path / "data" / "raw" / "transcripts" / "abc123.json"
    transcript_path.parent.mkdir(parents=True, exist_ok=True)
    transcript_path.write_text(
        json.dumps(
            {
                "video_id": "abc123",
                "source": "fixture",
                "language_code": "en",
                "is_generated": False,
                "error": None,
                "snippets": [{"text": "  A transcript with  extra spacing.  ", "start": 0.0, "duration": 1.0}],
            }
        ),
        encoding="utf-8",
    )

    result = run_processing_pipeline(str(config_path))

    assert result == {
        "playlist_id": "demo",
        "videos": 1,
        "long_rows": 5,
        "wide_rows": 1,
        "review_rows": 5,
    }

    long_rows = [
        json.loads(line)
        for line in (tmp_path / "data" / "processed" / "long" / "demo.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert len(long_rows) == 5
    assert {row["segment_text"] for row in long_rows} == {"A transcript with extra spacing."}
    assert {row["transcript_error"] for row in long_rows} == {None}

    with (tmp_path / "data" / "processed" / "wide" / "demo.csv").open(encoding="utf-8", newline="") as handle:
        wide_rows = list(csv.DictReader(handle))

    assert len(wide_rows) == 1
    assert wide_rows[0]["child_text"] == "A transcript with extra spacing."
    assert wide_rows[0]["topic_key"] == "physics"


def test_run_processing_pipeline_handles_missing_raw_transcripts(tmp_path):
    config_path = _write_config(tmp_path, playlist_id="missing_transcript")
    raw_playlist_path = tmp_path / "data" / "raw" / "playlists" / "missing_transcript.json"
    raw_playlist_path.parent.mkdir(parents=True, exist_ok=True)
    raw_playlist_path.write_text(
        json.dumps(
            {
                "playlist_id": "missing_transcript",
                "playlist_name": "Demo Playlist",
                "entries": [
                    {
                        "playlist_index": 1,
                        "video_id": "missing123",
                        "title": "Biology in 5 Levels",
                        "url": "https://youtu.be/missing123",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = run_processing_pipeline(str(config_path))

    assert result["long_rows"] == 5
    review_rows = [
        json.loads(line)
        for line in (tmp_path / "data" / "processed" / "review" / "missing_transcript_low_confidence.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]

    assert len(review_rows) == 5
    assert {row["transcript_error"] for row in review_rows} == {"raw_transcript_missing"}
    assert {row["segment_text"] for row in review_rows} == {""}


def test_run_ingestion_pipeline_applies_configured_transcript_delay(tmp_path, monkeypatch):
    config_path = _write_config(tmp_path, playlist_id="ingest_delay")
    config_text = config_path.read_text(encoding="utf-8")
    config_path.write_text(
        config_text.replace("segmentation_strategy: generic\n", "segmentation_strategy: generic\n  transcript_request_delay_seconds: 1.5\n"),
        encoding="utf-8",
    )

    playlist_payload = {
        "playlist_id": "ingest_delay",
        "playlist_name": "Demo Playlist",
        "entries": [
            {"playlist_index": 1, "video_id": "first", "title": "One in 5 Levels", "url": "https://youtu.be/first"},
            {"playlist_index": 2, "video_id": "second", "title": "Two in 5 Levels", "url": "https://youtu.be/second"},
        ],
    }
    sleep_calls: list[float] = []

    monkeypatch.setattr("src.pipeline.ingest_playlist_metadata", lambda config: playlist_payload)
    monkeypatch.setattr("src.pipeline.download_playlist_media", lambda config: None)
    monkeypatch.setattr(
        "src.pipeline.fetch_transcript",
        lambda video_id, config: SimpleNamespace(
            to_dict=lambda: {
                "video_id": video_id,
                "source": "fixture",
                "language_code": "en",
                "is_generated": False,
                "error": None,
                "snippets": [{"text": f"transcript for {video_id}", "start": 0.0, "duration": 1.0}],
            }
        ),
    )
    monkeypatch.setattr("src.pipeline.write_transcript_result", lambda result, output_dir: None)
    monkeypatch.setattr("src.pipeline.time.sleep", lambda seconds: sleep_calls.append(seconds))

    result = run_ingestion_pipeline(str(config_path))

    assert result["videos"] == 2
    assert sleep_calls == [1.5]