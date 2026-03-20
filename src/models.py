from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

NORMALIZED_BUCKETS = [
    "child",
    "teen",
    "college_student",
    "grad_student",
    "expert",
]


@dataclass(slots=True)
class PlaylistConfig:
    playlist_id: str
    playlist_url: str
    playlist_name: str
    segmentation_strategy: str
    raw_playlist_path: Path
    raw_transcript_dir: Path
    processed_long_path: Path
    processed_wide_path: Path
    review_low_confidence_path: Path
    download_media: bool = False
    media_output_dir: Path | None = None
    write_parquet: bool = False
    long_parquet_path: Path | None = None
    wide_parquet_path: Path | None = None
    transcript_languages: list[str] = field(default_factory=lambda: ["en"])
    manual_transcript_first: bool = True
    transcript_request_delay_seconds: float = 0.0
    topic_overrides: dict[str, str] = field(default_factory=dict)
    segmentation: dict[str, Any] = field(default_factory=dict)
    llm_topic_fallback: dict[str, Any] = field(default_factory=dict)
    yt_dlp: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TranscriptSnippet:
    text: str
    start: float
    duration: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class TranscriptResult:
    video_id: str
    source: str | None
    language_code: str | None
    is_generated: bool | None
    snippets: list[TranscriptSnippet]
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "video_id": self.video_id,
            "source": self.source,
            "language_code": self.language_code,
            "is_generated": self.is_generated,
            "error": self.error,
            "snippets": [snippet.to_dict() for snippet in self.snippets],
        }


@dataclass(slots=True)
class LevelSegment:
    level_key: str
    raw_label: str
    content: str
    confidence: float
    start_index: int | None = None
    end_index: int | None = None
    evidence: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
