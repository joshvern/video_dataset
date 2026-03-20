from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.models import PlaylistConfig, TranscriptResult, TranscriptSnippet


def _list_transcripts(video_id: str):
    from youtube_transcript_api import YouTubeTranscriptApi

    if hasattr(YouTubeTranscriptApi, "list_transcripts"):
        return YouTubeTranscriptApi.list_transcripts(video_id)

    api = YouTubeTranscriptApi()
    if hasattr(api, "list"):
        return api.list(video_id)

    raise AttributeError("youtube_transcript_api does not expose a transcript listing API")


def _coerce_snippet(item: Any) -> TranscriptSnippet:
    if isinstance(item, dict):
        text = item.get("text", "")
        start = item.get("start", 0.0)
        duration = item.get("duration", 0.0)
    else:
        text = getattr(item, "text", "")
        start = getattr(item, "start", 0.0)
        duration = getattr(item, "duration", 0.0)

    return TranscriptSnippet(
        text=str(text).strip(),
        start=float(start),
        duration=float(duration),
    )


def _pick_transcript(video_id: str, languages: list[str], manual_first: bool):
    from youtube_transcript_api._errors import NoTranscriptFound

    transcript_list = _list_transcripts(video_id)
    finders = []
    if manual_first:
        finders = [transcript_list.find_manually_created_transcript, transcript_list.find_generated_transcript]
    else:
        finders = [transcript_list.find_generated_transcript, transcript_list.find_manually_created_transcript]

    for finder in finders:
        try:
            return finder(languages)
        except NoTranscriptFound:
            continue
    raise NoTranscriptFound(video_id, languages, transcript_list)


def fetch_transcript(video_id: str, config: PlaylistConfig) -> TranscriptResult:
    try:
        from youtube_transcript_api._errors import NoTranscriptFound, TranscriptsDisabled

        transcript = _pick_transcript(
            video_id=video_id,
            languages=config.transcript_languages,
            manual_first=config.manual_transcript_first,
        )
        fetched_transcript = transcript.fetch()
        snippets = [_coerce_snippet(item) for item in fetched_transcript]
        return TranscriptResult(
            video_id=video_id,
            source="youtube_transcript_api",
            language_code=getattr(transcript, "language_code", getattr(fetched_transcript, "language_code", None)),
            is_generated=getattr(transcript, "is_generated", getattr(fetched_transcript, "is_generated", None)),
            snippets=snippets,
        )
    except ModuleNotFoundError as exc:
        return TranscriptResult(
            video_id=video_id,
            source="youtube_transcript_api",
            language_code=None,
            is_generated=None,
            snippets=[],
            error=f"dependency_missing: {exc}",
        )
    except (NoTranscriptFound, TranscriptsDisabled) as exc:
        return TranscriptResult(
            video_id=video_id,
            source="youtube_transcript_api",
            language_code=None,
            is_generated=None,
            snippets=[],
            error=str(exc),
        )
    except Exception as exc:  # pragma: no cover - defensive integration guard
        return TranscriptResult(
            video_id=video_id,
            source="youtube_transcript_api",
            language_code=None,
            is_generated=None,
            snippets=[],
            error=f"unexpected_error: {exc}",
        )


def write_transcript_result(result: TranscriptResult, output_dir: str | Path) -> Path:
    output_path = Path(output_dir) / f"{result.video_id}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result.to_dict(), indent=2), encoding="utf-8")
    return output_path


def load_transcript_result(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))
