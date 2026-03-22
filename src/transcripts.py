from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from requests import Session
from youtube_transcript_api import YouTubeTranscriptApi

from src.models import PlaylistConfig, TranscriptResult, TranscriptSnippet


def _make_api() -> YouTubeTranscriptApi:
    http_client = Session()
    http_client.trust_env = False
    http_client.proxies = {}
    return YouTubeTranscriptApi(http_client=http_client)


def _list_transcripts(video_id: str, api: YouTubeTranscriptApi | None = None):
    if api is None:
        api = _make_api()
    return api.list(video_id)


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


def _pick_transcript(video_id: str, languages: list[str], manual_first: bool, api: YouTubeTranscriptApi | None = None):
    from youtube_transcript_api._errors import NoTranscriptFound

    transcript_list = _list_transcripts(video_id, api=api)
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


def fetch_transcript(video_id: str, config: PlaylistConfig, api: YouTubeTranscriptApi | None = None) -> TranscriptResult:
    try:
        from youtube_transcript_api._errors import NoTranscriptFound, TranscriptsDisabled

        if api is None:
            api = _make_api()
        transcript = _pick_transcript(
            video_id=video_id,
            languages=config.transcript_languages,
            manual_first=config.manual_transcript_first,
            api=api,
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
