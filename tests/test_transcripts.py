import sys
from types import ModuleType, SimpleNamespace

from src.models import PlaylistConfig
from src.transcripts import fetch_transcript


def _build_config() -> PlaylistConfig:
    return PlaylistConfig(
        playlist_id="wired_5_levels",
        playlist_url="https://example.com/playlist",
        playlist_name="WIRED 5 Levels",
        segmentation_strategy="wired_5_levels",
        raw_playlist_path="/tmp/raw_playlist.json",
        raw_transcript_dir="/tmp/transcripts",
        processed_long_path="/tmp/long.jsonl",
        processed_wide_path="/tmp/wide.csv",
        review_low_confidence_path="/tmp/review.jsonl",
        transcript_request_delay_seconds=0.0,
    )


def test_fetch_transcript_supports_instance_based_api(monkeypatch):
    fake_pkg = ModuleType("youtube_transcript_api")
    fake_errors = ModuleType("youtube_transcript_api._errors")

    class NoTranscriptFound(Exception):
        pass

    class TranscriptsDisabled(Exception):
        pass

    class FakeTranscript:
        language_code = "en"
        is_generated = False

        def fetch(self):
            return [SimpleNamespace(text=" Hello world ", start=1.25, duration=3.5)]

    class FakeTranscriptList:
        def find_manually_created_transcript(self, languages):
            assert languages == ["en"]
            return FakeTranscript()

        def find_generated_transcript(self, languages):
            raise AssertionError("manual transcript should be preferred in this test")

    class FakeApi:
        def list(self, video_id):
            assert video_id == "abc123"
            return FakeTranscriptList()

    fake_pkg.YouTubeTranscriptApi = FakeApi
    fake_errors.NoTranscriptFound = NoTranscriptFound
    fake_errors.TranscriptsDisabled = TranscriptsDisabled

    monkeypatch.setitem(sys.modules, "youtube_transcript_api", fake_pkg)
    monkeypatch.setitem(sys.modules, "youtube_transcript_api._errors", fake_errors)

    result = fetch_transcript("abc123", _build_config())

    assert result.error is None
    assert result.language_code == "en"
    assert result.is_generated is False
    assert len(result.snippets) == 1
    assert result.snippets[0].text == "Hello world"
    assert result.snippets[0].start == 1.25
    assert result.snippets[0].duration == 3.5