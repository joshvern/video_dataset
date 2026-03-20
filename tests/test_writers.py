from src.models import LevelSegment
from src.writers import build_long_rows, build_review_rows, build_wide_rows


def test_build_long_and_wide_rows_cover_all_levels():
    playlist_payload = {
        "playlist_id": "wired_5_levels",
        "playlist_name": "WIRED 5 Levels",
        "entries": [
            {
                "playlist_index": 1,
                "video_id": "abc123",
                "title": "Physics in 5 Levels",
                "url": "https://youtu.be/abc123",
            }
        ],
    }
    transcript_payloads = {
        "abc123": {
            "language_code": "en",
            "is_generated": False,
            "error": None,
            "snippets": [{"text": "level 1"}],
        }
    }
    segmentation_payloads = {
        "abc123": [
            LevelSegment(level_key="child", raw_label="level_1", content="A", confidence=0.9),
            LevelSegment(level_key="teen", raw_label="level_2", content="B", confidence=0.8),
            LevelSegment(level_key="college_student", raw_label="level_3", content="C", confidence=0.7),
            LevelSegment(level_key="grad_student", raw_label="level_4", content="D", confidence=0.6),
            LevelSegment(level_key="expert", raw_label="level_5", content="E", confidence=0.4),
        ]
    }
    topic_keys = {"abc123": "physics"}

    long_rows = build_long_rows(playlist_payload, transcript_payloads, segmentation_payloads, topic_keys)
    wide_rows = build_wide_rows(long_rows)
    review_rows = build_review_rows(long_rows, threshold=0.5)

    assert len(long_rows) == 5
    assert len(wide_rows) == 1
    assert wide_rows[0]["child_text"] == "A"
    assert wide_rows[0]["expert_text"] == "E"
    assert len(review_rows) == 1
    assert review_rows[0]["level_key"] == "expert"
