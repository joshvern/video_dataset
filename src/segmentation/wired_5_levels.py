from __future__ import annotations

import re
from typing import Any

from src.models import LevelSegment, NORMALIZED_BUCKETS
from src.normalization import normalize_text
from src.segmentation.base import SegmentationStrategy

LEVEL_MARKERS: list[tuple[str, list[str]]] = [
    ("child", [r"\blevel\s*1\b", r"\bfor a child\b", r"\bto a child\b"]),
    ("teen", [r"\blevel\s*2\b", r"\bfor a teen(?:ager)?\b", r"\bto a teen(?:ager)?\b"]),
    ("college_student", [r"\blevel\s*3\b", r"\bcollege student\b", r"\bundergrad(?:uate)?\b"]),
    ("grad_student", [r"\blevel\s*4\b", r"\bgrad(?:uate)? student\b", r"\bphd student\b"]),
    ("expert", [r"\blevel\s*5\b", r"\bexpert\b", r"\bprofessor\b"]),
]


class WiredFiveLevelsStrategy(SegmentationStrategy):
    def segment(self, transcript_snippets: list[dict[str, Any]], metadata: dict[str, Any]) -> list[LevelSegment]:
        texts = [normalize_text(snippet.get("text", "")) for snippet in transcript_snippets]
        boundaries: list[tuple[int, str, str]] = []

        for idx, text in enumerate(texts):
            for level_key, patterns in LEVEL_MARKERS:
                for pattern in patterns:
                    if re.search(pattern, text, flags=re.IGNORECASE):
                        boundaries.append((idx, level_key, pattern))
                        break
                else:
                    continue
                break

        ordered_boundaries: list[tuple[int, str, str]] = []
        seen_levels: set[str] = set()
        for idx, level_key, evidence in sorted(boundaries, key=lambda item: item[0]):
            if level_key not in seen_levels:
                ordered_boundaries.append((idx, level_key, evidence))
                seen_levels.add(level_key)

        if not ordered_boundaries:
            combined = normalize_text(" ".join(texts))
            return [
                LevelSegment(
                    level_key=level,
                    raw_label="unmatched",
                    content=combined,
                    confidence=0.15,
                    evidence=["no explicit Wired level markers found"],
                )
                for level in NORMALIZED_BUCKETS
            ]

        segments: list[LevelSegment] = []
        for pos, level in enumerate(NORMALIZED_BUCKETS):
            boundary = next((item for item in ordered_boundaries if item[1] == level), None)
            if boundary is None:
                segments.append(
                    LevelSegment(
                        level_key=level,
                        raw_label="missing_marker",
                        content="",
                        confidence=0.0,
                        evidence=["expected level marker not found"],
                    )
                )
                continue

            start_index = boundary[0]
            next_start = len(texts)
            for future_idx, future_level, _ in ordered_boundaries:
                if future_idx > start_index and NORMALIZED_BUCKETS.index(future_level) > pos:
                    next_start = future_idx
                    break

            content = normalize_text(" ".join(texts[start_index:next_start]))
            evidence = [f"matched {boundary[2]} at snippet {start_index}"]
            span_size = max(next_start - start_index, 1)
            confidence = 0.6 if content else 0.0
            if span_size >= 2:
                confidence += 0.2
            if len(content.split()) >= 30:
                confidence += 0.15
            confidence = min(confidence, 0.99)
            segments.append(
                LevelSegment(
                    level_key=level,
                    raw_label=f"level_{pos + 1}",
                    content=content,
                    confidence=confidence,
                    start_index=start_index,
                    end_index=next_start - 1,
                    evidence=evidence,
                )
            )

        return segments
