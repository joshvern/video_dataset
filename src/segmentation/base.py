from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from src.models import LevelSegment, NORMALIZED_BUCKETS
from src.normalization import normalize_text


class SegmentationStrategy(ABC):
    @abstractmethod
    def segment(self, transcript_snippets: list[dict[str, Any]], metadata: dict[str, Any]) -> list[LevelSegment]:
        raise NotImplementedError


class GenericSegmentationStrategy(SegmentationStrategy):
    def segment(self, transcript_snippets: list[dict[str, Any]], metadata: dict[str, Any]) -> list[LevelSegment]:
        combined = normalize_text(" ".join(snippet.get("text", "") for snippet in transcript_snippets))
        return [
            LevelSegment(
                level_key=level,
                raw_label="generic_fallback",
                content=combined,
                confidence=0.1,
                start_index=None,
                end_index=None,
                evidence=["generic fallback used; transcript not segmented"],
            )
            for level in NORMALIZED_BUCKETS
        ]


def get_strategy(name: str) -> SegmentationStrategy:
    if name == "wired_5_levels":
        from src.segmentation.wired_5_levels import WiredFiveLevelsStrategy

        return WiredFiveLevelsStrategy()
    if name == "generic":
        return GenericSegmentationStrategy()
    raise ValueError(f"Unknown segmentation strategy: {name}")
