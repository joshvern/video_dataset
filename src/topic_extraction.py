from __future__ import annotations

import re
from collections.abc import Callable

from src.models import PlaylistConfig

TOPIC_PATTERNS = [
    re.compile(r"5\s+levels?\s+of\s+(?P<topic>.+)$", re.IGNORECASE),
    re.compile(r"(?P<topic>.+?)\s+in\s+5\s+levels?", re.IGNORECASE),
    re.compile(r"(?P<topic>.+?)\s*[:\-]\s*5\s+levels?", re.IGNORECASE),
]


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def default_topic_from_title(title: str) -> str:
    compact = re.sub(r"\[[^\]]+\]|\([^\)]+\)", "", title).strip()
    for pattern in TOPIC_PATTERNS:
        match = pattern.search(compact)
        if match:
            return slugify(match.group("topic"))
    compact = re.sub(r"\b(wired|explains?|expert|breaks? down)\b", "", compact, flags=re.IGNORECASE)
    return slugify(compact)


def extract_topic_key(
    title: str,
    config: PlaylistConfig,
    llm_fallback: Callable[[str], str] | None = None,
) -> str:
    if title in config.topic_overrides:
        return config.topic_overrides[title]

    topic_key = default_topic_from_title(title)
    if topic_key:
        return topic_key

    if llm_fallback and config.llm_topic_fallback.get("enabled"):
        return slugify(llm_fallback(title))

    return "unknown_topic"
