from __future__ import annotations

import re
from typing import Iterable


def normalize_text(value: str) -> str:
    cleaned = value.replace("\n", " ").replace("\t", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def join_snippet_texts(snippets: Iterable[dict]) -> str:
    return normalize_text(" ".join(snippet.get("text", "") for snippet in snippets))
