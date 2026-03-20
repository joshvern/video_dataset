from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.models import PlaylistConfig


def ingest_playlist_metadata(config: PlaylistConfig) -> dict[str, Any]:
    from yt_dlp import YoutubeDL

    config.raw_playlist_path.parent.mkdir(parents=True, exist_ok=True)
    ydl_opts = {
        "extract_flat": True,
        "quiet": True,
        "skip_download": True,
        **config.yt_dlp,
    }
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(config.playlist_url, download=False)

    playlist_payload = {
        "playlist_id": config.playlist_id,
        "playlist_name": config.playlist_name,
        "playlist_url": config.playlist_url,
        "extractor": info.get("extractor"),
        "webpage_url": info.get("webpage_url"),
        "title": info.get("title"),
        "description": info.get("description"),
        "entries": [
            {
                "playlist_index": entry.get("playlist_index"),
                "video_id": entry.get("id"),
                "title": entry.get("title"),
                "url": entry.get("url") or f"https://www.youtube.com/watch?v={entry.get('id')}",
                "channel": entry.get("channel") or entry.get("uploader"),
                "duration": entry.get("duration"),
                "availability": entry.get("availability"),
            }
            for entry in info.get("entries", [])
            if entry
        ],
    }

    config.raw_playlist_path.write_text(json.dumps(playlist_payload, indent=2), encoding="utf-8")
    return playlist_payload


def load_playlist_metadata(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))
