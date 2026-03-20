from __future__ import annotations

from src.models import PlaylistConfig


def download_playlist_media(config: PlaylistConfig) -> None:
    if not config.download_media or not config.media_output_dir:
        return

    from yt_dlp import YoutubeDL

    config.media_output_dir.mkdir(parents=True, exist_ok=True)
    ydl_opts = {
        "quiet": True,
        "outtmpl": str(config.media_output_dir / "%(playlist_index)s_%(id)s.%(ext)s"),
    }
    with YoutubeDL(ydl_opts) as ydl:
        ydl.download([config.playlist_url])
