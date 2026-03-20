from pathlib import Path

from src.config import load_config
from src.models import NORMALIZED_BUCKETS


def test_load_config_resolves_paths_from_repo_root():
    config_path = Path("configs/playlists/wired_5_levels.yaml").resolve()
    config = load_config(config_path)
    repo_root = config_path.parents[2]

    assert config.playlist_id == "wired_5_levels"
    assert config.segmentation_strategy == "wired_5_levels"
    assert config.raw_playlist_path == repo_root / "data/raw/playlists/wired_5_levels.json"
    assert config.processed_long_path.name == "wired_5_levels.jsonl"
    assert config.transcript_languages == ["en"]
    assert config.transcript_request_delay_seconds == 2.0
    assert NORMALIZED_BUCKETS == ["child", "teen", "college_student", "grad_student", "expert"]
