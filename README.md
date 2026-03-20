# Video Dataset Pipeline

A config-driven Python pipeline for ingesting YouTube playlists into analysis-ready datasets, starting with WIRED's "5 Levels" format.

## Features

- Extract playlist metadata with `yt-dlp`.
- Optionally download playlist media with `yt-dlp`.
- Fetch YouTube transcripts with `youtube-transcript-api`, preferring manual transcripts over generated transcripts.
- Preserve raw transcript snippets with timestamps for traceability and reprocessing.
- Segment transcripts into normalized audience buckets:
  - `child`
  - `teen`
  - `college_student`
  - `grad_student`
  - `expert`
- Generate both long and wide datasets.
- Emit per-segment confidence scores and a low-confidence review export.
- Keep raw, processed, and review outputs separated.
- Add future playlists by creating another YAML config.

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

Optional Parquet support:

```bash
pip install -e .[dev,parquet]
```

## Run the WIRED 5 Levels pipeline

```bash
python -m src.cli run --config configs/playlists/wired_5_levels.yaml
```

## Config schema

The repository includes a machine-readable schema at `schemas/playlist_config.schema.json` that documents the expected YAML structure for playlist configs.

## Project structure

```text
configs/playlists/                  # Playlist-specific YAML configs
src/                                # Pipeline modules
tests/                              # Automated tests

data/raw/playlists/<playlist_id>.json
                                  # Raw playlist metadata extracted by yt-dlp
data/raw/transcripts/<video_id>.json
                                  # Raw transcript snippets with timestamps
data/processed/long/<playlist_id>.jsonl
                                  # One row per (video, level)
data/processed/wide/<playlist_id>.csv
                                  # One row per video with all five levels
data/processed/review/<playlist_id>_low_confidence.jsonl
                                  # Rows flagged for manual review
```

## Pipeline modules

- `src/config.py`: YAML config loading and validation.
- `src/playlist_ingestion.py`: Playlist metadata extraction.
- `src/download.py`: Optional media download.
- `src/transcripts.py`: Transcript retrieval and raw transcript persistence.
- `src/topic_extraction.py`: Canonical `topic_key` extraction with optional fallback hook.
- `src/segmentation/`: Pluggable transcript segmentation strategies.
- `src/normalization.py`: Transcript normalization helpers.
- `src/writers.py`: Long/wide/review dataset generation and writing.
- `src/pipeline.py`: End-to-end orchestration.
- `src/cli.py`: Command line interface.

## Segmentation strategies

### `wired_5_levels`

The initial strategy looks for transcript markers such as `Level 1`, `Level 2`, and audience phrases like `college student` or `expert`. It slices the transcript into five normalized buckets and assigns heuristic confidence scores.

### `generic`

A scaffold strategy that copies the normalized transcript into every bucket with low confidence. This is useful as a placeholder for future playlist formats.

## Traceability and reprocessing

Each long and wide row includes:

- playlist and video metadata
- transcript language / generated status / transcript error
- segmentation confidence and evidence
- the path of the raw transcript JSON used to create the row

Transcript failures are non-fatal: the pipeline still writes playlist metadata and emits empty/low-confidence segments when transcripts are missing or unusable.

## Testing

```bash
pytest
```

## Notes for future extension

- Add more playlist YAMLs under `configs/playlists/`.
- Replace the topic extraction fallback hook with an actual LLM client if desired.
- Add richer segmentation strategies per channel or series.
- Expand the review workflow to include adjudication state and reviewer notes.
