# spotify-etl-parquet

Converts Spotify MPD (Million Playlist Dataset) JSON slices (`mpd.slice.X-Y.json`, ~33 MB each, 1000 files, ~32 GB total) into Parquet, then builds a Streamlit analytics dashboard on top.

## Layer structure

| Layer | Script | Output | Description |
|---|---|---|---|
| Silver | `bench_convert.py` | `silver/` | 1 JSON → 2 parquet per file (playlists + tracks) |
| Gold | `build_gold.py` | `gold/` | Pre-aggregated tables for the dashboard |
| Dashboard | `dashboard.py` | — | Streamlit app reading from `gold/` |

Each JSON produces:
- `silver/mpd.slice.X-Y.playlist.parquet` — one row per playlist, no tracks
- `silver/mpd.slice.X-Y.tracks.parquet` — one row per track, with `pid` as join key

Gold tables built by `build_gold.py` via DuckDB glob scans of `silver/`:
- `top_tracks.parquet` — top 50k tracks by appearance count, with `unique_playlists`
- `top_artists.parquet` — top 20k artists by total appearances, with `unique_tracks` + `unique_playlists`
- `playlists.parquet` — all 1M playlists in one file (for distribution histograms)
- `kpis.parquet` — single-row summary (total playlists, unique tracks/artists/albums, total duration)

## Installation

```bash
pip install -r requirements.txt
```

Windows — add these exclusions (PowerShell admin) or each output file gets antivirus-scanned on write:

```powershell
Add-MpPreference -ExclusionPath "...\spotify-etl-parquet\silver"
Add-MpPreference -ExclusionPath "...\spotify-etl-parquet\gold"
Add-MpPreference -ExclusionPath "...\data"
```

## Usage

```bash
# Production run (validated: 32 GB in 74 s, ~440 MB/s)
python bench_convert.py --engines msgspec --workers 8 --compressions zstd --no-dictionary

# Benchmark a sample (compare engines / workers / compressions)
python bench_convert.py --engines python,msgspec,duckdb --workers 4,8,12 --compressions snappy,zstd --limit 30

# Parse only, no disk writes
python bench_convert.py --engines msgspec --workers 8 --no-write --limit 30

# Build gold layer (once, after silver is populated)
python build_gold.py

# Launch dashboard
streamlit run dashboard.py
```

Default `--data-dir`: `C:\Users\emman\Desktop\YNOV\M2\Outils ETL\data`. Override with `--data-dir <path>`.

`bench_convert.py` options: `--engines` (`python` = orjson+pyarrow, `msgspec` = typed Structs+pyarrow, `duckdb` = read_json+COPY), `--workers`, `--compressions` (snappy, zstd), `--duckdb-threads` (default 1), `--zstd-level` (default 1), `--limit` (0 = all), `--no-write`, `--no-dictionary`.

Benchmark output lands in `logs/`: `benchmark_results.csv` (one row per config — time, MB/s, playlists/s, tracks/s, compression ratio, 30 GB estimate) and `benchmark_files_detail.csv` (one row per file — parse/build/write breakdown).

## Architecture

Silver ETL (`bench_convert.py`) — three engines, one file per task via `ProcessPoolExecutor`:

- **`python`** — orjson + pyarrow, dict-based `flatten()`
- **`msgspec`** — typed `Struct(gc=False)` decoded directly into attribute-access objects via `flatten_ms()`; fastest
- **`duckdb`** — `read_json` + `COPY TO` parquet; ~5× slower (one large JSON object per file = mono-threaded parse per file)

`_warmup_worker` pool initializer pre-imports pyarrow/msgspec and calls `gc.disable()` before the first file hits each worker, avoiding ~0.8 s of import overhead and GC thrashing across millions of short-lived objects.

`MsTrack`, `MsPlaylist`, `MsSlice` are defined at module level — required for msgspec forward-ref resolution (`from __future__ import annotations`) and pickle compatibility under Windows `spawn`.

Schemas are explicit (defined in `get_schemas()`). `collaborative` is `bool` (source JSON has `"true"`/`"false"` strings). `description` is nullable.

Dashboard (`dashboard.py`) — three tabs: KPI header row → Top Tracks (horizontal bar, color intensity = unique playlists) → Top Artists → Playlist Stats (4 histograms). All tables loaded once via `@st.cache_data`.

## Optimization history (benchmarks on 30 files, ~970 MB)

| Step | Config | MB/s | Est. 30 GB |
|---|---|---|---|
| Baseline | orjson, 8 workers, zstd-1 | 259 | 119 s |
| + Defender exclusions | same | 292 | 105 s |
| + msgspec typed | msgspec, 8 workers, zstd-1 | 322 | 95 s |
| + warmup pool, gc.disable, no-dictionary | final config | ~440 | **74 s (measured, full 32 GB)** |

Rejected approaches:
- **DuckDB for silver**: ~5× slower (49 MB/s) — large single-object JSON, mono-threaded parse, lower compression ratio
- **snappy**: same speed as zstd-1 but ratio ×9.2 vs ×12.2 (with dictionary)
- **>8 workers**: plateau at 8 physical cores; 12–16 degrade
- **dictionary encoding**: ratio ×12.2 → ×9 (~+0.8 GB on 30 GB) but faster writes — trade-off accepted, speed wins
