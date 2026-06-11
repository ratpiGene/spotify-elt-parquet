"""
Benchmark JSON (Spotify MPD) -> Parquet.

Sorties par fichier source mpd.slice.X-Y.json :
  silver/mpd.slice.X-Y.playlist.parquet
  silver/mpd.slice.X-Y.tracks.parquet

Moteurs comparables : --engines python (orjson+pyarrow), msgspec (Structs typés+pyarrow), duckdb.

Usage :
  python bench_convert.py --limit 20                              # benchmark rapide sur 20 fichiers
  python bench_convert.py --engines python,msgspec --limit 30     # comparer les moteurs
  python bench_convert.py --engines msgspec --workers 8 --compressions zstd --no-dictionary   # run de production validé (32 Go en 74s)
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from pathlib import Path
from statistics import mean, median

# Parser JSON le plus rapide disponible
try:
    import orjson

    json_loads = orjson.loads
    JSON_PARSER = "orjson"
except ImportError:
    try:
        import msgspec.json

        json_loads = msgspec.json.decode
        JSON_PARSER = "msgspec"
    except ImportError:
        import json

        json_loads = json.loads
        JSON_PARSER = "json(stdlib)"

MB = 1024 * 1024


def get_schemas():
    import pyarrow as pa

    playlist_schema = pa.schema(
        [
            ("pid", pa.int64()),
            ("name", pa.string()),
            ("description", pa.string()),
            ("collaborative", pa.bool_()),
            ("modified_at", pa.int64()),
            ("num_tracks", pa.int32()),
            ("num_albums", pa.int32()),
            ("num_artists", pa.int32()),
            ("num_followers", pa.int32()),
            ("num_edits", pa.int32()),
            ("duration_ms", pa.int64()),
        ]
    )
    tracks_schema = pa.schema(
        [
            ("pid", pa.int64()),
            ("pos", pa.int32()),
            ("track_uri", pa.string()),
            ("track_name", pa.string()),
            ("artist_uri", pa.string()),
            ("artist_name", pa.string()),
            ("album_uri", pa.string()),
            ("album_name", pa.string()),
            ("duration_ms", pa.int64()),
        ]
    )
    return playlist_schema, tracks_schema


def flatten(playlists: list[dict]):
    """Aplatit playlists + tracks en colonnes (listes Python)."""
    pl_cols = {
        "pid": [p["pid"] for p in playlists],
        "name": [p["name"] for p in playlists],
        "description": [p.get("description") for p in playlists],
        "collaborative": [p["collaborative"] == "true" for p in playlists],
        "modified_at": [p["modified_at"] for p in playlists],
        "num_tracks": [p["num_tracks"] for p in playlists],
        "num_albums": [p["num_albums"] for p in playlists],
        "num_artists": [p["num_artists"] for p in playlists],
        "num_followers": [p["num_followers"] for p in playlists],
        "num_edits": [p["num_edits"] for p in playlists],
        "duration_ms": [p["duration_ms"] for p in playlists],
    }

    pids: list[int] = []
    all_tracks: list[dict] = []
    for p in playlists:
        trs = p["tracks"]
        pids.extend([p["pid"]] * len(trs))
        all_tracks.extend(trs)

    tr_cols = {
        "pid": pids,
        "pos": [t["pos"] for t in all_tracks],
        "track_uri": [t["track_uri"] for t in all_tracks],
        "track_name": [t["track_name"] for t in all_tracks],
        "artist_uri": [t["artist_uri"] for t in all_tracks],
        "artist_name": [t["artist_name"] for t in all_tracks],
        "album_uri": [t["album_uri"] for t in all_tracks],
        "album_name": [t["album_name"] for t in all_tracks],
        "duration_ms": [t["duration_ms"] for t in all_tracks],
    }
    return pl_cols, tr_cols


# --------------------------------------------------------------- msgspec ----

_MS_DECODER = None  # décodeur msgspec typé, par process worker

# Structs définis au niveau module : indispensable pour que msgspec puisse
# résoudre les forward refs (from __future__ import annotations) et pour
# le pickling multiprocessing sous Windows (spawn).
try:
    import msgspec as _msgspec

    class MsTrack(_msgspec.Struct, gc=False):
        pos: int
        artist_name: str
        track_uri: str
        artist_uri: str
        track_name: str
        album_uri: str
        duration_ms: int
        album_name: str

    class MsPlaylist(_msgspec.Struct, gc=False):
        name: str
        collaborative: str
        pid: int
        modified_at: int
        num_tracks: int
        num_albums: int
        num_followers: int
        num_edits: int
        duration_ms: int
        num_artists: int
        tracks: list[MsTrack]
        description: str | None = None

    class MsSlice(_msgspec.Struct):
        playlists: list[MsPlaylist]  # 'info' ignoré par msgspec

    HAS_MSGSPEC = True
except ImportError:
    HAS_MSGSPEC = False


def _get_ms_decoder():
    global _MS_DECODER
    if _MS_DECODER is None:
        if not HAS_MSGSPEC:
            raise RuntimeError("msgspec non installé : pip install msgspec")
        _MS_DECODER = _msgspec.json.Decoder(MsSlice)
    return _MS_DECODER


def flatten_ms(playlists) -> tuple[dict, dict]:
    """Comme flatten(), mais sur des Structs msgspec (accès attributs, pas de dicts)."""
    pl_cols = {
        "pid": [p.pid for p in playlists],
        "name": [p.name for p in playlists],
        "description": [p.description for p in playlists],
        "collaborative": [p.collaborative == "true" for p in playlists],
        "modified_at": [p.modified_at for p in playlists],
        "num_tracks": [p.num_tracks for p in playlists],
        "num_albums": [p.num_albums for p in playlists],
        "num_artists": [p.num_artists for p in playlists],
        "num_followers": [p.num_followers for p in playlists],
        "num_edits": [p.num_edits for p in playlists],
        "duration_ms": [p.duration_ms for p in playlists],
    }

    pids: list[int] = []
    all_tracks: list = []
    for p in playlists:
        trs = p.tracks
        pids.extend([p.pid] * len(trs))
        all_tracks.extend(trs)

    tr_cols = {
        "pid": pids,
        "pos": [t.pos for t in all_tracks],
        "track_uri": [t.track_uri for t in all_tracks],
        "track_name": [t.track_name for t in all_tracks],
        "artist_uri": [t.artist_uri for t in all_tracks],
        "artist_name": [t.artist_name for t in all_tracks],
        "album_uri": [t.album_uri for t in all_tracks],
        "album_name": [t.album_name for t in all_tracks],
        "duration_ms": [t.duration_ms for t in all_tracks],
    }
    return pl_cols, tr_cols


def process_file_msgspec(path_str: str, out_dir: str, compression: str, zstd_level: int,
                         no_write: bool, use_dict: bool = True) -> dict:
    """Traite UN fichier JSON -> 2 Parquet via msgspec (décodage typé) + pyarrow."""
    t0 = time.perf_counter()
    path = Path(path_str)

    raw = path.read_bytes()
    json_bytes = len(raw)
    data = _get_ms_decoder().decode(raw)
    del raw
    t_parse = time.perf_counter()

    playlists = data.playlists
    pl_cols, tr_cols = flatten_ms(playlists)
    n_playlists = len(pl_cols["pid"])
    n_tracks = len(tr_cols["pid"])
    del data, playlists
    t_build = time.perf_counter()

    pq_bytes = 0
    if not no_write:
        import pyarrow as pa
        import pyarrow.parquet as pq

        playlist_schema, tracks_schema = get_schemas()
        kw = {"compression": compression, "use_dictionary": use_dict}
        if compression == "zstd":
            kw["compression_level"] = zstd_level

        stem = path.name[: -len(".json")]
        out = Path(out_dir)

        pl_path = out / f"{stem}.playlist.parquet"
        pq.write_table(pa.table(pl_cols, schema=playlist_schema), pl_path, **kw)

        tr_path = out / f"{stem}.tracks.parquet"
        pq.write_table(pa.table(tr_cols, schema=tracks_schema), tr_path, **kw)

        pq_bytes = pl_path.stat().st_size + tr_path.stat().st_size
    t_write = time.perf_counter()

    return {
        "file": path.name,
        "json_bytes": json_bytes,
        "parquet_bytes": pq_bytes,
        "n_playlists": n_playlists,
        "n_tracks": n_tracks,
        "parse_s": round(t_parse - t0, 4),
        "build_s": round(t_build - t_parse, 4),
        "write_s": round(t_write - t_build, 4),
        "total_s": round(t_write - t0, 4),
    }


# ---------------------------------------------------------------- DuckDB ----

_DUCK = None  # connexion DuckDB par process worker

DUCK_PLAYLIST_STRUCT = (
    "STRUCT(name VARCHAR, collaborative VARCHAR, pid BIGINT, modified_at BIGINT, "
    "num_tracks INTEGER, num_albums INTEGER, num_followers INTEGER, num_edits INTEGER, "
    "duration_ms BIGINT, num_artists INTEGER, description VARCHAR, "
    "tracks STRUCT(pos INTEGER, artist_name VARCHAR, track_uri VARCHAR, artist_uri VARCHAR, "
    "track_name VARCHAR, album_uri VARCHAR, duration_ms BIGINT, album_name VARCHAR)[])[]"
)


def _get_duck(threads: int):
    global _DUCK
    if _DUCK is None:
        import duckdb

        _DUCK = duckdb.connect()
        _DUCK.execute(f"SET threads={threads}")
    return _DUCK


def process_file_duckdb(path_str: str, out_dir: str, compression: str, zstd_level: int,
                        no_write: bool, duckdb_threads: int) -> dict:
    """Traite UN fichier JSON -> 2 Parquet via DuckDB (read_json + COPY)."""
    t0 = time.perf_counter()
    path = Path(path_str)
    json_bytes = path.stat().st_size
    conn = _get_duck(duckdb_threads)

    src = path.as_posix().replace("'", "''")
    read_json = (
        f"read_json('{src}', columns := {{'playlists': '{DUCK_PLAYLIST_STRUCT}'}}, "
        f"maximum_object_size := 268435456)"
    )
    codec = f"COMPRESSION 'zstd', COMPRESSION_LEVEL {zstd_level}" if compression == "zstd" \
        else f"COMPRESSION '{compression}'"

    stem = path.name[: -len(".json")]
    out = Path(out_dir)
    pl_path = out / f"{stem}.playlist.parquet"
    tr_path = out / f"{stem}.tracks.parquet"

    pl_query = f"""
        SELECT p.pid, p.name, p.description,
               p.collaborative = 'true' AS collaborative,
               p.modified_at, p.num_tracks, p.num_albums, p.num_artists,
               p.num_followers, p.num_edits, p.duration_ms
        FROM (SELECT unnest(playlists) AS p FROM {read_json})
    """
    tr_query = f"""
        SELECT pid, pos, track_uri, track_name, artist_uri, artist_name,
               album_uri, album_name, duration_ms
        FROM (
            SELECT p.pid AS pid, unnest(p.tracks, recursive := true)
            FROM (SELECT unnest(playlists) AS p FROM {read_json})
        )
    """

    if no_write:
        n_playlists = conn.execute(f"SELECT count(*) FROM ({pl_query})").fetchone()[0]
        t_pl = time.perf_counter()
        n_tracks = conn.execute(f"SELECT count(*) FROM ({tr_query})").fetchone()[0]
        t_tr = time.perf_counter()
        pq_bytes = 0
    else:
        n_playlists = conn.execute(
            f"COPY ({pl_query}) TO '{pl_path.as_posix()}' (FORMAT PARQUET, {codec})"
        ).fetchone()[0]
        t_pl = time.perf_counter()
        n_tracks = conn.execute(
            f"COPY ({tr_query}) TO '{tr_path.as_posix()}' (FORMAT PARQUET, {codec})"
        ).fetchone()[0]
        t_tr = time.perf_counter()
        pq_bytes = pl_path.stat().st_size + tr_path.stat().st_size

    return {
        "file": path.name,
        "json_bytes": json_bytes,
        "parquet_bytes": pq_bytes,
        "n_playlists": n_playlists,
        "n_tracks": n_tracks,
        "parse_s": 0.0,  # non séparable avec DuckDB (parse+write fusionnés)
        "build_s": round(t_pl - t0, 4),   # COPY playlists
        "write_s": round(t_tr - t_pl, 4),  # COPY tracks
        "total_s": round(t_tr - t0, 4),
    }


# ------------------------------------------------------- Python (orjson) ----


def process_file(path_str: str, out_dir: str, compression: str, zstd_level: int,
                 no_write: bool, use_dict: bool = True) -> dict:
    """Traite UN fichier JSON -> 2 Parquet via orjson + pyarrow. Exécuté dans un process worker."""
    t0 = time.perf_counter()
    path = Path(path_str)

    raw = path.read_bytes()
    json_bytes = len(raw)
    data = json_loads(raw)
    del raw
    t_parse = time.perf_counter()

    playlists = data["playlists"]
    pl_cols, tr_cols = flatten(playlists)
    n_playlists = len(pl_cols["pid"])
    n_tracks = len(tr_cols["pid"])
    del data, playlists
    t_build = time.perf_counter()

    pq_bytes = 0
    if not no_write:
        import pyarrow as pa
        import pyarrow.parquet as pq

        playlist_schema, tracks_schema = get_schemas()
        kw = {"compression": compression, "use_dictionary": use_dict}
        if compression == "zstd":
            kw["compression_level"] = zstd_level

        stem = path.name[: -len(".json")]  # mpd.slice.X-Y
        out = Path(out_dir)

        pl_table = pa.table(pl_cols, schema=playlist_schema)
        pl_path = out / f"{stem}.playlist.parquet"
        pq.write_table(pl_table, pl_path, **kw)
        del pl_table

        tr_table = pa.table(tr_cols, schema=tracks_schema)
        tr_path = out / f"{stem}.tracks.parquet"
        pq.write_table(tr_table, tr_path, **kw)
        del tr_table

        pq_bytes = pl_path.stat().st_size + tr_path.stat().st_size
    t_write = time.perf_counter()

    return {
        "file": path.name,
        "json_bytes": json_bytes,
        "parquet_bytes": pq_bytes,
        "n_playlists": n_playlists,
        "n_tracks": n_tracks,
        "parse_s": round(t_parse - t0, 4),
        "build_s": round(t_build - t_parse, 4),
        "write_s": round(t_write - t_build, 4),
        "total_s": round(t_write - t0, 4),
    }


def _warmup_worker(engine: str, no_write: bool, duckdb_threads: int):
    """Initializer du pool : paie les imports/initialisations AVANT le 1er fichier,
    pour ne pas fausser les mesures (sinon ~0.8s d'import pyarrow sur le 1er write).
    Désactive aussi le GC : millions d'objets sans cycles, il ne fait que ralentir."""
    import gc

    gc.disable()
    if engine == "duckdb":
        _get_duck(duckdb_threads)
        return
    if engine == "msgspec":
        _get_ms_decoder()
    if not no_write:
        import pyarrow.parquet  # noqa: F401


def run_benchmark(files: list[Path], out_dir: Path, workers: int, compression: str,
                  zstd_level: int, no_write: bool, engine: str, duckdb_threads: int,
                  use_dict: bool = True):
    if engine == "duckdb":
        worker_fn = partial(
            process_file_duckdb,
            out_dir=str(out_dir),
            compression=compression,
            zstd_level=zstd_level,
            no_write=no_write,
            duckdb_threads=duckdb_threads,
        )
    elif engine == "msgspec":
        worker_fn = partial(
            process_file_msgspec,
            out_dir=str(out_dir),
            compression=compression,
            zstd_level=zstd_level,
            no_write=no_write,
            use_dict=use_dict,
        )
    else:
        worker_fn = partial(
            process_file,
            out_dir=str(out_dir),
            compression=compression,
            zstd_level=zstd_level,
            no_write=no_write,
            use_dict=use_dict,
        )
    paths = [str(f) for f in files]

    if workers == 1:
        _warmup_worker(engine, no_write, duckdb_threads)
        t0 = time.perf_counter()
        results = [worker_fn(p) for p in paths]
        wall = time.perf_counter() - t0
    else:
        with ProcessPoolExecutor(
            max_workers=workers,
            initializer=_warmup_worker,
            initargs=(engine, no_write, duckdb_threads),
        ) as ex:
            t0 = time.perf_counter()
            results = list(ex.map(worker_fn, paths, chunksize=2))
            wall = time.perf_counter() - t0

    json_mb = sum(r["json_bytes"] for r in results) / MB
    pq_mb = sum(r["parquet_bytes"] for r in results) / MB
    n_pl = sum(r["n_playlists"] for r in results)
    n_tr = sum(r["n_tracks"] for r in results)
    times = [r["total_s"] for r in results]

    summary = {
        "engine": engine,
        "workers": workers,
        "compression": f"zstd-{zstd_level}" if compression == "zstd" else compression,
        "json_parser": engine if engine in ("duckdb", "msgspec") else JSON_PARSER,
        "use_dictionary": use_dict,
        "n_files": len(results),
        "total_time_s": round(wall, 3),
        "json_mb": round(json_mb, 1),
        "parquet_mb": round(pq_mb, 1),
        "compression_ratio": round(json_mb / pq_mb, 2) if pq_mb else 0,
        "mb_per_s": round(json_mb / wall, 1),
        "playlists_per_s": round(n_pl / wall, 0),
        "tracks_per_s": round(n_tr / wall, 0),
        "file_time_mean_s": round(mean(times), 3),
        "file_time_median_s": round(median(times), 3),
        "file_time_max_s": round(max(times), 3),
        "est_30gb_s": round(30 * 1024 / (json_mb / wall), 1),
    }
    return summary, results


def main():
    ap = argparse.ArgumentParser(description="Benchmark JSON MPD -> Parquet")
    ap.add_argument("--data-dir", default=r"C:\Users\emman\Desktop\YNOV\M2\Outils ETL\data")
    ap.add_argument("--out-dir", default="silver")
    ap.add_argument("--logs-dir", default="logs")
    ap.add_argument("--workers", default="1,2,4,8", help="liste, ex: 1,4,8,12")
    ap.add_argument("--compressions", default="snappy,zstd", help="snappy,zstd")
    ap.add_argument("--engines", default="python", help="python,msgspec,duckdb")
    ap.add_argument("--duckdb-threads", type=int, default=1,
                    help="threads par connexion DuckDB (1 conseillé avec multiprocessing)")
    ap.add_argument("--zstd-level", type=int, default=1)
    ap.add_argument("--limit", type=int, default=0, help="nb max de fichiers (0 = tous)")
    ap.add_argument("--pattern", default="mpd.slice.*.json")
    ap.add_argument("--no-write", action="store_true", help="parse seulement, pas d'écriture Parquet")
    ap.add_argument("--no-dictionary", action="store_true",
                    help="désactive l'encodage dictionnaire Parquet (écriture plus rapide, fichiers un peu plus gros)")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    out_dir = Path(args.out_dir)
    logs_dir = Path(args.logs_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(data_dir.glob(args.pattern))
    if not files:
        sys.exit(f"Aucun fichier {args.pattern} dans {data_dir}")
    if args.limit:
        files = files[: args.limit]

    workers_list = [int(w) for w in args.workers.split(",")]
    comp_list = [c.strip() for c in args.compressions.split(",")]
    engine_list = [e.strip() for e in args.engines.split(",")]
    cpu = os.cpu_count()
    total_mb = sum(f.stat().st_size for f in files) / MB

    print(f"parser={JSON_PARSER} | cpu={cpu} | fichiers={len(files)} ({total_mb:.0f} Mo)")
    print(f"configs: engines={engine_list} x workers={workers_list} x compressions={comp_list}\n")

    summaries, all_details = [], []
    for engine in engine_list:
        for comp in comp_list:
            for w in workers_list:
                label = f"{engine:<7} workers={w:<3} comp={comp}"
                print(f"[{label}] ...", end="", flush=True)
                summary, details = run_benchmark(
                    files, out_dir, w, comp, args.zstd_level, args.no_write,
                    engine, args.duckdb_threads, use_dict=not args.no_dictionary,
                )
                summaries.append(summary)
                for d in details:
                    all_details.append(
                        {"engine": engine, "workers": w, "compression": summary["compression"], **d}
                    )
                print(
                    f"\r[{label}] {summary['total_time_s']:>7.1f}s | "
                    f"{summary['mb_per_s']:>7.1f} Mo/s | "
                    f"{summary['tracks_per_s']:>9.0f} tracks/s | "
                    f"ratio x{summary['compression_ratio']} | "
                    f"est. 30Go: {summary['est_30gb_s']:.0f}s"
                )

    results_csv = logs_dir / "benchmark_results.csv"
    with open(results_csv, "w", newline="", encoding="utf-8") as f:
        wtr = csv.DictWriter(f, fieldnames=list(summaries[0].keys()))
        wtr.writeheader()
        wtr.writerows(summaries)

    detail_csv = logs_dir / "benchmark_files_detail.csv"
    with open(detail_csv, "w", newline="", encoding="utf-8") as f:
        wtr = csv.DictWriter(f, fieldnames=list(all_details[0].keys()))
        wtr.writeheader()
        wtr.writerows(all_details)

    best = max(summaries, key=lambda s: s["mb_per_s"])
    print(f"\nMeilleure config: engine={best['engine']} workers={best['workers']} {best['compression']} "
          f"-> {best['mb_per_s']} Mo/s, 30 Go estimés en {best['est_30gb_s']}s")
    print(f"Résultats: {results_csv} + {detail_csv}")


if __name__ == "__main__":
    main()
