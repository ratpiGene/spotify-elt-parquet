"""Build gold layer: aggregates silver parquet files into gold/ for dashboard use."""
import argparse
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def build(silver: Path, gold: Path) -> None:
    gold.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    tr = (silver / "*.tracks.parquet").as_posix()
    pl = (silver / "*.playlist.parquet").as_posix()

    jobs = {
        "top_tracks": f"""
            SELECT track_uri, track_name, artist_uri, artist_name,
                   album_uri, album_name,
                   COUNT(*)            AS appearance_count,
                   COUNT(DISTINCT pid) AS unique_playlists
            FROM read_parquet('{tr}')
            GROUP BY ALL
            ORDER BY appearance_count DESC
            LIMIT 50000
        """,
        "top_artists": f"""
            SELECT artist_uri, artist_name,
                   COUNT(*)                  AS total_appearances,
                   COUNT(DISTINCT pid)       AS unique_playlists,
                   COUNT(DISTINCT track_uri) AS unique_tracks
            FROM read_parquet('{tr}')
            GROUP BY artist_uri, artist_name
            ORDER BY total_appearances DESC
            LIMIT 20000
        """,
        "playlists": f"SELECT * FROM read_parquet('{pl}')",
    }

    for name, query in jobs.items():
        t0 = time.perf_counter()
        out = (gold / f"{name}.parquet").as_posix()
        con.execute(f"COPY ({query}) TO '{out}' (FORMAT PARQUET, COMPRESSION 'zstd')")
        print(f"  {name}.parquet  {time.perf_counter() - t0:.1f}s")

    t0 = time.perf_counter()
    pl_row = con.execute(f"""
        SELECT COUNT(*)          AS total_playlists,
               SUM(num_followers) AS total_followers,
               SUM(duration_ms)   AS total_duration_ms
        FROM read_parquet('{pl}')
    """).fetchone()

    tr_row = con.execute(f"""
        SELECT COUNT(*)                   AS total_track_entries,
               COUNT(DISTINCT track_uri)  AS unique_tracks,
               COUNT(DISTINCT artist_uri) AS unique_artists,
               COUNT(DISTINCT album_uri)  AS unique_albums
        FROM read_parquet('{tr}')
    """).fetchone()

    pq.write_table(
        pa.table({
            "total_playlists":    [pl_row[0]],
            "total_followers":    [pl_row[1]],
            "total_duration_ms":  [pl_row[2]],
            "total_track_entries":[tr_row[0]],
            "unique_tracks":      [tr_row[1]],
            "unique_artists":     [tr_row[2]],
            "unique_albums":      [tr_row[3]],
        }),
        gold / "kpis.parquet",
        compression="zstd",
    )
    print(f"  kpis.parquet  {time.perf_counter() - t0:.1f}s")


def main():
    ap = argparse.ArgumentParser(description="Build gold layer from silver parquet files")
    ap.add_argument("--silver-dir", default="silver")
    ap.add_argument("--gold-dir", default="gold")
    args = ap.parse_args()

    silver, gold = Path(args.silver_dir), Path(args.gold_dir)
    files = list(silver.glob("*.parquet"))
    if not files:
        import sys
        sys.exit(f"No parquet files found in {silver}")

    print(f"silver={silver} ({len(files)} files) → gold={gold}")
    t0 = time.perf_counter()
    build(silver, gold)
    print(f"Total  {time.perf_counter() - t0:.1f}s")


if __name__ == "__main__":
    main()
