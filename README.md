# spotify-etl-parquet — Conversion JSON → Parquet (Spotify MPD)

Convertit les `mpd.slice.X-Y.json` (Million Playlist Dataset, 1000 fichiers, ~32 Go) en deux
Parquet par fichier source :

- `silver/mpd.slice.X-Y.playlist.parquet` (1 ligne par playlist, sans tracks)
- `silver/mpd.slice.X-Y.tracks.parquet` (1 ligne par track, avec `pid` pour la jointure)

## Configuration validée (production)

```
python bench_convert.py --engines msgspec --workers 8 --compressions zstd --no-dictionary
```

**Résultat mesuré : 32 Go convertis en 74,4 s (~440 Mo/s), ratio de compression ×9**
(machine 16 CPU logiques / 8 cœurs physiques, Windows, Python 3.13).
Contrainte « 1 batch de 30 Go par heure » tenue avec un facteur ~48 de marge.

## Installation

```
pip install -r requirements.txt
```

Prérequis perf sous Windows — exclusions Defender (PowerShell admin), sinon chaque
`.parquet` créé est scanné :

```powershell
Add-MpPreference -ExclusionPath "...\spotify-etl-parquet\silver"
Add-MpPreference -ExclusionPath "...\data"
```

## Usage benchmark

```
# Comparer moteurs / workers / compressions sur un échantillon
python bench_convert.py --engines python,msgspec,duckdb --workers 4,8,12 --compressions snappy,zstd --limit 30
```

Options : `--data-dir`, `--out-dir` (défaut `silver`), `--logs-dir` (défaut `logs`),
`--engines` (`python` = orjson+pyarrow, `msgspec` = Structs typés+pyarrow, `duckdb` = read_json+COPY),
`--duckdb-threads` (défaut 1, par worker), `--zstd-level` (défaut 1), `--limit` (0 = tous),
`--no-write` (parse seul), `--no-dictionary` (désactive l'encodage dictionnaire Parquet).

Sorties de mesure :

- `logs/benchmark_results.csv` : 1 ligne par config — temps total, Mo/s, playlists/s, tracks/s,
  tailles JSON/Parquet, ratio de compression, estimation 30 Go.
- `logs/benchmark_files_detail.csv` : 1 ligne par fichier — parse_s, build_s, write_s, total_s.

## Conception

- 1 fichier JSON = 1 tâche indépendante (`ProcessPoolExecutor`) → jamais plus de `workers`
  fichiers en mémoire simultanément (~33 Mo de JSON chacun).
- Moteur retenu `msgspec` : décodage JSON typé vers des `Struct(gc=False)` — pas de dicts
  Python créés, accès attributs en C, champ `info` ignoré au parsing.
- Écriture `pyarrow.parquet.write_table`, schémas explicites, zstd niveau 1.
- `initializer` du pool : pré-import de pyarrow + construction du décodeur (sinon le 1er fichier
  de chaque worker paie ~0,8 s d'import) et `gc.disable()` (millions d'objets sans cycles).
- `collaborative` converti en booléen ; `description` nullable ; `pid` propagé dans tracks.

## Historique d'optimisation (benchmarks sur 30 fichiers, ~970 Mo)

| Étape | Config | Mo/s | Est. 30 Go |
|---|---|---|---|
| Baseline | orjson, 8 workers, zstd-1 | 259 | 119 s |
| + exclusions Defender | idem | 292 | 105 s |
| + msgspec typé | msgspec, 8 workers, zstd-1 | 322 | 95 s |
| + warmup pool, gc.disable, no-dictionary | config finale | ~440 | **74 s (mesuré, run complet 32 Go)** |

Pistes écartées :

- **DuckDB** (`read_json` + `COPY`) : ~5× plus lent (49 Mo/s) — 1 gros objet JSON par fichier
  ⇒ parsing mono-thread par fichier, et ratio de compression inférieur.
- **snappy** : même vitesse que zstd-1 mais ratio ×9,2 vs ×12,2 (avec dictionnaire).
- **>8 workers** : plateau mesuré à 8 (8 cœurs physiques), 12-16 dégradent.
- **no-dictionary** : ratio ×12,2 → ×9 (~+0,8 Go sur 30 Go) mais écriture plus rapide —
  trade-off accepté, la vitesse prime.
