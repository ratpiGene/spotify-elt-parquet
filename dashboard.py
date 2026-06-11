"""Spotify MPD — Gold layer dashboard."""
from pathlib import Path

import pandas as pd
import plotly.express as px
import pyarrow.parquet as pq
import streamlit as st

GOLD = Path("gold")

st.set_page_config(page_title="Spotify MPD Dashboard", layout="wide")

if not GOLD.exists() or not any(GOLD.glob("*.parquet")):
    st.error("Gold layer not found. Run `python build_gold.py` first.")
    st.stop()


@st.cache_data
def load_kpis() -> dict:
    return {k: v[0] for k, v in pq.read_table(GOLD / "kpis.parquet").to_pydict().items()}


@st.cache_data
def load_top_tracks() -> pd.DataFrame:
    return pq.read_table(GOLD / "top_tracks.parquet").to_pandas()


@st.cache_data
def load_top_artists() -> pd.DataFrame:
    return pq.read_table(GOLD / "top_artists.parquet").to_pandas()


@st.cache_data
def load_playlists() -> pd.DataFrame:
    return pq.read_table(
        GOLD / "playlists.parquet",
        columns=["num_tracks", "num_followers", "num_artists", "duration_ms"],
    ).to_pandas()


st.title("Spotify Million Playlist Dataset")

kpis = load_kpis()
cols = st.columns(4)
cols[0].metric("Playlists", f"{kpis['total_playlists']:,}")
cols[1].metric("Unique Tracks", f"{kpis['unique_tracks']:,}")
cols[2].metric("Unique Artists", f"{kpis['unique_artists']:,}")
cols[3].metric("Unique Albums", f"{kpis['unique_albums']:,}")

cols2 = st.columns(2)
cols2[0].metric("Total Listening Time", f"{kpis['total_duration_ms'] / 3_600_000:,.0f} h")
cols2[1].metric("Total Track Entries", f"{kpis['total_track_entries']:,}")

st.divider()

tab_tracks, tab_artists, tab_playlists = st.tabs(["Top Tracks", "Top Artists", "Playlist Stats"])

with tab_tracks:
    n = st.slider("Top N", 10, 200, 50, key="n_tracks")
    df = load_top_tracks().head(n).copy()
    df["label"] = df["track_name"] + " — " + df["artist_name"]
    fig = px.bar(
        df.sort_values("appearance_count"),
        x="appearance_count",
        y="label",
        orientation="h",
        labels={"appearance_count": "Playlist appearances", "label": ""},
        height=max(450, n * 16),
        color="unique_playlists",
        color_continuous_scale="Teal",
    )
    fig.update_layout(yaxis_tickfont_size=10, margin={"l": 0})
    st.plotly_chart(fig, use_container_width=True)

with tab_artists:
    n2 = st.slider("Top N", 10, 200, 30, key="n_artists")
    df2 = load_top_artists().head(n2).copy()
    fig2 = px.bar(
        df2.sort_values("total_appearances"),
        x="total_appearances",
        y="artist_name",
        orientation="h",
        labels={"total_appearances": "Track appearances", "artist_name": ""},
        height=max(450, n2 * 20),
        color="unique_playlists",
        color_continuous_scale="Viridis",
    )
    fig2.update_layout(yaxis_tickfont_size=11, margin={"l": 0})
    st.plotly_chart(fig2, use_container_width=True)

with tab_playlists:
    pldf = load_playlists()
    col1, col2 = st.columns(2)

    with col1:
        st.plotly_chart(
            px.histogram(pldf, x="num_tracks", nbins=60,
                         title="Tracks per playlist",
                         labels={"num_tracks": "Tracks"}),
            use_container_width=True,
        )
        st.plotly_chart(
            px.histogram(pldf, x="num_artists", nbins=60,
                         title="Artists per playlist",
                         labels={"num_artists": "Artists"}),
            use_container_width=True,
        )

    with col2:
        cap = pldf["num_followers"].quantile(0.99)
        st.plotly_chart(
            px.histogram(pldf[pldf["num_followers"] <= cap], x="num_followers",
                         nbins=60, title="Followers per playlist (99th pct cap)",
                         labels={"num_followers": "Followers"}),
            use_container_width=True,
        )
        pldf["duration_h"] = pldf["duration_ms"] / 3_600_000
        st.plotly_chart(
            px.histogram(pldf, x="duration_h", nbins=60,
                         title="Playlist duration",
                         labels={"duration_h": "Hours"}),
            use_container_width=True,
        )
