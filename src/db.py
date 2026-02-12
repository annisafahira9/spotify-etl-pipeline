from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from dotenv import load_dotenv

DDL = """
PRAGMA foreign_keys = ON;

-- Tracks pipeline watermarks + last run metadata
CREATE TABLE IF NOT EXISTS etl_state (
  pipeline_name TEXT NOT NULL,
  state_key     TEXT NOT NULL,
  state_value   TEXT,
  updated_at    TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (pipeline_name, state_key)
);

-- Dimensions
CREATE TABLE IF NOT EXISTS dim_playlist (
  playlist_id   TEXT PRIMARY KEY,
  name          TEXT,
  owner_id      TEXT,
  is_public     INTEGER,
  is_collab     INTEGER,
  snapshot_id   TEXT,
  tracks_total  INTEGER,
  updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS dim_album (
  album_id      TEXT PRIMARY KEY,
  name          TEXT,
  release_date  TEXT,
  release_precision TEXT,
  total_tracks  INTEGER,
  album_type    TEXT,
  updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS dim_artist (
  artist_id     TEXT PRIMARY KEY,
  name          TEXT,
  popularity    INTEGER,
  followers     INTEGER,
  updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS dim_track (
  track_id      TEXT PRIMARY KEY,
  name          TEXT,
  album_id      TEXT,
  duration_ms   INTEGER,
  explicit      INTEGER,
  popularity    INTEGER,
  track_number  INTEGER,
  disc_number   INTEGER,
  is_local      INTEGER,
  is_playable   INTEGER,
  updated_at    TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (album_id) REFERENCES dim_album(album_id)
);

CREATE TABLE IF NOT EXISTS dim_audio_features (
  track_id         TEXT PRIMARY KEY,
  danceability     REAL,
  energy           REAL,
  key              INTEGER,
  loudness         REAL,
  mode             INTEGER,
  speechiness      REAL,
  acousticness     REAL,
  instrumentalness REAL,
  liveness         REAL,
  valence          REAL,
  tempo            REAL,
  time_signature   INTEGER,
  updated_at       TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (track_id) REFERENCES dim_track(track_id)
);

-- Many-to-many between tracks and artists
CREATE TABLE IF NOT EXISTS bridge_track_artist (
  track_id      TEXT NOT NULL,
  artist_id     TEXT NOT NULL,
  artist_order  INTEGER NOT NULL,
  PRIMARY KEY (track_id, artist_id),
  FOREIGN KEY (track_id) REFERENCES dim_track(track_id),
  FOREIGN KEY (artist_id) REFERENCES dim_artist(artist_id)
);

-- Date dimension for added_at (playlist snapshot events)
CREATE TABLE IF NOT EXISTS dim_date (
  date_id     INTEGER PRIMARY KEY,   -- YYYYMMDD
  date        TEXT NOT NULL UNIQUE,  -- YYYY-MM-DD
  year        INTEGER NOT NULL,
  month       INTEGER NOT NULL,
  day         INTEGER NOT NULL,
  day_of_week INTEGER NOT NULL
);

-- Fact grain: 1 row = 1 track at 1 position in 1 playlist snapshot
CREATE TABLE IF NOT EXISTS fact_playlist_track (
  playlist_id   TEXT NOT NULL,
  snapshot_id   TEXT NOT NULL,
  position      INTEGER NOT NULL,
  track_id      TEXT,
  added_at      TEXT,
  added_by_id   TEXT,
  is_local      INTEGER,
  extracted_at  TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (playlist_id, snapshot_id, position),
  FOREIGN KEY (playlist_id) REFERENCES dim_playlist(playlist_id),
  FOREIGN KEY (track_id) REFERENCES dim_track(track_id)
);

-- Helpful indexes (optional but good)
CREATE INDEX IF NOT EXISTS idx_fact_track_id ON fact_playlist_track(track_id);
CREATE INDEX IF NOT EXISTS idx_fact_playlist ON fact_playlist_track(playlist_id);
"""

def connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def migrate(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()

def get_db_path() -> str:
    load_dotenv()
    return os.environ.get("SQLITE_PATH", "./data/spotify.db")
