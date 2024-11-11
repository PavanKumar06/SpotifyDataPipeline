CREATE OR REPLACE DATABASE SpotifyPlaylist_DW;

USE SpotifyPlaylist_DW;

CREATE OR REPLACE SCHEMA SpotifyPlaylist_DW_SCHEMA;

USE SCHEMA SpotifyPlaylist_DW_SCHEMA;

CREATE OR REPLACE TABLE SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.album (
    album_id STRING PRIMARY KEY,
    name STRING,
    release_date DATE,
    total_tracks INT,
    url STRING
);

CREATE OR REPLACE TABLE SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.artist (
    artist_id STRING PRIMARY KEY,
    artist_name STRING,
    external_url STRING
);

CREATE TABLE SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.song (
    song_id STRING PRIMARY KEY,
    song_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added DATE,
    album_id STRING,  -- Foreign key referencing album table
    artist_id STRING  -- Foreign key referencing artist table
);