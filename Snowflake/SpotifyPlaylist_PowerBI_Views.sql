CREATE SCHEMA IF NOT EXISTS SpotifyPlaylist_DW.Views;

CREATE OR REPLACE VIEW SpotifyPlaylist_DW.Views.top_5_artists_view AS
WITH DistinctSongs AS (
    SELECT DISTINCT song_id, popularity, artist_id
    FROM SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.song
)
SELECT 
    artist.artist_name, 
    COUNT(song.song_id) AS song_count
FROM 
    DistinctSongs AS song
JOIN 
    SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.artist AS artist
    ON song.artist_id = artist.artist_id
WHERE 
    song.song_id IN (
        SELECT song_id
        FROM DistinctSongs
        ORDER BY popularity DESC
        LIMIT 50
    )
GROUP BY 
    artist.artist_name
ORDER BY 
    song_count DESC
LIMIT 5;

SELECT * FROM SpotifyPlaylist_DW.Views.top_5_artists_view;


CREATE OR REPLACE VIEW SpotifyPlaylist_DW.Views.bottom_5_artists_view AS
WITH DistinctSongs AS (
    SELECT DISTINCT song_id, popularity, artist_id
    FROM SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.song
)
SELECT 
    artist.artist_name, 
    COUNT(song.song_id) AS song_count
FROM 
    DistinctSongs AS song
JOIN 
    SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.artist AS artist
    ON song.artist_id = artist.artist_id
WHERE 
    song.song_id IN (
        SELECT song_id
        FROM DistinctSongs
        ORDER BY popularity DESC
        LIMIT 50
    )
GROUP BY 
    artist.artist_name
ORDER BY 
    song_count ASC
LIMIT 5;

SELECT * FROM SpotifyPlaylist_DW.Views.bottom_5_artists_view;


CREATE OR REPLACE VIEW SpotifyPlaylist_DW.Views.album_performance_comparison AS
SELECT 
    DISTINCT album.album_id, 
    album.name AS album_name, 
    SUM(DISTINCT song.popularity) AS total_popularity
FROM 
    SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.album AS album
JOIN 
    SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.song AS song
    ON album.album_id = song.album_id
GROUP BY 
    album.album_id, 
    album.name
ORDER BY 
    total_popularity DESC;

SELECT * FROM SpotifyPlaylist_DW.Views.album_performance_comparison;


CREATE OR REPLACE VIEW SpotifyPlaylist_DW.Views.album_release_periods AS
SELECT 
    DATE_TRUNC('MONTH', album.release_date) AS release_month,
    SUM(DISTINCT song.popularity) AS total_popularity
FROM 
    SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.album AS album
JOIN 
    SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.song AS song
    ON album.album_id = song.album_id
GROUP BY 
    DATE_TRUNC('MONTH', album.release_date)
ORDER BY 
    release_month;

SELECT * FROM SpotifyPlaylist_DW.Views.album_release_periods;


CREATE OR REPLACE VIEW SpotifyPlaylist_DW.Views.listener_engagement_heatmap AS
SELECT 
    CASE 
        WHEN DATE_PART('DOW', song_added) = 0 THEN 'Sunday'
        WHEN DATE_PART('DOW', song_added) = 1 THEN 'Monday'
        WHEN DATE_PART('DOW', song_added) = 2 THEN 'Tuesday'
        WHEN DATE_PART('DOW', song_added) = 3 THEN 'Wednesday'
        WHEN DATE_PART('DOW', song_added) = 4 THEN 'Thursday'
        WHEN DATE_PART('DOW', song_added) = 5 THEN 'Friday'
        WHEN DATE_PART('DOW', song_added) = 6 THEN 'Saturday'
    END AS day_of_week,
    SUM(popularity) AS total_popularity
FROM 
    SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.song
GROUP BY 
    DATE_PART('DOW', song_added)
ORDER BY 
    DATE_PART('DOW', song_added);

SELECT * FROM SpotifyPlaylist_DW.Views.listener_engagement_heatmap;


CREATE OR REPLACE VIEW SpotifyPlaylist_DW.Views.artist_contributions AS
SELECT 
    artist.artist_name,
    COUNT(DISTINCT song.song_id) AS song_count
FROM 
    SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.song AS song
JOIN 
    SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.artist AS artist
    ON song.artist_id = artist.artist_id
GROUP BY 
    artist.artist_name
ORDER BY 
    song_count DESC;

SELECT * FROM SpotifyPlaylist_DW.Views.artist_contributions;


CREATE OR REPLACE VIEW SpotifyPlaylist_DW.Views.song_duration_analysis AS
SELECT 
    FLOOR(duration_ms / 60000) AS duration_minutes,
    COUNT(song_id) AS song_count
FROM 
    SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.song
GROUP BY 
    duration_minutes
ORDER BY 
    duration_minutes;

SELECT * FROM SpotifyPlaylist_DW.Views.song_duration_analysis;


CREATE OR REPLACE VIEW SpotifyPlaylist_DW.Views.new_vs_old_songs_performance AS
SELECT 
    CASE 
        WHEN song_added >= CURRENT_DATE - INTERVAL '20 DAYS' THEN 'New'
        ELSE 'Old'
    END AS song_category,
    AVG(popularity) AS avg_popularity,
    COUNT(DISTINCT song_id) AS song_count
FROM 
    SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.song
GROUP BY 
    song_category
ORDER BY 
    song_category;

SELECT * FROM SpotifyPlaylist_DW.Views.new_vs_old_songs_performance;


CREATE OR REPLACE VIEW SpotifyPlaylist_DW.Views.top_albums_by_total_tracks AS
SELECT 
    DISTINCT album.album_id,
    album.name AS album_name,
    album.total_tracks
FROM 
    SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.album
ORDER BY 
    total_tracks DESC;

SELECT * FROM SpotifyPlaylist_DW.Views.top_albums_by_total_tracks;


CREATE OR REPLACE VIEW SpotifyPlaylist_DW.Views.top_bottom_artists AS
WITH ArtistMetrics AS (
    SELECT 
        artist.artist_name,
        AVG(song.popularity) AS avg_popularity_score,
        AVG(song.duration_ms) / 60000 AS avg_song_duration_min, -- Convert ms to minutes
        SUM(DISTINCT album.total_tracks) AS total_tracks_in_album
    FROM 
        SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.song AS song
    JOIN 
        SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.artist AS artist
        ON song.artist_id = artist.artist_id
    JOIN 
        SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.album AS album
        ON song.album_id = album.album_id
    GROUP BY 
        artist.artist_name
)
SELECT 
    artist_name,
    avg_popularity_score,
    avg_song_duration_min,
    total_tracks_in_album
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (ORDER BY avg_popularity_score DESC) AS popularity_rank_top,
        ROW_NUMBER() OVER (ORDER BY avg_popularity_score ASC) AS popularity_rank_bottom
    FROM 
        ArtistMetrics
) ranked_artists
WHERE 
    popularity_rank_top <= 5 OR popularity_rank_bottom <= 5
ORDER BY 
    popularity_rank_top, 
    popularity_rank_bottom;

SELECT * FROM SpotifyPlaylist_DW.Views.top_bottom_artists;
-- ------------------------------------------------------------------------------------------------