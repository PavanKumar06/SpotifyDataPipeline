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

CREATE OR REPLACE TABLE SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.song (
    song_id STRING PRIMARY KEY,
    song_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added DATE,
    album_id STRING,  -- Foreign key referencing album table
    artist_id STRING  -- Foreign key referencing artist table
);



--Create Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION Spotify_S3
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::471112791117:role/snowflake-s3-conn'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-snowflake-etl-pavan')
    COMMENT = 'Has Access To The Spotify S3 Bucket';

--Add the STORAGE_AWS_EXTERNAL_ID to the Trust Relationships of the IAM Role
DESC INTEGRATION Spotify_S3;

CREATE SCHEMA IF NOT EXISTS SpotifyPlaylist_DW.FILE_FORMATS;

--Create File Format
CREATE OR REPLACE FILE FORMAT SpotifyPlaylist_DW.FILE_FORMATS.CSV_FILEFORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    NULL_IF = ('NULL')
    EMPTY_FIELD_AS_NULL = TRUE;

CREATE SCHEMA IF NOT EXISTS SpotifyPlaylist_DW.EXTERNAL_STAGES;

--Create the Stage, this allows access to the transformed_data folder 
CREATE OR REPLACE STAGE SpotifyPlaylist_DW.EXTERNAL_STAGES.SpotifyBucket_TransformedData
    URL = 's3://spotify-snowflake-etl-pavan/transformed_data/'
    STORAGE_INTEGRATION = Spotify_S3
    FILE_FORMAT = SpotifyPlaylist_DW.FILE_FORMATS.CSV_FILEFORMAT;

--Test the connection, you should see all the csv files
LIST @SpotifyPlaylist_DW.EXTERNAL_STAGES.SpotifyBucket_TransformedData;



--Create Pipes to automatically load data in real-time from S3 into Snowflake
CREATE SCHEMA IF NOT EXISTS SpotifyPlaylist_DW.PIPES;

--Pipe to detect file changes from album_data and ingest into album table 
CREATE OR REPLACE PIPE SpotifyPlaylist_DW.PIPES.ALBUM_DATA
    AUTO_INGEST = TRUE
    AS
    COPY INTO SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.album
    FROM @SpotifyPlaylist_DW.EXTERNAL_STAGES.SpotifyBucket_TransformedData/album_data;

--Use the notification_channel, to add an Event to the S3 bucket. Ensure you specify the exact album_data folder.
DESC PIPE SpotifyPlaylist_DW.PIPES.ALBUM_DATA;

--Manually run the lambda function to test if data is inserted into the Table
SELECT TOP 10 * FROM SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.album;


--Pipe to detect file changes from artist_data and ingest into artist table 
CREATE OR REPLACE PIPE SpotifyPlaylist_DW.PIPES.ARTIST_DATA
    AUTO_INGEST = TRUE
    AS
    COPY INTO SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.artist
    FROM @SpotifyPlaylist_DW.EXTERNAL_STAGES.SpotifyBucket_TransformedData/artist_data;

--Copy the notification_channel, goto the S3 bucket -> Properties -> Add Event -> Destination Type - SQS Queue
DESC PIPE SpotifyPlaylist_DW.PIPES.ARTIST_DATA;

--Manually run the lambda function to test if data is inserted into the Table
SELECT TOP 10 * FROM SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.artist;


--Pipe to detect file changes from artist_data and ingest into artist table 
CREATE OR REPLACE PIPE SpotifyPlaylist_DW.PIPES.SONG_DATA
    AUTO_INGEST = TRUE
    AS
    COPY INTO SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.song
    FROM @SpotifyPlaylist_DW.EXTERNAL_STAGES.SpotifyBucket_TransformedData/songs_data;

--Specify the proper filter i.e., the exact path (PREFIX) and the file type (SUFFIX)
DESC PIPE SpotifyPlaylist_DW.PIPES.ARTIST_DATA;

--Manually run the lambda function to test if data is inserted into the Table
SELECT TOP 10 * FROM SpotifyPlaylist_DW.SpotifyPlaylist_DW_SCHEMA.song;