-- Snowflake DDL for Spotify ETL Pipeline
-- Complete schema for Album, Artist, and Song data

-- Database and Schema Creation
CREATE DATABASE IF NOT EXISTS SPOTIFY_ETL;
USE DATABASE SPOTIFY_ETL;

CREATE SCHEMA IF NOT EXISTS RAW_DATA;
USE SCHEMA RAW_DATA;

-- =============================================
-- ALBUM DATA TABLE
-- =============================================
CREATE OR REPLACE TABLE ALBUM_DATA (
    ID VARCHAR(50) NOT NULL COMMENT 'Spotify Album ID - Primary identifier',
    NAME VARCHAR(500) NOT NULL COMMENT 'Album name/title',
    RELEASE_DATE DATE NOT NULL COMMENT 'Album release date in YYYY-MM-DD format',
    TOTAL_TRACKS NUMBER(10,0) NOT NULL COMMENT 'Total number of tracks in the album',
    URL VARCHAR(1000) NOT NULL COMMENT 'Spotify URL for the album',
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp',
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record update timestamp'
);

-- Primary Key and Indexes for Album
ALTER TABLE ALBUM_DATA ADD CONSTRAINT PK_ALBUM_DATA PRIMARY KEY (ID);
CREATE INDEX IF NOT EXISTS IDX_ALBUM_RELEASE_DATE ON ALBUM_DATA (RELEASE_DATE);
CREATE INDEX IF NOT EXISTS IDX_ALBUM_NAME ON ALBUM_DATA (NAME);

COMMENT ON TABLE ALBUM_DATA IS 'Spotify album information extracted from playlists via ETL pipeline';

-- =============================================
-- ARTIST DATA TABLE
-- =============================================
CREATE OR REPLACE TABLE ARTIST_DATA (
    ID VARCHAR(50) NOT NULL COMMENT 'Spotify Artist ID - Primary identifier',
    NAME VARCHAR(500) NOT NULL COMMENT 'Artist name',
    URL VARCHAR(1000) NOT NULL COMMENT 'Spotify URL for the artist',
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp',
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record update timestamp'
);

-- Primary Key and Indexes for Artist
ALTER TABLE ARTIST_DATA ADD CONSTRAINT PK_ARTIST_DATA PRIMARY KEY (ID);
CREATE INDEX IF NOT EXISTS IDX_ARTIST_NAME ON ARTIST_DATA (NAME);

COMMENT ON TABLE ARTIST_DATA IS 'Spotify artist information extracted from playlists via ETL pipeline';

-- =============================================
-- SONG DATA TABLE
-- =============================================
CREATE OR REPLACE TABLE SONG_DATA (
    ID VARCHAR(50) NOT NULL COMMENT 'Spotify Track ID - Primary identifier',
    NAME VARCHAR(500) NOT NULL COMMENT 'Song/track name',
    ADDED_AT TIMESTAMP_LTZ NOT NULL COMMENT 'When the track was added to the playlist',
    DURATION_MS NUMBER(12,0) NOT NULL COMMENT 'Track duration in milliseconds',
    POPULARITY NUMBER(3,0) NOT NULL COMMENT 'Track popularity score (0-100)',
    URL VARCHAR(1000) NOT NULL COMMENT 'Spotify URL for the track',
    ALBUM_ID VARCHAR(50) NOT NULL COMMENT 'Reference to Album ID',
    ARTIST_IDS VARCHAR(2000) NOT NULL COMMENT 'Comma-separated list of artist IDs',
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp',
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record update timestamp'
);

-- Primary Key and Indexes for Song
ALTER TABLE SONG_DATA ADD CONSTRAINT PK_SONG_DATA PRIMARY KEY (ID);
CREATE INDEX IF NOT EXISTS IDX_SONG_ALBUM_ID ON SONG_DATA (ALBUM_ID);
CREATE INDEX IF NOT EXISTS IDX_SONG_ADDED_AT ON SONG_DATA (ADDED_AT);
CREATE INDEX IF NOT EXISTS IDX_SONG_POPULARITY ON SONG_DATA (POPULARITY);
CREATE INDEX IF NOT EXISTS IDX_SONG_NAME ON SONG_DATA (NAME);

COMMENT ON TABLE SONG_DATA IS 'Spotify song/track information extracted from playlists via ETL pipeline';

-- =============================================
-- FOREIGN KEY CONSTRAINTS
-- =============================================
-- Add foreign key relationship between song and album
ALTER TABLE SONG_DATA ADD CONSTRAINT FK_SONG_ALBUM 
    FOREIGN KEY (ALBUM_ID) REFERENCES ALBUM_DATA(ID);

-- =============================================
-- DATA LOADING COMMANDS
-- =============================================

-- Album Data Loading
/*
PUT file:///path/to/album_data.csv @%ALBUM_DATA;

COPY INTO ALBUM_DATA (ID, NAME, RELEASE_DATE, TOTAL_TRACKS, URL)
FROM @%ALBUM_DATA/album_data.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = 'CONTINUE';
*/

-- Artist Data Loading
/*
PUT file:///path/to/artist_data.csv @%ARTIST_DATA;

COPY INTO ARTIST_DATA (ID, NAME, URL)
FROM @%ARTIST_DATA/artist_data.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = 'CONTINUE';
*/

-- Song Data Loading
/*
PUT file:///path/to/song_data.csv @%SONG_DATA;

COPY INTO SONG_DATA (ID, NAME, ADDED_AT, DURATION_MS, POPULARITY, URL, ALBUM_ID, ARTIST_IDS)
FROM @%SONG_DATA/song_data.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS+TZH:TZM'
)
ON_ERROR = 'CONTINUE';
*/

-- =============================================
-- ANALYTICAL VIEWS
-- =============================================

-- Create a consolidated view joining all tables
CREATE OR REPLACE VIEW V_SPOTIFY_ANALYTICS AS
SELECT 
    s.ID as SONG_ID,
    s.NAME as SONG_NAME,
    s.DURATION_MS,
    s.POPULARITY,
    s.ADDED_AT,
    a.ID as ALBUM_ID,
    a.NAME as ALBUM_NAME,
    a.RELEASE_DATE as ALBUM_RELEASE_DATE,
    a.TOTAL_TRACKS,
    s.ARTIST_IDS,
    ROUND(s.DURATION_MS / 1000.0, 2) as DURATION_SECONDS,
    ROUND(s.DURATION_MS / 60000.0, 2) as DURATION_MINUTES,
    YEAR(a.RELEASE_DATE) as RELEASE_YEAR,
    QUARTER(a.RELEASE_DATE) as RELEASE_QUARTER,
    CASE 
        WHEN s.POPULARITY >= 80 THEN 'High'
        WHEN s.POPULARITY >= 60 THEN 'Medium'
        WHEN s.POPULARITY >= 40 THEN 'Low'
        ELSE 'Very Low'
    END as POPULARITY_CATEGORY
FROM SONG_DATA s
JOIN ALBUM_DATA a ON s.ALBUM_ID = a.ID;

COMMENT ON VIEW V_SPOTIFY_ANALYTICS IS 'Consolidated view combining song, album, and derived analytical fields';

-- =============================================
-- SAMPLE ANALYTICAL QUERIES
-- =============================================

-- Verify table structure
-- DESC TABLE ALBUM_DATA;
-- DESC TABLE ARTIST_DATA;
-- DESC TABLE SONG_DATA;

-- Basic data validation queries
/*
-- Count records in each table
SELECT 'ALBUMS' as TABLE_NAME, COUNT(*) as RECORD_COUNT FROM ALBUM_DATA
UNION ALL
SELECT 'ARTISTS' as TABLE_NAME, COUNT(*) as RECORD_COUNT FROM ARTIST_DATA
UNION ALL
SELECT 'SONGS' as TABLE_NAME, COUNT(*) as RECORD_COUNT FROM SONG_DATA;

-- Top 10 most popular songs
SELECT 
    SONG_NAME,
    ALBUM_NAME,
    POPULARITY,
    DURATION_MINUTES,
    RELEASE_YEAR
FROM V_SPOTIFY_ANALYTICS 
ORDER BY POPULARITY DESC 
LIMIT 10;

-- Albums by release year
SELECT 
    RELEASE_YEAR,
    COUNT(*) as ALBUM_COUNT,
    AVG(TOTAL_TRACKS) as AVG_TRACKS_PER_ALBUM
FROM ALBUM_DATA 
GROUP BY RELEASE_YEAR 
ORDER BY RELEASE_YEAR DESC;

-- Average song duration by popularity category
SELECT 
    POPULARITY_CATEGORY,
    COUNT(*) as SONG_COUNT,
    ROUND(AVG(DURATION_MINUTES), 2) as AVG_DURATION_MINUTES,
    ROUND(AVG(POPULARITY), 1) as AVG_POPULARITY_SCORE
FROM V_SPOTIFY_ANALYTICS
GROUP BY POPULARITY_CATEGORY
ORDER BY AVG_POPULARITY_SCORE DESC;

-- Most recent additions to playlist
SELECT 
    SONG_NAME,
    ALBUM_NAME,
    ADDED_AT,
    POPULARITY
FROM V_SPOTIFY_ANALYTICS 
ORDER BY ADDED_AT DESC 
LIMIT 10;
*/