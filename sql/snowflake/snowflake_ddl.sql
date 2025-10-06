-- =====================================================
-- Snowflake DDL Scripts for Spotify Data Pipeline
-- Created for: Spotify ETL Data Pipeline Project
-- Author: Akshat Sharma
-- Date: 2025-01-16
-- =====================================================

-- Create database and schema
CREATE DATABASE IF NOT EXISTS SPOTIFY_DATA;
USE DATABASE SPOTIFY_DATA;

CREATE SCHEMA IF NOT EXISTS RAW_DATA;
USE SCHEMA RAW_DATA;

-- =====================================================
-- 1. ALBUMS TABLE DDL
-- =====================================================
CREATE OR REPLACE TABLE ALBUMS (
    ALBUM_ID VARCHAR(255) NOT NULL PRIMARY KEY,
    ALBUM_NAME VARCHAR(500) NOT NULL,
    RELEASE_DATE DATE,
    TOTAL_TRACKS INTEGER,
    SPOTIFY_URL VARCHAR(500),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add comments for documentation
COMMENT ON TABLE ALBUMS IS 'Spotify album information extracted from playlist data';
COMMENT ON COLUMN ALBUMS.ALBUM_ID IS 'Unique Spotify album identifier';
COMMENT ON COLUMN ALBUMS.ALBUM_NAME IS 'Name of the album';
COMMENT ON COLUMN ALBUMS.RELEASE_DATE IS 'Date when the album was released';
COMMENT ON COLUMN ALBUMS.TOTAL_TRACKS IS 'Total number of tracks in the album';
COMMENT ON COLUMN ALBUMS.SPOTIFY_URL IS 'Spotify web URL for the album';
COMMENT ON COLUMN ALBUMS.CREATED_AT IS 'Timestamp when record was created';
COMMENT ON COLUMN ALBUMS.UPDATED_AT IS 'Timestamp when record was last updated';

-- =====================================================
-- 2. ARTISTS TABLE DDL
-- =====================================================
CREATE OR REPLACE TABLE ARTISTS (
    ARTIST_ID VARCHAR(255) NOT NULL PRIMARY KEY,
    ARTIST_NAME VARCHAR(500) NOT NULL,
    SPOTIFY_URL VARCHAR(500),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add comments for documentation
COMMENT ON TABLE ARTISTS IS 'Spotify artist information extracted from playlist data';
COMMENT ON COLUMN ARTISTS.ARTIST_ID IS 'Unique Spotify artist identifier';
COMMENT ON COLUMN ARTISTS.ARTIST_NAME IS 'Name of the artist';
COMMENT ON COLUMN ARTISTS.SPOTIFY_URL IS 'Spotify web URL for the artist profile';
COMMENT ON COLUMN ARTISTS.CREATED_AT IS 'Timestamp when record was created';
COMMENT ON COLUMN ARTISTS.UPDATED_AT IS 'Timestamp when record was last updated';

-- =====================================================
-- 3. SONGS TABLE DDL
-- =====================================================
CREATE OR REPLACE TABLE SONGS (
    SONG_ID VARCHAR(255) NOT NULL PRIMARY KEY,
    SONG_NAME VARCHAR(500) NOT NULL,
    ADDED_AT TIMESTAMP_NTZ,
    DURATION_MS INTEGER,
    POPULARITY INTEGER,
    SPOTIFY_URL VARCHAR(500),
    ALBUM_ID VARCHAR(255) NOT NULL,
    ARTIST_ID VARCHAR(255) NOT NULL,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Foreign key constraints
    CONSTRAINT FK_SONGS_ALBUM FOREIGN KEY (ALBUM_ID) REFERENCES ALBUMS(ALBUM_ID),
    CONSTRAINT FK_SONGS_ARTIST FOREIGN KEY (ARTIST_ID) REFERENCES ARTISTS(ARTIST_ID)
);

-- Add comments for documentation
COMMENT ON TABLE SONGS IS 'Spotify song/track information extracted from playlist data';
COMMENT ON COLUMN SONGS.SONG_ID IS 'Unique Spotify track identifier';
COMMENT ON COLUMN SONGS.SONG_NAME IS 'Name of the song/track';
COMMENT ON COLUMN SONGS.ADDED_AT IS 'Timestamp when the track was added to the playlist';
COMMENT ON COLUMN SONGS.DURATION_MS IS 'Duration of the track in milliseconds';
COMMENT ON COLUMN SONGS.POPULARITY IS 'Spotify popularity score (0-100)';
COMMENT ON COLUMN SONGS.SPOTIFY_URL IS 'Spotify web URL for the track';
COMMENT ON COLUMN SONGS.ALBUM_ID IS 'Foreign key reference to ALBUMS table';
COMMENT ON COLUMN SONGS.ARTIST_ID IS 'Foreign key reference to ARTISTS table';
COMMENT ON COLUMN SONGS.CREATED_AT IS 'Timestamp when record was created';
COMMENT ON COLUMN SONGS.UPDATED_AT IS 'Timestamp when record was last updated';

-- =====================================================
-- 4. CREATE INDEXES FOR PERFORMANCE
-- =====================================================

-- Indexes on foreign keys for better join performance
CREATE INDEX IF NOT EXISTS IDX_SONGS_ALBUM_ID ON SONGS(ALBUM_ID);
CREATE INDEX IF NOT EXISTS IDX_SONGS_ARTIST_ID ON SONGS(ARTIST_ID);

-- Indexes on commonly queried columns
CREATE INDEX IF NOT EXISTS IDX_SONGS_POPULARITY ON SONGS(POPULARITY DESC);
CREATE INDEX IF NOT EXISTS IDX_ALBUMS_RELEASE_DATE ON ALBUMS(RELEASE_DATE);
CREATE INDEX IF NOT EXISTS IDX_SONGS_ADDED_AT ON SONGS(ADDED_AT);

-- =====================================================
-- 5. CREATE VIEWS FOR COMMON ANALYTICS QUERIES
-- =====================================================

-- View for popular songs with artist and album information
CREATE OR REPLACE VIEW POPULAR_SONGS_VIEW AS
SELECT 
    s.SONG_NAME,
    a.ARTIST_NAME,
    al.ALBUM_NAME,
    s.POPULARITY,
    s.DURATION_MS,
    ROUND(s.DURATION_MS / 1000.0 / 60.0, 2) AS DURATION_MINUTES,
    s.SPOTIFY_URL,
    s.ADDED_AT
FROM SONGS s
JOIN ARTISTS a ON s.ARTIST_ID = a.ARTIST_ID
JOIN ALBUMS al ON s.ALBUM_ID = al.ALBUM_ID
ORDER BY s.POPULARITY DESC;

-- View for album statistics
CREATE OR REPLACE VIEW ALBUM_STATISTICS_VIEW AS
SELECT 
    al.ALBUM_NAME,
    a.ARTIST_NAME,
    al.RELEASE_DATE,
    al.TOTAL_TRACKS,
    COUNT(s.SONG_ID) AS SONGS_IN_PLAYLIST,
    AVG(s.POPULARITY) AS AVG_POPULARITY,
    SUM(s.DURATION_MS) AS TOTAL_DURATION_MS,
    ROUND(SUM(s.DURATION_MS) / 1000.0 / 60.0, 2) AS TOTAL_DURATION_MINUTES
FROM ALBUMS al
LEFT JOIN SONGS s ON al.ALBUM_ID = s.ALBUM_ID
LEFT JOIN ARTISTS a ON s.ARTIST_ID = a.ARTIST_ID
GROUP BY al.ALBUM_ID, al.ALBUM_NAME, a.ARTIST_NAME, al.RELEASE_DATE, al.TOTAL_TRACKS;

-- View for artist statistics
CREATE OR REPLACE VIEW ARTIST_STATISTICS_VIEW AS
SELECT 
    a.ARTIST_NAME,
    COUNT(DISTINCT s.SONG_ID) AS TOTAL_SONGS,
    COUNT(DISTINCT s.ALBUM_ID) AS TOTAL_ALBUMS,
    AVG(s.POPULARITY) AS AVG_POPULARITY,
    MAX(s.POPULARITY) AS MAX_POPULARITY,
    SUM(s.DURATION_MS) AS TOTAL_DURATION_MS,
    ROUND(SUM(s.DURATION_MS) / 1000.0 / 60.0, 2) AS TOTAL_DURATION_MINUTES
FROM ARTISTS a
LEFT JOIN SONGS s ON a.ARTIST_ID = s.ARTIST_ID
GROUP BY a.ARTIST_ID, a.ARTIST_NAME;

-- =====================================================
-- 6. STAGING TABLES FOR DATA LOADING
-- =====================================================

-- Staging table for albums data loading
CREATE OR REPLACE TABLE STAGING_ALBUMS (
    ALBUM_ID VARCHAR(255),
    ALBUM_NAME VARCHAR(500),
    RELEASE_DATE VARCHAR(50),  -- Keep as VARCHAR for flexible date parsing
    TOTAL_TRACKS INTEGER,
    SPOTIFY_URL VARCHAR(500),
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Staging table for artists data loading
CREATE OR REPLACE TABLE STAGING_ARTISTS (
    ARTIST_ID VARCHAR(255),
    ARTIST_NAME VARCHAR(500),
    SPOTIFY_URL VARCHAR(500),
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Staging table for songs data loading
CREATE OR REPLACE TABLE STAGING_SONGS (
    SONG_ID VARCHAR(255),
    SONG_NAME VARCHAR(500),
    ADDED_AT VARCHAR(50),  -- Keep as VARCHAR for flexible timestamp parsing
    DURATION_MS INTEGER,
    POPULARITY INTEGER,
    SPOTIFY_URL VARCHAR(500),
    ALBUM_ID VARCHAR(255),
    ARTIST_ID VARCHAR(255),
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- 7. DATA QUALITY AND MONITORING TABLES
-- =====================================================

-- Table to track data loading history
CREATE OR REPLACE TABLE DATA_LOAD_LOG (
    LOAD_ID VARCHAR(255) DEFAULT UUID_STRING(),
    TABLE_NAME VARCHAR(100),
    RECORDS_LOADED INTEGER,
    LOAD_STATUS VARCHAR(50),
    LOAD_START_TIME TIMESTAMP_NTZ,
    LOAD_END_TIME TIMESTAMP_NTZ,
    ERROR_MESSAGE VARCHAR(1000),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Table to track data quality metrics
CREATE OR REPLACE TABLE DATA_QUALITY_METRICS (
    METRIC_ID VARCHAR(255) DEFAULT UUID_STRING(),
    TABLE_NAME VARCHAR(100),
    METRIC_TYPE VARCHAR(100),
    METRIC_VALUE NUMBER,
    MEASURED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- 8. GRANTS AND PERMISSIONS (Customize as needed)
-- =====================================================

-- Grant permissions to roles (customize based on your Snowflake setup)
-- GRANT SELECT ON ALL TABLES IN SCHEMA RAW_DATA TO ROLE ANALYST_ROLE;
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA RAW_DATA TO ROLE ETL_ROLE;

-- =====================================================
-- 9. USEFUL QUERIES FOR DATA VALIDATION
-- =====================================================

-- Query to check data completeness
/*
SELECT 
    'ALBUMS' as TABLE_NAME,
    COUNT(*) as TOTAL_RECORDS,
    COUNT(ALBUM_ID) as NON_NULL_IDS,
    COUNT(ALBUM_NAME) as NON_NULL_NAMES,
    COUNT(RELEASE_DATE) as NON_NULL_DATES
FROM ALBUMS
UNION ALL
SELECT 
    'ARTISTS' as TABLE_NAME,
    COUNT(*) as TOTAL_RECORDS,
    COUNT(ARTIST_ID) as NON_NULL_IDS,
    COUNT(ARTIST_NAME) as NON_NULL_NAMES,
    0 as NON_NULL_DATES
FROM ARTISTS
UNION ALL
SELECT 
    'SONGS' as TABLE_NAME,
    COUNT(*) as TOTAL_RECORDS,
    COUNT(SONG_ID) as NON_NULL_IDS,
    COUNT(SONG_NAME) as NON_NULL_NAMES,
    COUNT(ADDED_AT) as NON_NULL_DATES
FROM SONGS;
*/

-- Query to check referential integrity
/*
SELECT 
    'SONGS with invalid ALBUM_ID' as CHECK_TYPE,
    COUNT(*) as VIOLATION_COUNT
FROM SONGS s
LEFT JOIN ALBUMS a ON s.ALBUM_ID = a.ALBUM_ID
WHERE a.ALBUM_ID IS NULL
UNION ALL
SELECT 
    'SONGS with invalid ARTIST_ID' as CHECK_TYPE,
    COUNT(*) as VIOLATION_COUNT
FROM SONGS s
LEFT JOIN ARTISTS a ON s.ARTIST_ID = a.ARTIST_ID
WHERE a.ARTIST_ID IS NULL;
*/
