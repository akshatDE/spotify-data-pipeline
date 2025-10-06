-- =====================================================
-- Snowflake Data Loading Script for Spotify CSV Files
-- Instructions for loading data from CSV files to Snowflake tables
-- =====================================================

-- Set the database and schema context
USE DATABASE SPOTIFY_DATA;
USE SCHEMA RAW_DATA;

-- =====================================================
-- METHOD 1: Using Snowflake Web UI or SnowSQL
-- =====================================================

-- Step 1: Upload CSV files to Snowflake stage (if using web UI)
-- Or create an external stage pointing to your cloud storage

-- Step 2: Load Albums data
-- Note: Adjust the file path and format options based on your CSV file location
COPY INTO ALBUMS (
    ALBUM_ID,
    ALBUM_NAME, 
    RELEASE_DATE,
    TOTAL_TRACKS,
    SPOTIFY_URL
)
FROM '@your_stage/album_data.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Step 3: Load Artists data
COPY INTO ARTISTS (
    ARTIST_ID,
    ARTIST_NAME,
    SPOTIFY_URL
)
FROM '@your_stage/artist_data.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Step 4: Load Songs data
COPY INTO SONGS (
    SONG_ID,
    SONG_NAME,
    ADDED_AT,
    DURATION_MS,
    POPULARITY,
    SPOTIFY_URL,
    ALBUM_ID,
    ARTIST_ID
)
FROM '@your_stage/song_data.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- =====================================================
-- METHOD 2: Using Python Snowflake Connector
-- =====================================================

-- If you're loading data programmatically using Python, here's the approach:

/*
# Python code example for loading data:
import snowflake.connector
import pandas as pd

# Read CSV files
albums_df = pd.read_csv('album_data.csv')
artists_df = pd.read_csv('artist_data.csv') 
songs_df = pd.read_csv('song_data.csv')

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='your_username',
    password='your_password',
    account='your_account',
    warehouse='your_warehouse',
    database='SPOTIFY_DATA',
    schema='RAW_DATA'
)

# Create cursor
cur = conn.cursor()

# Load albums data
albums_df.to_sql('ALBUMS', conn, if_exists='append', index=False)

# Load artists data  
artists_df.to_sql('ARTISTS', conn, if_exists='append', index=False)

# Load songs data
songs_df.to_sql('SONGS', conn, if_exists='append', index=False)

conn.close()
*/

-- =====================================================
-- METHOD 3: Using Snowflake PUT command (for local files)
-- =====================================================

-- If you have local CSV files and want to upload them:

-- Step 1: Create a stage (if not exists)
CREATE STAGE IF NOT EXISTS spotify_stage;

-- Step 2: Upload files to stage
-- PUT file:///path/to/your/album_data.csv @spotify_stage;
-- PUT file:///path/to/your/artist_data.csv @spotify_stage;  
-- PUT file:///path/to/your/song_data.csv @spotify_stage;

-- Step 3: Copy data from stage to tables
COPY INTO ALBUMS (ALBUM_ID, ALBUM_NAME, RELEASE_DATE, TOTAL_TRACKS, SPOTIFY_URL)
FROM '@spotify_stage/album_data.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO ARTISTS (ARTIST_ID, ARTIST_NAME, SPOTIFY_URL)
FROM '@spotify_stage/artist_data.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO SONGS (SONG_ID, SONG_NAME, ADDED_AT, DURATION_MS, POPULARITY, SPOTIFY_URL, ALBUM_ID, ARTIST_ID)
FROM '@spotify_stage/song_data.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- =====================================================
-- DATA VALIDATION QUERIES
-- =====================================================

-- Check record counts
SELECT 'ALBUMS' as TABLE_NAME, COUNT(*) as RECORD_COUNT FROM ALBUMS
UNION ALL
SELECT 'ARTISTS' as TABLE_NAME, COUNT(*) as RECORD_COUNT FROM ARTISTS  
UNION ALL
SELECT 'SONGS' as TABLE_NAME, COUNT(*) as RECORD_COUNT FROM SONGS;

-- Check for data quality issues
SELECT 
    'Albums with NULL names' as ISSUE_TYPE,
    COUNT(*) as COUNT
FROM ALBUMS 
WHERE ALBUM_NAME IS NULL
UNION ALL
SELECT 
    'Songs with invalid popularity scores' as ISSUE_TYPE,
    COUNT(*) as COUNT
FROM SONGS 
WHERE POPULARITY < 0 OR POPULARITY > 100
UNION ALL
SELECT 
    'Songs with NULL durations' as ISSUE_TYPE,
    COUNT(*) as COUNT
FROM SONGS 
WHERE DURATION_MS IS NULL OR DURATION_MS <= 0;

-- Check referential integrity
SELECT 
    'Songs with missing album references' as ISSUE_TYPE,
    COUNT(*) as COUNT
FROM SONGS s
LEFT JOIN ALBUMS a ON s.ALBUM_ID = a.ALBUM_ID
WHERE a.ALBUM_ID IS NULL
UNION ALL
SELECT 
    'Songs with missing artist references' as ISSUE_TYPE,
    COUNT(*) as COUNT
FROM SONGS s
LEFT JOIN ARTISTS a ON s.ARTIST_ID = a.ARTIST_ID
WHERE a.ARTIST_ID IS NULL;

-- =====================================================
-- SAMPLE ANALYTICS QUERIES
-- =====================================================

-- Top 10 most popular songs
SELECT 
    s.SONG_NAME,
    a.ARTIST_NAME,
    al.ALBUM_NAME,
    s.POPULARITY,
    ROUND(s.DURATION_MS / 1000.0 / 60.0, 2) AS DURATION_MINUTES
FROM SONGS s
JOIN ARTISTS a ON s.ARTIST_ID = a.ARTIST_ID
JOIN ALBUMS al ON s.ALBUM_ID = al.ALBUM_ID
ORDER BY s.POPULARITY DESC
LIMIT 10;

-- Albums by release year
SELECT 
    YEAR(al.RELEASE_DATE) as RELEASE_YEAR,
    COUNT(*) as ALBUM_COUNT,
    COUNT(DISTINCT a.ARTIST_ID) as UNIQUE_ARTISTS
FROM ALBUMS al
JOIN SONGS s ON al.ALBUM_ID = s.ALBUM_ID
JOIN ARTISTS a ON s.ARTIST_ID = a.ARTIST_ID
WHERE al.RELEASE_DATE IS NOT NULL
GROUP BY YEAR(al.RELEASE_DATE)
ORDER BY RELEASE_YEAR DESC;

-- Artist statistics
SELECT 
    a.ARTIST_NAME,
    COUNT(DISTINCT s.SONG_ID) as TOTAL_SONGS,
    COUNT(DISTINCT s.ALBUM_ID) as TOTAL_ALBUMS,
    AVG(s.POPULARITY) as AVG_POPULARITY,
    SUM(s.DURATION_MS) / 1000.0 / 60.0 as TOTAL_DURATION_MINUTES
FROM ARTISTS a
JOIN SONGS s ON a.ARTIST_ID = s.ARTIST_ID
GROUP BY a.ARTIST_ID, a.ARTIST_NAME
ORDER BY AVG_POPULARITY DESC;
