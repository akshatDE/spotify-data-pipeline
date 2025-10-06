from loguru import logger
import pandas as pd

class TransformData:
    """
    Handles data transformation and cleaning operations on extracted Spotify data.
    
    This class inherits from DataExtract and adds transformation capabilities
    to convert raw extracted data into clean, analysis-ready pandas DataFrames.
    It performs deduplication, data type conversions, and data quality improvements.
    
    Inherits all extraction methods from DataExtract and adds transformation logic.
    """
    def __init__(self, album_data=None, artist_data=None, song_data=None):
        """
        Initialize transformer with already-extracted data.
        
        Args:
            album_data (list): Extracted album data from DataExtract
            artist_data (list): Extracted artist data from DataExtract  
            song_data (list): Extracted song data from DataExtract
        """
        self.album_data = album_data
        self.artist_data = artist_data
        self.song_data = song_data

    def transform_album_data(self):
        """
        Transform raw album data into a clean pandas DataFrame.
        
        Applies data cleaning operations including:
        - Deduplication based on album ID
        - Date parsing and conversion to datetime format
        - Data quality logging and validation
        
            
        Returns:
            pandas.DataFrame: Cleaned album data with columns:
                - id, name, release_date (datetime), total_tracks, url
                
        Note:
            Uses 'coerce' for date parsing to handle malformed dates gracefully
        """
        try:

            album_df = pd.DataFrame(self.album_data)
            # Droping duplicates based on 'id' column
            album_df = album_df.drop_duplicates(subset=['id'])
            # Converting release_date to datetime format
            album_df['release_date'] = pd.to_datetime(album_df['release_date'], errors='coerce')
            logger.info(f"Album Data transformed to DataFrame with shape {album_df.shape}")
            return album_df
        except Exception as e:
            logger.error(f"Error transforming album data: {e}")
            return pd.DataFrame()

    def transform_artist_data(self):
        """
        Transform raw artist data into a clean pandas DataFrame.
        
        Applies data cleaning operations including:
        - Deduplication based on artist ID to handle artists appearing multiple times
        - Data structure validation and logging
        
        Args:
            playlist_id (str): Spotify playlist identifier
            
        Returns:
            pandas.DataFrame: Cleaned artist data with columns:
                - id, name, url
        """
        try:

            artist_df = pd.DataFrame(self.artist_data)
            # Droping duplicates based on 'id' column
            artist_df = artist_df.drop_duplicates(subset=['id'])
            logger.info(f"Artist Data transformed to DataFrame with shape {artist_df.shape}")
            return artist_df
        except Exception as e:
            logger.error(f"Error transforming artist data: {e}")
            return pd.DataFrame()

    def transform_song_data(self):
        """
        Transform raw song data into a clean pandas DataFrame.
        
        Applies comprehensive data cleaning including:
        - Deduplication based on track ID
        - Timestamp parsing for playlist addition dates
        - Data validation and quality logging
        
        Args:
            playlist_id (str): Spotify playlist identifier
            
        Returns:
            pandas.DataFrame: Cleaned song data with columns:
                - id, name, added_at (datetime), duration_ms, popularity, 
                  url, album_id, artist_ids
                  
        Note:
            The added_at timestamp indicates when the track was added to the playlist
        """
        try:
            song_df = pd.DataFrame(self.song_data)
            # Droping duplicates based on 'id' column
            song_df = song_df.drop_duplicates(subset=['id'])
            # Converting added_at to datetime format
            song_df['added_at'] = pd.to_datetime(song_df['added_at'], errors='coerce')
            logger.info(f"Song Data transformed to DataFrame with shape {song_df.shape}")
            return song_df
        except Exception as e:
            logger.error(f"Error transforming song data: {e}")
            return pd.DataFrame()