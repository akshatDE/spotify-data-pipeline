import os
from loguru import logger
import pandas as pd

class LoadDataLocal:
    """
    Handles local file persistence for transformed Spotify data.
    
    Saves cleaned DataFrames to CSV files for local storage or analysis.
    Part of the ETL pipeline's Load stage.
    """

    def __init__(self, album_df=None, artist_df=None, song_df=None):
        """
        Initialize loader with transformed DataFrames.
        
        Args:
            album_df (pd.DataFrame): Transformed album data
            artist_df (pd.DataFrame): Transformed artist data  
            song_df (pd.DataFrame): Transformed song data
        """
        self.album_df = album_df
        self.artist_df = artist_df
        self.song_df = song_df

    def _validate_and_save(self, df, filepath, data_type):
        """Helper method to validate and save DataFrame."""
        if df is None or df.empty:
            logger.error(f"{data_type} data is None or empty")
            raise ValueError(f"No {data_type} data to save")
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath) or '.', exist_ok=True)
        
        try:
            df.to_csv(filepath, index=False)
            logger.info(f"{data_type} data saved to {filepath} with shape {df.shape}")
        except Exception as e:
            logger.error(f"Failed to save {data_type} data to {filepath}: {e}")
            raise

    def load_album_data(self, filepath):
        """Save album DataFrame to CSV file."""
        self._validate_and_save(self.album_df, filepath, "Album")
    
    def load_artist_data(self, filepath):
        """Save artist DataFrame to CSV file."""
        self._validate_and_save(self.artist_df, filepath, "Artist")

    def load_song_data(self, filepath):
        """Save song DataFrame to CSV file."""
        self._validate_and_save(self.song_df, filepath, "Song")
    
    def load_all(self, base_dir):
        """
        Save all DataFrames to specified directory.
        
        Args:
            base_dir (str): Directory to save all CSV files
        """
        self.load_album_data(os.path.join(base_dir, "albums.csv"))
        self.load_artist_data(os.path.join(base_dir, "artists.csv"))
        self.load_song_data(os.path.join(base_dir, "songs.csv"))
        logger.info(f"All data saved to {base_dir}")