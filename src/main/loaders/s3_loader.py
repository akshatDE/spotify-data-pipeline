from datetime import datetime
from io import StringIO
from loguru import logger
import pandas as pd

class LoadDataS3:
    """
    Handles S3 persistence for transformed Spotify data.
    
    Saves cleaned DataFrames to S3 buckets as CSV files.
    Part of the ETL pipeline's Load stage for cloud storage.
    """

    def __init__(self, album_df=None, artist_df=None, song_df=None, s3_connector=None, bucket_name=None):
        """
        Initialize S3 loader with transformed DataFrames and S3 connection.
        
        Args:
            album_df (pd.DataFrame): Transformed album data
            artist_df (pd.DataFrame): Transformed artist data  
            song_df (pd.DataFrame): Transformed song data
            s3_connector (S3Connection): Instance of S3Connection class
            bucket_name (str): S3 bucket name for uploads
        """
        self.album_df = album_df
        self.artist_df = artist_df
        self.song_df = song_df
        self.s3_client = s3_connector.s3_client
        self.bucket_name = bucket_name

    def _validate_and_upload(self, df, s3_key, data_type):
        """
        Private helper to validate DataFrame and upload to S3.
        
        Args:
            df (pd.DataFrame): DataFrame to upload
            s3_key (str): S3 object key (path)
            data_type (str): Type of data (for logging)
        
        Returns:
            str: S3 URI of uploaded file
        
        Raises:
            ValueError: If DataFrame is None or empty
            Exception: If S3 upload fails
        """
        # Validate DataFrame
        if df is None or df.empty:
            logger.error(f"{data_type} data is None or empty")
            raise ValueError(f"No {data_type} data to upload")
        
        # Convert DataFrame to CSV in memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        try:
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            
            s3_uri = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"{data_type} data uploaded to {s3_uri} with shape {df.shape}")
            return s3_uri
            
        except Exception as e:
            logger.error(f"Failed to upload {data_type} data to S3: {e}")
            raise

    def load_album_data_to_s3(self, s3_prefix="processed-data/spotify"):
        """
        Upload album DataFrame to S3.
        
        Args:
            s3_prefix (str): S3 key prefix for organization
            
        Returns:
            str: S3 URI of uploaded file
        """
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        s3_key = f"{s3_prefix}/albums/albums_{timestamp}.csv"
        return self._validate_and_upload(self.album_df, s3_key, "Album")

    def load_artist_data_to_s3(self, s3_prefix="processed-data/spotify"):
        """Upload artist DataFrame to S3."""
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        s3_key = f"{s3_prefix}/artists/artists_{timestamp}.csv"
        return self._validate_and_upload(self.artist_df, s3_key, "Artist")

    def load_song_data_to_s3(self, s3_prefix="processed-data/spotify"):
        """Upload song DataFrame to S3."""
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        s3_key = f"{s3_prefix}/songs/songs_{timestamp}.csv"
        return self._validate_and_upload(self.song_df, s3_key, "Song")
    
    def load_all_to_s3(self, s3_prefix="processed-data/spotify"):
        """
        Upload all DataFrames to S3 in one call.
        
        Returns:
            dict: S3 URIs for all uploaded files
        """
        uploaded_files = {
            'albums': self.load_album_data_to_s3(s3_prefix),
            'artists': self.load_artist_data_to_s3(s3_prefix),
            'songs': self.load_song_data_to_s3(s3_prefix)
        }
        logger.info(f"All data uploaded to S3 under {s3_prefix}")
        return uploaded_files