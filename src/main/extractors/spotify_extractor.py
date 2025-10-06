from loguru import logger

class DataExtract:
    """
    Handles extraction of structured data from Spotify playlist JSON responses.
    
    This class provides methods to parse Spotify API playlist data and extract
    information about albums, artists, and songs into structured dictionaries.
    It handles common data quality issues like None values and missing fields.
    
    Attributes:
        playlist_data (dict): Raw Spotify playlist data from API response
    """
    
    def __init__(self, playlist_data):
        """
        Initialize the DataExtract class with playlist data.
        
        Args:
            playlist_data (dict): Spotify playlist data containing 'items' array
                                with track information from the Spotify Web API
        """
        self.playlist_data = playlist_data
        
    def extract_album_data(self):
        """
        Extract album information from playlist data.
        
        Parses through all tracks in the playlist and extracts album metadata
        including ID, name, release date, track count, and Spotify URL.
        
        Args:
            playlist_id (str): Spotify playlist identifier (for logging purposes)
            
        Returns:
            list[dict]: List of album dictionaries with keys:
                - id: Album Spotify ID
                - name: Album title
                - release_date: Album release date string
                - total_tracks: Number of tracks on album
                - url: Spotify web URL for album
                
        Raises:
            Exception: Logs any parsing errors and continues processing
        """
        try:
            album_list = []
            for data in self.playlist_data['items']:
                if data['track'] is None:  # Handle None tracks
                    continue
                album_id = data['track']['album']['id']
                album_name = data['track']['album']['name']
                album_release_date = data['track']['album']['release_date']
                album_total_tracks = data['track']['album']['total_tracks']
                album_url = data['track']['album']['external_urls']['spotify']

                album_dict = {
                    'id': album_id,
                    'name': album_name,
                    'release_date': album_release_date,
                    'total_tracks': album_total_tracks,
                    'url': album_url
                }
                album_list.append(album_dict)
            logger.info(f"Data Extraction for album Succesfull with {len(album_list)} ")
            return album_list
        except Exception as e:
            logger.info(f"Got Some error with exceptin {e}")

    def extract_artist_data(self):
        """
        Extract artist information from playlist data.
        
        Iterates through all tracks and their associated artists to build
        a comprehensive list of artist metadata. Handles tracks with multiple
        artists by creating separate entries for each.
        
        Args:
            playlist_id (str): Spotify playlist identifier (for logging purposes)
            
        Returns:
            list[dict]: List of artist dictionaries with keys:
                - id: Artist Spotify ID  
                - name: Artist name
                - url: Spotify web URL for artist profile
                
        Raises:
            Exception: Logs any parsing errors and continues processing
        """
        try:

            artist_list = []
            for data in self.playlist_data['items']:
                if data['track'] is None:  # Handle None tracks
                    continue
                    
                for artist in data['track']['artists']:
                    artist_id = artist['id']
                    artist_name = artist['name']
                    artist_url = artist['external_urls']['spotify']
                    artist_dict = {
                        'id': artist_id,
                        'name': artist_name,
                        'url': artist_url
                    }
                    artist_list.append(artist_dict)
            logger.info(f"Data Extraction for arist Succesfull with {len(artist_list)} ")
            return artist_list
        
        except Exception as e:
            logger.info(f"Got some error with exception {e}")

    def extract_song_data(self, playlist_data):
        """
        Extract song/track information from playlist data.
        
        Processes each track in the playlist to extract comprehensive song
        metadata including playback information, popularity metrics, and 
        relationship data to albums and artists.
        
        Args:
            playlist_data: Raw playlist data (parameter not used, uses self.playlist_data)
            
        Returns:
            list[dict]: List of song dictionaries with keys:
                - id: Track Spotify ID
                - name: Track title
                - added_at: Timestamp when track was added to playlist
                - duration_ms: Track duration in milliseconds
                - popularity: Spotify popularity score (0-100)
                - url: Spotify web URL for track
                - album_id: Associated album Spotify ID
                - artist_ids: Primary artist Spotify ID
                
        Raises:
            Exception: Logs any parsing errors and continues processing
        """
        try:

            song_list = []
            for data in self.playlist_data['items']:
                if data['track'] is None:  # Handle None tracks
                    continue
                    
                song_id = data['track']['id']
                song_name = data['track']['name']
                song_duration_ms = data['track']['duration_ms']
                song_popularity = data['track']['popularity']
                song_url = data['track']['external_urls']['spotify']
                song_added_at = data['added_at']
                album_id = data['track']['album']['id']
                artist_ids = data['track']['album']['artists'][0]['id']

                song_dict = {
                    'id': song_id,
                    'name': song_name,
                    'added_at': song_added_at,
                    'duration_ms': song_duration_ms,
                    'popularity': song_popularity,
                    'url': song_url,
                    'album_id': album_id,
                    'artist_ids': artist_ids
                }
                song_list.append(song_dict)
            logger.info(f"Data Extraction for Songs Succesfull with {len(song_list)} ")
            return song_list
        
        except Exception as e:
            logger.info(f"Got some Error with exception {e}")
    



