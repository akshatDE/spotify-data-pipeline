import spotipy
from loguru import logger
from spotipy.oauth2 import SpotifyClientCredentials

class SpotifyConnection:
    _instance = None

    def __init__(self,config):
        if SpotifyConnection._instance is not None:
            raise Exception("Use get_instance() instead of creating a new object")
        self.config = config
        self.spotify_client = None
        self.connectAPI()
    
    @classmethod
    def get_instance(cls,config=None):
        if cls._instance is None:
            cls._instance = cls(config)
        return cls._instance
    
    def connectAPI(self):
        try:
            credentials = SpotifyClientCredentials(
            client_id=self.config["spotify-auth"]["CLIENT_ID"],
            client_secret=self.config["spotify-auth"]["CLIENT_SECRET_KEY"]
            )

            self.spotify_client = spotipy.Spotify(client_credentials_manager=credentials)
            logger.info(f"Connected and object created as {self.spotify_client}")

        except Exception as e:
            logger.info(f"API connection got some Error {e}")

    def get_playlist_track(self,playlist_id):
        if self.spotify_client is None:
            raise Exception("Spotify client not initialized")
        
        try:
            return self.spotify_client.playlist_tracks(playlist_id)
        except Exception as e:
            logger.error(f"Failed to get playlist tracks: {e}")
            raise