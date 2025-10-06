from src.main.connectors.spotify_connector import SpotifyConnection
from src.main.connectors.s3_connector import S3Connection
from src.main.extractors.spotify_extractor import DataExtract
from src.main.transfomers.spotify_transformer import TransformData
from src.main.loaders.local_loader import LoadDataLocal
from src.main.loaders.s3_loader import LoadDataS3
from loguru import logger
import configparser

def main(data_load):
    logger.info("Starting ETL process...")
    # Load configuration
    config = configparser.ConfigParser()
    path='/Users/akshatsharma/Desktop/Portfolio_Projects/Spotify_Data_Pipeline/spotify-data-pipeline/.config.ini'
    config.read(path)

    # Connect to Spotify API
    api_conn = SpotifyConnection.get_instance(config)
    logger.info(f"API connection successful: {api_conn is not None}")
    
    playlist_id = config['playlist-id']['PLAYLIST_ID']
    playlist_data = api_conn.get_playlist_track(playlist_id)
    
    # Extract data
    data_extractor = DataExtract(playlist_data)
    album_data = data_extractor.extract_album_data()
    logger.info(f"Extracted album data: {len(album_data) if album_data else 0} albums")
    artist_data = data_extractor.extract_artist_data()
    logger.info(f"Extracted artist data: {len(artist_data) if artist_data else 0} artists")
    song_data = data_extractor.extract_song_data()
    logger.info(f"Extracted song data: {len(song_data) if song_data else 0} songs")
    
    # Transform data
    transformer = TransformData(album_data, artist_data, song_data)
    album_df = transformer.transform_album_data()
    artist_df = transformer.transform_artist_data()
    song_df = transformer.transform_song_data()
    logger.info("Data transformation completed.")

    if data_load == "s3":
        # Load data to S3
        s3_conn = S3Connection.get_instance(config)
        s3_loader = LoadDataS3(album_df, artist_df, song_df, s3_conn, config['aws-bucket']['bucket_name'])
        s3_loader.load_all_to_s3()
        logger.info("Data uploaded to S3 successfully.")
    else:
        data_load = "local"
        local_loader = LoadDataLocal(album_df, artist_df, song_df)
        local_loader.load_album_data(f"data/album_data_{playlist_id}.csv")
        local_loader.load_artist_data(f"data/artist_data_{playlist_id}.csv")
        local_loader.load_song_data(f"data/song_data_{playlist_id}.csv")
        logger.info("Data saved locally.")
    
    logger.info("ETL process completed successfully.")

if __name__ == "__main__":
    main(data_load="s3")


    

