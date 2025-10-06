import mysql.connector
from mysql.connector import Error
from loguru import logger

class MySqlConnection:
    """
    Singleton class to manage MySQL database connection.
    """
    _instance = None

    def __init__(self, config):
        if MySqlConnection._instance is not None:
            raise Exception("Use get_instance() instead of creating a new object")
        self.config = config
        self.connection = None
        self.connect_to_database()
    
    @classmethod
    def get_instance(cls, config=None):
        if cls._instance is None:
            cls._instance = cls(config)
        return cls._instance
    
    def connect_to_database(self):
        """
        Private method to create a MySQL database connection.
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.config['mysql-auth']['HOST'],
                user=self.config['mysql-auth']['USER'],
                password=self.config['mysql-auth']['PASSWORD'],
                database=self.config['mysql-auth']['DATABASE']
            )
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            raise

    def close(self):
        """
        Close the MySQL database connection.
        """
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("MySQL connection closed.")

# Check for connection
if __name__ == "__main__":
    import configparser

    config = configparser.ConfigParser()
    path = '/Users/akshatsharma/Desktop/Portfolio_Projects/Spotify_Data_Pipeline/spotifyETL/resources/config.ini'
    config.read(path)

    try:
        db_conn = MySqlConnection.get_instance(config)
        if db_conn.connection.is_connected():
            logger.info("MySQL connection is successful.")
        db_conn.close()
        
    except Exception as e:
        logger.error(f"Failed to connect to MySQL: {e}")