import boto3
from loguru import logger

class S3Connection:
    _instance = None

    def __init__(self,config):
        if S3Connection._instance is not None:
            raise Exception("Use get_instance() instead of creating a new object")
        self.config = config
        self.s3_client = None
        self.connect_to_s3()
    
    @classmethod
    def get_instance(cls,config=None):
        if cls._instance is None:
            cls._instance = cls(config)
        return cls._instance
    
    def connect_to_s3(self):
        """Private method to create S3 client connection"""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.config['aws-auth']['aws_access_key_id'],
                aws_secret_access_key=self.config['aws-auth']['aws_secret_access_key'],
                region_name=self.config['aws-auth']['region_name']
            )
            logger.info("Connected to AWS S3 successfully.")
            return self.s3_client
        except Exception as e:
            logger.error(f"Failed to connect to S3: {e}")
            raise