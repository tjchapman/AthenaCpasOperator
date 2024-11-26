import logging
from os import getenv
from dotenv import load_dotenv
from boto3 import client, resource

logger = logging.getLogger(__name__)

FORMAT= "%(levelname)s\t%(asctime)s\t%(funcName)s\t%(message)s\t"

logging.basicConfig(level=logging.INFO,
                     format=FORMAT)


load_dotenv()

aws_access_key= getenv('AWS_ACCESS_KEY')
aws_secret_key= getenv('AWS_SECRET_KEY')
aws_region= getenv('AWS_REGION')

s3_client = client('s3', aws_access_key_id= aws_access_key, aws_secret_access_key=aws_secret_key)
s3_resource = resource('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)
athena_client = client('athena', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)
glue_client = client('glue',  aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)

