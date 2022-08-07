import boto3
import pandas as pd
from io import StringIO

# connect to aws and s3
AWS_ACCESS_KEY = "AKIAXEHCL3424LRLZJED"
AWS_SECRET_KEY = "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"
AWS_REGION = "us-east-1"
SCHEMA_NAME = "covid_19"
S3_STAGING_DIR = "s3://covid-19-output-elijah/output/"
S3_BUCKET_NAME = "covid-19-output-elijah"
S3_OUTPUT_DIRECTORY = "output"

# connect to athena
athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)
