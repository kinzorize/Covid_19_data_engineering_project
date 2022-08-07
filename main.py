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

# Basically this function do is to takes the boto3 object,
# run some queries on athena and store the output in s3

Dict = {}


def download_and_load_query_results(
    client: boto3.client, query_response: Dict
) -> pd.DataFrame:
    while True:
        try:
            # This function only loads the first 1000 rows
            client.get_query_results(
                QueryExecutionId=query_response["QueryExecutionId"]
            )
            break
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(0.001)
            else:
                raise err
    temp_file_location: str = "athena_query_result.csv"
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,

    )
    s3_client.download_file(
        S3_BUCKET_NAME,
        f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
        temp_file_location,
    )
    return pd.read_csv(temp_file_location)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM enigma_jhud",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    },
)


response

enigma_jhud = download_and_load_query_results(athena_client, response)

enigma_jhud.head()


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM covid_19_testing_data_states_daily",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    },
)
covid_19_testing_data_states_daily = download_and_load_query_results(
    athena_client, response)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM covid_19_testing_data_us_total_latest",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    },
)
covid_19_testing_data_us_total_latest = download_and_load_query_results(
    athena_client, response)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM covid_19_testing_data_us_daily",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    },
)
covid_19_testing_data_us_daily = download_and_load_query_results(
    athena_client, response)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM nytimes_data_in_usa_us_county",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    },
)
nytimes_data_in_usa_us_county = download_and_load_query_results(
    athena_client, response)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM nytimes_data_in_usa_us_states",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    },
)
nytimes_data_in_usa_us_states = download_and_load_query_results(
    athena_client, response)

response = athena_client.start_query_execution(
    QueryString="SELECT * FROM static_datasets_countrycode",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    },
)
static_datasets_countrycode = download_and_load_query_results(
    athena_client, response)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM static_datasets_countypopulation",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    },
)
static_datasets_countypopulation = download_and_load_query_results(
    athena_client, response)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM static_datasets_state_abv",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    },
)
static_datasets_state_abv = download_and_load_query_results(
    athena_client, response)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM usa_hospital_beds",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    },
)
usa_hospital_beds = download_and_load_query_results(athena_client, response)
