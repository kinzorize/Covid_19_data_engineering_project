import redshift_connector
import boto3
import time
import pandas as pd
from io import StringIO

# connect to aws and s3
AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
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

# grab the first role of the ender
new_header = static_datasets_state_abv.iloc[0]
# takes the data below the data header row
static_datasets_state_abv = static_datasets_state_abv[:1]
# Set the header row as the df header.
static_datasets_state_abv.column = new_header


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM usa_hospital_beds",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    },
)
usa_hospital_beds = download_and_load_query_results(athena_client, response)

factCovid_1 = enigma_jhud[['fips', 'province_state',
                           'country_region', 'confirmed', 'deaths', 'recovered', 'active']]
factCovid_2 = covid_19_testing_data_states_daily[[
    'fips', 'date', 'positive', 'negative', 'hospitalized', 'hospitalizedcurrently', 'hospitalizeddischarged']]

factCovid = pd.merge(factCovid_1, factCovid_2, on='fips', how='inner')


factCovid.shape

dimRegion_1 = enigma_jhud[['fips', 'province_state',
                           'country_region', 'latitude', 'longitude']]
dimRegion_2 = nytimes_data_in_usa_us_county[['fips', 'county', 'state']]
dimRegion = pd.merge(dimRegion_1, dimRegion_2, on='fips', how='inner')


dimHospital = usa_hospital_beds[['fips', 'state_name', 'latitude', 'longtitude',
                                 'hq_address', 'hospital_name', 'hospital_type', 'hq_city', 'hq_state']]
dimDate = [['fips', 'date']]

dimDate.head()

dimDate['date'] = pd.to_datetime(dimDate['date'], format='%Y%m%d')

dimDate.head()

dimDate['year'] = dimDate['date'].dt.year
dimDate['month'] = dimDate['date'].dt.month
dimDate['day_of_week'] = dimDate['date'].dt.dayofweek

dimDate.head()

bucket = 'elijah-covid-project'  # already created on s3

csv_buffer = StringIO()

factCovid.to_csv(csv_buffer)

s3_resource = boto3.resource('s3')
s3_resource.Object(
    bucket, 'output/factCovid.csv').put(Body=csv_buffer.getvalue())


csv_buffer = StringIO()
dimRegion.to_csv(csv_buffer)
s3_resource = boto3.resource('s3')
s3_resource.Object(
    bucket, 'output/dimRegion.csv').put(Body=csv_buffer.getvalue())

csv_buffer = StringIO()
dimHospital.to_csv(csv_buffer)
s3_resource = boto3.resource('s3')
s3_resource.Object(
    bucket, 'output/dimHospital.csv').put(Body=csv_buffer.getvalue())

csv_buffer = StringIO()
dimDate.to_csv(csv_buffer)
s3_resource = boto3.resource('s3')
s3_resource.Object(
    bucket, 'output/dimDate.csv').put(Body=csv_buffer.getvalue())


dimDatesql = pd.io.sql.get_schema(dimDate.reset_index(), 'dimDate')
print(''.join(dimDatesql))

factCovidsql = pd.io.sql.get_schema(factCovid.reset_index(), 'factCovid')
print(''.join(factCovidsql))

dimRegionsql = pd.io.sql.get_schema(dimRegion.reset_index(), 'dimRegion')
print(''.join(dimRegionsql))

dimHospitalsql = pd.io.sql.get_schema(dimHospital.reset_index(), 'dimHospital')
print(''.join(dimHospitalsql))


conn = redshift_connector.connect(
    host='redshift-cluster-1.cq86xufekc54.us-east-1.redshift.amazonaws.com',
    database='dev',
    user='awsuser',
    password=''
)


conn.autocommit = True

cursor = redshift_connector.Cursor = conn.cursor()

cursor.execute(""" 
CREATE TABLE "dimDate" (
"index" INTEGER,
  "fips" REAL,
  "date" TIMESTAMP,
  "year" INTEGER,
  "month" INTEGER,
  "day_of_week" INTEGER
)
""")

cursor.execute(""" 
CREATE TABLE "factCovid" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "confirmed" REAL,
  "deaths" REAL,
  "recovered" REAL,
  "active" REAL,
  "date" INTEGER,
  "positive" INTEGER,
  "negative" REAL,
  "hospitalized" REAL,
  "hospitalizedcurrently" REAL,
  "hospitalizeddischarged" REAL
)

""")

cursor.execute("""
CREATE TABLE "dimRegion" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "latitude" REAL,
  "longitude" REAL,
  "county" TEXT,
  "state" TEXT
)
""")

cursor.execute("""
CREATE TABLE "dimHospital" (
"index" INTEGER,
  "fips" REAL,
  "state_name" TEXT,
  "latitude" REAL,
  "longtitude" REAL,
  "hq_address" TEXT,
  "hospital_name" TEXT,
  "hospital_type" TEXT,
  "hq_city" TEXT,
  "hq_state" TEXT
)
""")


cursor.execute("""
copy dimDate from 's3://elijah-covid-project/output/dimDate.csv'
credentials 'aws_iam_role:arn:aws:iam::490101006133:role/redshift-s3-access'
delimiter ','
region 'us-east-1'
IGNOREHEADER 1

""")
# arn:aws:iam::490101006133:role/redshift-s3-access
