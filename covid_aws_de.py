# import required packages
import boto3
import pandas as pd
import requests
import time
import json
import redshift_connector
from decouple import config
from io import StringIO
from inspect import cleandoc

# Set Variables

# AWS ACCESS
PROFILE = config("PROFILE")
AWS_ACCESS_KEY_ID = config("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config("AWS_SECRET_ACCESS_KEY")
AWS_REGION_NAME = config("AWS_REGION_NAME")
# S3 BUCKETS
S3_STAGING_PATH = config("S3_STAGING_PATH")
S3_OUTPUT_PATH = config("S3_OUTPUT_PATH")
S3_BUCKET_NAME = config("S3_BUCKET_NAME")
S3_STAGING_DIR = config("S3_STAGING_DIR")
S3_OUTPUT_DIR = config("S3_OUTPUT_DIR")
S3_SCRIPTS_DIR = config("S3_SCRIPTS_DIR")
EXT_PKG_DIR = config("EXT_PKG_DIR")
# GLUE
GLUE_IAM_ROLE = config("GLUE_IAM_ROLE")
GLUE_DB = config("GLUE_DB")
GLUE_ETL_JOB = config("GLUE_ETL_JOB")
# REDSHIFT
DWH_CLUSTER_TYPE = config("DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config("DWH_NUM_NODES")
DWH_NODE_TYPE = config("DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config("DWH_CLUSTER_IDENTIFIER")
DWH_DB = config("DWH_DB")
DWH_DB_USER = config("DWH_DB_USER")
DWH_DB_PASSWORD = config("DWH_DB_PASSWORD")
DWH_PORT = config("DWH_PORT")
DWH_IAM_ROLE_NAME = config("DWH_IAM_ROLE_NAME")

bsession = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION_NAME,
)


# Establish Client/Service Connections
athena_client = bsession.client("athena")
ec2_resource = bsession.resource("ec2")
glue_client = bsession.client("glue")
iam_client = bsession.client("iam")
redshift_client = bsession.client("redshift")
s3_client = bsession.client("s3")
s3_resource = bsession.resource("s3")


# Create IAM roles

# Function to initiate role creation for service
def create_role_trust_policy(service):
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": f"{service}.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    return trust_policy


# Glue
try:
    print("Creating Glue IAM Role...")
    iam_response = iam_client.create_role(RoleName=GLUE_IAM_ROLE,
                                          AssumeRolePolicyDocument=json.dumps(create_role_trust_policy(service="glue")))
    # Attach Policy
    print(f"Attaching Policies to {GLUE_IAM_ROLE}...")
    iam_client.attach_role_policy(RoleName=GLUE_IAM_ROLE,
                                  PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess")
    iam_client.attach_role_policy(RoleName=GLUE_IAM_ROLE,
                                  PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole")
    iam_client.attach_role_policy(RoleName=GLUE_IAM_ROLE,
                                  PolicyArn="arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess")
    # Get and print the IAM role ARN
    print(f"Getting {GLUE_IAM_ROLE} IAM role ARN...")
    glue_roleArn = iam_client.get_role(RoleName=GLUE_IAM_ROLE)['Role']['Arn']
    print(f"{GLUE_IAM_ROLE} IAM Role ARN = {glue_roleArn}")
except Exception as e:
    print(e)

# Redshift
try:
    print("Creating Redshift IAM Role...")
    iam_response = iam_client.create_role(RoleName=DWH_IAM_ROLE_NAME,
                                          AssumeRolePolicyDocument=json.dumps(
                                              create_role_trust_policy(service="redshift")
                                          )
                                          )
    # Attach Policy
    print(f"Attaching Policies to {DWH_IAM_ROLE_NAME}...")
    iam_client.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                                  PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess")
    # Get and print the IAM role ARN
    print(f"Getting {DWH_IAM_ROLE_NAME} IAM role ARN...")
    redshift_roleArn = iam_client.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print(f"{DWH_IAM_ROLE_NAME} IAM Role ARN = {redshift_roleArn}")
except Exception as e:
    print(e)


# Function to get dowload file from URL
# and upload to s3 bucket
def url_download_upload(bucket, output_dir, file, url):
    # Uses the creds in ~/.aws/credentials
    s3 = bsession.resource('s3')
    # Do this as a quick and easy check to make sure your S3 access is OK
    if output_dir in [obj.key for obj in s3.Bucket(bucket).objects.all()]:
        print('Found the upload directory.')
        # Given an Internet-accessible URL, download the image and upload it to S3,
        # without needing to persist the image to disk locally

        # Do the actual upload to s3
        try:
            t0 = time.time()
            with requests.get(url, stream=True) as r:
                print(f"uploading {file} to {output_dir} in {bucket}...")
                s3.Object(bucket, f"{output_dir}{file}").put(Body=r.content)
            t1 = time.time()
            texec = f"[{round(t1 - t0, 2)}s]"
            print(f"{bucket}/{output_dir}{file} upload SUCCESSFUL.  {texec : >30}]s")
        except Exception as e:
            print(e)
    else:
        print('Not seeing your s3 bucket, might want to double check permissions in IAM')


def upload_local_file(file, bucket, output_dir):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    s3 = bsession.resource('s3')
    # Do this as a quick and easy check to make sure your S3 access is OK
    if output_dir in [obj.key for obj in s3.Bucket(bucket).objects.all()]:
        print('Found the upload directory.')

        # Upload the file
        try:
            t0 = time.time()
            print(f"uploading _url {file} to {output_dir} in {bucket}...")
            s3.Object(bucket, f"{output_dir}{file}").upload_file(file)
            t1 = time.time()
            texec = f"[{round(t1 - t0, 2)}s]"
            print(f"{bucket}/{output_dir}{file} upload SUCCESSFUL.  {texec : >30}")
        except Exception as e:
            print(e)
    else:
        print('Not seeing your s3 bucket, might want to double check permissions in IAM')


def download_to_local(bucket, s3path, lpath):
    t0 = time.time()
    s3_client.download_file(
        bucket,
        f"{s3path}",
        lpath,
    )
    t1 = time.time()
    texec = f"[{round(t1-t0, 2)}s]"
    print(f"{bucket}/{s3path} downloaded. {texec : >30}")


# Create Project Bucket and Folder Structure
# Create Bucket
s3_client.create_bucket(
    Bucket=S3_BUCKET_NAME,
    CreateBucketConfiguration={"LocationConstraint": AWS_REGION_NAME},
    PublicAccessBlockConfiguration={
        'BlockPublicAcls': True,
        'IgnorePublicAcls': True,
        'BlockPublicPolicy': True,
        'RestrictPublicBuckets': True
    },
)

# Create Folders
folder_list = [
    'athena_output/', 'enigma-jhu/', 'enigma-nytimes-data-in-usa/',
    'output/', 'packages/', 'rearc-covid-19-testing-data/', 'rearc-usa-hospital-beds/',
    'staging/', 'static-datasets/', 'enigma-nytimes-data-in-usa/us_county/',
    'enigma-nytimes-data-in-usa/us_states/', 'rearc-covid-19-testing-data/us_daily/',
    'rearc-covid-19-testing-data/states_daily/', 'static-datasets/countrycode/',
    'static-datasets/CountyPopulation/', 'static-datasets/state-abv/'
]
for folder in folder_list:
    if folder not in [obj.key for obj in s3_resource.Bucket(S3_BUCKET_NAME).objects.all()]:
        print(f"Creating directory {folder} in bucket {S3_BUCKET_NAME}...")
        t0 = time.time()
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=folder)
        t1 = time.time()
        texec = f"[{round(t1-t0, 2)}s]"
        print(f"{folder} CREATED.  {time : >30}")
    else:
        print(f"Directory {folder} already exists\t SKIPPING.")

# Download files from Data Lake
dl_file_list = [
    "https://covid19-lake.s3.us-east-2.amazonaws.com/enigma-jhu/csv/Enigma-JHU.csv.gz",
    "https://covid19-lake.s3.us-east-2.amazonaws.com/enigma-nytimes-data-in-usa/csv/us_county/us_county.csv",
    "https://covid19-lake.s3.us-east-2.amazonaws.com/enigma-nytimes-data-in-usa/csv/us_states/us_states.csv",
    "https://covid19-lake.s3.us-east-2.amazonaws.com/rearc-covid-19-testing-data/csv/states_daily/states_daily.csv",
    "https://covid19-lake.s3.us-east-2.amazonaws.com/rearc-covid-19-testing-data/csv/us_daily/us_daily.csv",
    "https://covid19-lake.s3.us-east-2.amazonaws.com/rearc-usa-hospital-beds/json/usa-hospital-beds.geojson",
    "https://covid19-lake.s3.us-east-2.amazonaws.com/static-datasets/csv/countrycode/CountryCodeQS.csv",
    "https://covid19-lake.s3.us-east-2.amazonaws.com/static-datasets/csv/CountyPopulation/County_Population.csv",
    "https://covid19-lake.s3.us-east-2.amazonaws.com/static-datasets/csv/state-abv/states_abv.csv"
]
for file_url in dl_file_list:
    if len(file_url.split("/")) > 6:
        upload_loc = f"{file_url.split('/')[3]}/{file_url.split('/')[-2]}/"
    else:
        upload_loc = f"{file_url.split('/')[3]}/"
    filename = file_url.split("/")[-1]
    url_download_upload(S3_BUCKET_NAME, upload_loc, filename, file_url)


# Create Glue DB
dbName = GLUE_DB
glue_client.create_database(DatabaseInput={'Name': dbName})

# Function to create crawlers without having
# to repeat the code


def my_s3_create_crawler(client, name, role, source, db, prefix='', description=''):
    try:
        if type(source) is list:
            source = [{'Path': path, 'Exclusions': []} for path in source]
        elif type(source) is str:
            source = [{'Path': source, 'Exclusions': []}]
        targets = {'S3Targets': source}
    except Exception as e:
        print(e)

    client.create_crawler(
        Name=name,
        Role=role,
        Targets=targets,
        DatabaseName=db,
        Description=description,
        Classifiers=[],
        RecrawlPolicy={'RecrawlBehavior': 'CRAWL_EVERYTHING'},
        SchemaChangePolicy={'UpdateBehavior': 'UPDATE_IN_DATABASE',
                            'DeleteBehavior': 'DEPRECATE_IN_DATABASE'},
        LineageConfiguration={'CrawlerLineageSettings': 'DISABLE'},
        TablePrefix=prefix,
        LakeFormationConfiguration={'UseLakeFormationCredentials': False,
                                    'AccountId': ''}
    )


# Create Glue Crawlers
my_s3_create_crawler(glue_client, 'enigma_jhu_crawl', GLUE_IAM_ROLE,
                     f"s3://{S3_BUCKET_NAME}/enigma-jhu",
                     'covid19_db', prefix='', description='enigma-jhu directory')
my_s3_create_crawler(glue_client, 'enigma_nytimes_crawl', GLUE_IAM_ROLE,
                     [f"s3://{S3_BUCKET_NAME}/enigma-nytimes-data-in-usa/us_county",
                      f"s3://{S3_BUCKET_NAME}/enigma-nytimes-data-in-usa/us_states"],
                     'covid19_db', prefix='nytimes_data_', description='nytimes data')
my_s3_create_crawler(glue_client, 'rearc_beds_crawl', GLUE_IAM_ROLE,
                     f"s3://{S3_BUCKET_NAME}/rearc-usa-hospital-beds",
                     'covid19_db', prefix='', description='hospital beds data')
my_s3_create_crawler(glue_client, 'rearc_testing_crawl', GLUE_IAM_ROLE,
                     f"s3://{S3_BUCKET_NAME}/rearc-covid-19-testing-data/states_daily",
                     'covid19_db', prefix='rearc_testing_', description='rearc testing data')
my_s3_create_crawler(glue_client, 'static_data_crawl', GLUE_IAM_ROLE,
                     [f"s3://{S3_BUCKET_NAME}/static-datasets/CountyPopulation",
                      f"s3://{S3_BUCKET_NAME}/static-datasets/countrycode",
                      f"s3://{S3_BUCKET_NAME}/static-datasets/state-abv"],
                     'covid19_db', prefix='d_static_', description='static lookup data')


crawlers = glue_client.list_crawlers()['CrawlerNames']

# Run Crawlers
t0 = time.time()
for crawler in crawlers:
    glue_client.start_crawler(Name=f"{crawler}")
    t1 = time.time()
    print(f"Running {crawler}...")
    exit_v = 0
    while not (exit_v):
        response = glue_client.get_crawler(Name=f"{crawler}")
        if response['Crawler']['State'] == 'STOPPING':
            t2 = time.time()
            texec = f"[{round(t2-t1, 2)}]s"
            print(f"{response['Crawler']['Name']} COMPLETE"
                  f"crawler is currently {response['Crawler']['State']}. {texec : >30}")
            print("Waiting for crawler to stop...")
            time.sleep(60)
            exit_v = 1
t3 = time.time()
texec = f"[{round(t3-t0, 2)}min]"
print(f"All crawlers COMPLETE. DB tables creates.  {texec : >30}")

# Get List of tables in database
db_tables = [table['Name'] for table in glue_client.get_tables(
    DatabaseName=GLUE_DB, NextToken='', MaxResults=11)['TableList']]

# TODO possible implement awswrangler
# or build class see
# https://stackoverflow.com/questions/52026405/how-to-create-dataframe-from-aws-athena-using-boto3-get-query-results-method

Dict = {}


def download_and_load_query_results(
    client: boto3.client, query_response: Dict
) -> pd.DataFrame:
    t0 = time.time()
    print("Getting query results...")
    while True:
        try:
            # This function only loads the first 1000 rows
            client.get_query_results(
                QueryExecutionId=query_response["QueryExecutionId"]
            )
            break
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(0.1)
            else:
                raise err
    temp_file_location: str = "athena_query_results.csv"
    s3_client.download_file(
        S3_BUCKET_NAME,
        f"{S3_STAGING_DIR}{query_response['QueryExecutionId']}.csv",
        temp_file_location,
    )
    t1 = time.time()
    texec = f"[{round(t1-t0, 2)}s]"
    print(f"Query results downloaded. {texec : >30}")
    return pd.read_csv(temp_file_location)


# Execute table query
def get_query_response(table, database, output_location):
    print(f"Running Query for {table}...")
    t0 = time.time()
    response = athena_client.start_query_execution(
        QueryString=f"SELECT * FROM {table}",
        QueryExecutionContext={"Database": database},
        ResultConfiguration={
            "OutputLocation": f"{output_location}",
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )
    t1 = time.time()
    texec = f"[{round(t1-t0, 2)}s]"
    print(f"{table} query COMPLETE.  {texec : >30}")
    return response


# Create dataframes for each table in database
d_static_countrycode, d_static_countypopulation, d_static_state_abv, \
    enigma_jhu, nytimes_data_us_county, nytimes_data_us_states, \
    rearc_testing_states_daily, rearc_usa_hospital_beds = [download_and_load_query_results(
        athena_client, get_query_response(table, GLUE_DB, S3_STAGING_PATH)) for table in db_tables]

# Trasnform data for data model
d_static_state_abv.rename(columns=d_static_state_abv.iloc[0], inplace=True)
d_static_state_abv.drop([0], inplace=True)
d_static_state_abv.head()

print("Creating DWH dim_region table...")
t0 = time.time()
dim_region1 = enigma_jhu[['fips', 'province_state', 'country_region',
                          'latitude', 'longitude']][enigma_jhu['province_state'] != 'Grand Princess']
dim_region1 = dim_region1.dropna(subset=['fips'])
dim_region2 = nytimes_data_us_county[['fips', 'county']]
dim_region = pd.merge(dim_region1, dim_region2, on='fips', how='inner')
dim_region = dim_region.dropna(subset=['fips', 'latitude', 'longitude'])
dim_region.drop_duplicates(inplace=True)
dim_region.reset_index(drop=True, inplace=True)
dim_region['fips'] = dim_region['fips'].astype(int)
dim_region['fips'] = dim_region['fips'].astype(str).str.zfill(5)
dim_region['state_fips'] = dim_region['fips'].str[:2]
dim_region['county_fips'] = dim_region['fips'].str[-3:]
dim_region.loc[dim_region['state_fips'] == '00', 'state_fips'] = '72'
dim_region = dim_region.sort_values(by=['state_fips', 'county_fips']).reset_index(drop=True)
dim_region['region_sk'] = dim_region.reset_index()['index']
dim_region = dim_region.dropna(subset=['region_sk']).reset_index(drop=True)
dim_region['region_sk'] = dim_region['region_sk'].astype(int) + 1
dim_region = dim_region[['region_sk', 'fips', 'state_fips', 'county_fips',
                         'province_state', 'county', 'country_region', 'latitude', 'longitude']]
t1 = time.time()
texec = f"[{round(t1-t0, 2)}s]"
print(f"dim_region COMPLETE. {texec : >30}")


print("Creating DWH dim_hospital table...")
t0 = time.time()
dim_hospital = rearc_usa_hospital_beds[['fips', 'state_name', 'county_name', 'latitude', 'longtitude',
                                        'hospital_name', 'hq_address', 'hq_city', 'hq_state',
                                        'hq_zip_code', 'hospital_type']].dropna(
                                            subset=['fips', 'state_name']).reset_index(drop=True)
dim_hospital.fips = dim_hospital.fips.astype(int)
dim_hospital.fips = dim_hospital.fips.astype(str).str.zfill(5)
dim_hospital['state_fips'] = dim_hospital['fips'].astype(str).str[:2]
dim_hospital['county_fips'] = dim_hospital['fips'].astype(str).str[-3:]
dim_hospital.hq_zip_code = dim_hospital.hq_zip_code.astype(str).str.zfill(5)
dim_hospital = dim_hospital.sort_values(by=['state_fips', 'county_fips']).reset_index(drop=True)
dim_hospital['hosp_sk'] = dim_hospital.reset_index()['index']
dim_hospital['hosp_sk'] = dim_hospital['hosp_sk'].astype(int) + 1
dim_hospital = dim_hospital[['hosp_sk', 'fips', 'state_fips', 'county_fips', 'state_name',
                             'county_name', 'hospital_name', 'hq_address', 'hq_city',
                             'hq_state', 'hq_zip_code', 'hospital_type', 'latitude', 'longtitude', ]]
t1 = time.time()
texec = f"[{round(t1-t0, 2)}s]"
print(f"dim_hospital COMPLETE. {texec : >30}")

# Create date_dim calendar table
# This table could be passed an indefinite end date


def create_date_dim(start, end):
    print("Creating DWH dim_date table...")
    t0 = time.time()
    df = pd.DataFrame({"date": pd.date_range(start, end)})
    df["day_name"] = df.date.dt.day_name()
    df["day_of_week"] = df.date.dt.day_of_week + 1  # Start Monday at 1
    df["day"] = df.date.dt.day
    df["day_of_year"] = df.date.dt.day_of_year
    df["month"] = df.date.dt.month
    df["month_name"] = df.date.dt.month_name()
    df["week"] = df.date.dt.isocalendar().week
    df["quarter"] = df.date.dt.quarter
    df["year"] = df.date.dt.year
    df["year_half"] = df.date.dt.month.map(lambda mo: 1 if mo < 7 else 2)
    df["is_weekend"] = df.date.dt.weekday >= 5
    df.insert(0, 'date_id', (df.year.astype(str) + df.month.astype(str).str.zfill(2)
                             + df.day.astype(str).str.zfill(2)).astype(int))
    t1 = time.time()
    texec = f"[{round(t1-t0, 2)}s]"
    print(f"dim_date COMPLETE.  {texec : >30}")
    return df


dim_date = create_date_dim(pd.to_datetime(rearc_testing_states_daily['date'], format='%Y%m%d').min(),
                           pd.to_datetime(rearc_testing_states_daily['date'], format='%Y%m%d').max())

print("Creating DWH fact_covid table...")
t0 = time.time()
fact_covid1 = rearc_testing_states_daily[['fips', 'date', 'positive', 'negative', 'hospitalized', 'state',
                                          'hospitalizedcurrently', 'hospitalizeddischarged', 'hospitalizedcumulative',
                                          'death', 'recovered', 'deathincrease', 'hospitalizedincrease',
                                          'positiveincrease']]
fact_covid1 = fact_covid1.dropna(subset=['fips', 'state']).reset_index(drop=True)
fact_covid1['fips'] = fact_covid1['fips'].astype(int)
fact_covid1['fips'] = fact_covid1['fips'].astype(str).str.zfill(2)
fact_covid1['state_fips'] = fact_covid1['fips']
fact_covid1.drop(['fips'], axis=1, inplace=True)
fact_covid1 = fact_covid1.sort_values(by=['state_fips'])
fact_covid1.set_index(['state_fips'], inplace=True)

fact_covid2 = dim_region[['state_fips', 'region_sk']]
fact_covid2 = fact_covid2.sort_values(by=['state_fips', 'region_sk']).reset_index(drop=True)
fact_covid2.set_index(['state_fips'], inplace=True)
fact_covid2 = fact_covid2.groupby(fact_covid2.index).first()

fact_covid3 = dim_hospital[['state_fips', 'hosp_sk']]
fact_covid3 = fact_covid3.sort_values(by=['state_fips', 'hosp_sk']).reset_index(drop=True)
fact_covid3.set_index(['state_fips'], inplace=True)
fact_covid3 = fact_covid3.groupby(fact_covid3.index).first()

fact_covid4 = pd.merge(fact_covid1, fact_covid2, how='inner', left_index=True, right_index=True)
fact_covid = pd.merge(fact_covid3, fact_covid4, how='inner', left_index=True, right_index=True)
fact_covid.reset_index(inplace=True)
fact_covid.fillna(0, inplace=True)
fact_covid = fact_covid[['date', 'state_fips', 'state', 'positive', 'positiveincrease', 'negative',
                         'death', 'deathincrease', 'recovered', 'hospitalized', 'hospitalizedcurrently',
                         'hospitalizeddischarged', 'hospitalizedcumulative', 'hospitalizedincrease',
                         'region_sk', 'hosp_sk']]
t1 = time.time()
texec = f"[{round(t1-t0, 2)}s]"
print(f"fact_covid COMPLETE. {texec : >30}")


# Function to get df name string
def get_df_name(df):
    name = [x for x in globals() if globals()[x] is df][0]
    return name


def upload_transform_csv(df, ind, bucket, output_loc):
    buffer = StringIO()
    filename = f"{get_df_name(df)}.csv"
    location = f"{output_loc}{filename}"
    print(f"Coverting dataframe {filename.split('.')[0]} to csv...")
    t0 = time.time()
    # Save transformed table data to S3
    # create StringIO to convert data into binary
    df.to_csv(buffer, index=ind)
    t1 = time.time()
    texec = f"[{round(t1-t0, 2)}s]"
    print(f"Conversion COMPLETE. {texec : >30}")
    print(f"uploading {bucket}/{location}.....")
    t2 = time.time()
    s3_resource.Object(bucket, location).put(Body=buffer.getvalue())
    t3 = time.time()
    texec = f"[{round(t3-t2, 2)}s]"
    print(f"{bucket}/{location} upload complete.  {texec : >30}")


# upload new tables to s3
for df, ind in [(fact_covid, False), (dim_date, False), (dim_hospital, False), (dim_region, False)]:
    upload_transform_csv(df, ind, S3_BUCKET_NAME, S3_OUTPUT_DIR)


def create_schema_sqls(df_list):
    sqls = {}
    for df in df_list:
        t0 = time.time()
        k = f"{get_df_name(df)}_sql"
        dfname = k.split('_sql')[0]
        print(f"Creating DDL for {dfname}...")
        v = pd.io.sql.get_schema(df.reset_index(), dfname)
        sqls[k] = v
        t1 = time.time()
        texec = f"[{round(t1-t0, 2)}s]"
        print(f"{dfname} DDL created. {texec : >30}")
    return sqls


# Used to create DDL Statements
sql_dict = create_schema_sqls([fact_covid, dim_date, dim_hospital, dim_region])


# Download needed wheel pkg
filename = 'redshift_connector-2.0.909-py3-none-any.whl'
file_url = ('https://files.pythonhosted.org/packages/24/3c/'
            '471f5f7d43f1ed1be87494010f466849fe2376acf8bab49d4b676f870cf1/redshift_connector-2.0.909-py3-none-any.whl')
url_download_upload(S3_BUCKET_NAME, EXT_PKG_DIR, filename, file_url)

filename = 'boto3-1.26.23-py3-none-any.whl'
file_url = ('https://files.pythonhosted.org/packages/67/03/'
            '0e794cf0621ce8c3ee780bb5fdeeffeefc095dd1f7f264b1f434db207063/boto3-1.26.23-py3-none-any.whl')
url_download_upload(S3_BUCKET_NAME, EXT_PKG_DIR, filename, file_url)

filename = 's3transfer-0.6.0-py3-none-any.whl'
file_url = ('https://files.pythonhosted.org/packages/5e/c6/'
            'af903b5fab3f9b5b1e883f49a770066314c6dcceb589cf938d48c89556c1/s3transfer-0.6.0-py3-none-any.whl')
url_download_upload(S3_BUCKET_NAME, EXT_PKG_DIR, filename, file_url)

filename = 'botocore-1.29.23-py3-none-any.whl'
file_url = ('https://files.pythonhosted.org/packages/19/eb/'
            '1068bdad2424f509b5700ace7d8adb3b97595618000ec9bc1c3bd4224c98/botocore-1.29.23-py3-none-any.whl')
url_download_upload(S3_BUCKET_NAME, EXT_PKG_DIR, filename, file_url)

filename = 'awscli-1.27.23-py3-none-any.whl'
file_url = ('https://files.pythonhosted.org/packages/f2/2a/'
            'e199d0cfb949a6a1710c8b9ead4d0238690435457a6e32861b7c4214e0dd/awscli-1.27.23-py3-none-any.whl')
url_download_upload(S3_BUCKET_NAME, EXT_PKG_DIR, filename, file_url)

# Function to display key cluster info


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername",
                  "DBName", "Endpoint", "NumberOfNodes", "VpcId", "VpcSecurityGroups"]
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


# Create Redshift Cluster
try:
    t0 = time.time()
    print(f"Creating Redshift Cluster {DWH_CLUSTER_IDENTIFIER}...")
    response = redshift_client.create_cluster(
        # add parameters for hardware
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        # add parameters for identifiers & credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        # add parameter for role (to allow s3 access)
        IamRoles=[redshift_roleArn]
    )
    exit_v = 0
    print('Waiting for Cluster Endpoint...')
    while not (exit_v):
        response = redshift_client.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        if 'Endpoint' in response:
            t1 = time.time()
            texec = f"[{round(t1-t0, 2)}s]"
            print(f"{DWH_CLUSTER_IDENTIFIER} Build COMPLETE "
                  f"Cluster is currently {response['ClusterAvailabilityStatus']}. {texec : >30}")
            time.sleep(5)
            exit_v = 1
except Exception as e:
    print(e)

myClusterProps = redshift_client.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)

# ADD NEW DWH VARIABLES
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_VPCID = myClusterProps['VpcId']

# Create Security group for Cluster
try:
    t0 = time.time()
    print("Creating Security Group for Redshift Cluster...")
    vpc = ec2_resource.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[-1]

    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
    t1 = time.time()
    texec = f"[{round(t1-t0, 2)}]s"
    print(f"{myClusterProps['VpcSecurityGroups'][-1]['VpcSecurityGroupId']} updated. {texec : >30}")
except Exception as e:
    print(e)

# Create Glue-Redshift job script
with open('create_rs_tables.py', 'w') as f:
    f.write(
        cleandoc(f'''
                import sys
                sys.path.insert(0, '/glue/lib/installation')
                keys = [k for k in sys.modules.keys() if 'boto' in k]
                for k in keys:
                    if 'boto' in k:
                    del sys.modules[k]

                import awscli
                import s3transfer
                import redshift_connector

                # Estabish DWH Connection
                conn = redshift_connector.connect(
                   host='{DWH_ENDPOINT}',
                   database='{DWH_DB}',
                   user='{DWH_DB_USER}',
                   password='{DWH_DB_PASSWORD}',
                )

                conn.autocommit = True
                cur = conn.cursor()

                # Create DWH Tables
                cur.execute("""
                            CREATE TABLE IF NOT EXISTS "dim_date" (
                                "date_id" INTEGER,
                                "date" DATE NOT NULL,
                                "day_name" VARCHAR(9) NOT NULL,
                                "day_of_week" INTEGER NOT NULL,
                                "day" INTEGER NOT NULL,
                                "day_of_year" INTEGER NOT NULL,
                                "month" INTEGER NOT NULL,
                                "month_name" VARCHAR(10) NOT NULL,
                                "week" INTEGER NOT NULL,
                                "quarter" INTEGER NOT NULL,
                                "year" INTEGER NOT NULL,
                                "year_half" INTEGER NOT NULL,
                                "is_weekend" BOOLEAN NOT NULL,
                                PRIMARY KEY (date_id)
                                )
                                SORTKEY (date)
                            """)

                cur.execute("""
                            CREATE TABLE IF NOT EXISTS "dim_hospital" (
                                "hosp_sk" INTEGER IDENTITY(1, 1),
                                "fips" VARCHAR(6) NOT NULL,
                                "state_fips" VARCHAR(2) NOT NULL,
                                "county_fips" VARCHAR(3) NOT NULL,
                                "state_name" VARCHAR(30) NOT NULL,
                                "county_name" VARCHAR(120),
                                "hospital_name" TEXT NOT NULL,
                                "hq_address" VARCHAR(150),
                                "hq_city" VARCHAR(150),
                                "hq_state" CHAR(2),
                                "hq_zip_code" CHAR(5),
                                "hospital_type" VARCHAR(150),
                                "latitude" REAL,
                                "longtitude" REAL,
                                PRIMARY KEY (hosp_sk)
                                )
                                SORTKEY (state_name)
                            """)

                cur.execute("""
                            CREATE TABLE IF NOT EXISTS "dim_region" (
                                "region_SK" INTEGER IDENTITY(1,1),
                                "fips" VARCHAR(6) NOT NULL,
                                "state_fips" VARCHAR(2) NOT NULL,
                                "county_fips" VARCHAR(3) NOT NULL,
                                "state" VARCHAR(30) NOT NULL,
                                "county" VARCHAR(120),
                                "country" VARCHAR(20),
                                "latitude" REAL,
                                "longitude" REAL,
                                PRIMARY KEY (region_SK)
                                )
                                SORTKEY (state)
                            """)

                cur.execute("""
                            CREATE TABLE IF NOT EXISTS "fact_covid" (
                                "date" INTEGER,
                                "state_fips" VARCHAR(3) NOT NULL,
                                "state" VARCHAR(30) NOT NULL,
                                "positive" REAL,
                                "positiveincrease" INTEGER,
                                "negative" REAL,
                                "death" REAL,
                                "deathincrease" INTEGER,
                                "recovered" REAL,
                                "hospitalized" REAL,
                                "hospitalizedcurrently" REAL,
                                "hospitalizeddischarged" REAL,
                                "hospitalizedcumulative" REAL,
                                "hospitalizedincrease" INTEGER,
                                "region_sk" INTEGER,
                                "hosp_sk" INTEGER,
                                PRIMARY KEY (date, state_fips),
                                FOREIGN KEY (date) REFERENCES dim_date (date_id),
                                FOREIGN KEY (region_sk) REFERENCES dim_region (region_sk),
                                FOREIGN KEY (hosp_sk) REFERENCES dim_hospital (hosp_sk)
                                )
                                SORTKEY (date, state)
                            """)

                # Load data from S3 Bucket
                cur.execute("""
                            copy dim_date from 's3://{S3_BUCKET_NAME}/output/dim_date.csv'
                            credentials 'aws_iam_role={redshift_roleArn}'
                            region '{AWS_REGION_NAME}'
                            delimiter ','
                            IGNOREHEADER 1
                            COMPUPDATE OFF
                        """)

                cur.execute("""
                            copy dim_region from 's3://{S3_BUCKET_NAME}/output/dim_region.csv'
                            credentials 'aws_iam_role={redshift_roleArn}'
                            region '{AWS_REGION_NAME}'
                            delimiter ','
                            explicit_ids
                            IGNOREHEADER 1
                            COMPUPDATE OFF
                        """)

                cur.execute("""
                            copy dim_hospital from 's3://{S3_BUCKET_NAME}/output/dim_hospital.csv'
                            credentials 'aws_iam_role={redshift_roleArn}'
                            region '{AWS_REGION_NAME}'
                            delimiter ','
                            explicit_ids
                            IGNOREHEADER 1
                            COMPUPDATE OFF
                        """)

                cur.execute("""
                            copy fact_covid from 's3://{S3_BUCKET_NAME}/output/fact_covid.csv'
                            credentials 'aws_iam_role={redshift_roleArn}'
                            region '{AWS_REGION_NAME}'
                            delimiter ','
                            IGNOREHEADER 1
                            COMPUPDATE OFF
                        """)

                # Create Views for Visualizations
                # /* total by state positive, death, hospitalized */
                cur.execute("""
                            CREATE OR REPLACE VIEW state_totals (state, state_abv, total_positive_cases,
                            total_deaths, avg_hospitalized) AS
                                SELECT dr.state as state_name, fc.state, SUM(positive) as positive_cases,
                                    SUM(death) as deaths, ROUND(AVG(hospitalizedcurrently), 0) avg_hospitalized
                                FROM fact_covid fc
                                    JOIN dim_region dr ON fc.region_sk = dr.region_sk
                                GROUP BY dr.state, fc.state
                                ORDER BY fc.state
                        """)

                # /* total US */
                cur.execute("""
                            CREATE OR REPLACE VIEW us_totals (postive_cases, deaths, begin_data, end_data) AS
                                SELECT SUM(positive) as positive_cases, SUM(death) as deaths,
                                MIN(date) as From, MAX(date) as To
                                FROM fact_covid
                        """)
                # /* Daily */
                cur.execute("""
                            CREATE OR REPLACE VIEW state_daily (
                                date, state, state_abv, positive, pos_increase,
                                negative, deaths, death_increase, recovered, hospitalized, hosp_currently,
                                hosp_increase, lattitude, longitude) AS
                                SELECT dd.date, dr.state as state_name, fc.state, fc.positive, fc.positiveincrease,
                                        fc.negative, fc.death, fc.deathincrease, fc.recovered, fc.hospitalized,
                                        fc.hospitalizedcurrently, fc.hospitalizedincrease,
                                        min(dr.latitude) as latitude, min(dr.longitude) as longitude
                                FROM fact_covid fc
                                JOIN dim_date dd ON fc.date = dd.date_id
                                JOIN dim_region dr ON fc.region_sk = dr.region_sk
                                GROUP BY dd.date, dr.state , fc.state, fc.positive, fc.positiveincrease,
                                        negative, death, deathincrease, recovered, hospitalized, hospitalizedcurrently,
                                        hospitalizedincrease
                                ORDER BY dd.date, fc.state
                        """)

                cur.close()
                conn.close()
                ''')
    )

# Upload shema and data transfer script to S3
upload_local_file('create_rs_tables.py', S3_BUCKET_NAME, S3_SCRIPTS_DIR)

# Create Glue Job
etl_job = glue_client.create_job(Name='s3RedShiftGlue',
                                 Role=GLUE_IAM_ROLE,
                                 Command={
                                     'Name': 'pythonshell',
                                     'ScriptLocation': f"s3://{S3_BUCKET_NAME}/{S3_SCRIPTS_DIR}create_rs_tables.py",
                                     'PythonVersion': '3.9'
                                 },
                                 DefaultArguments={'--extra-py-files': cleandoc(f"""
                                        s3://{S3_BUCKET_NAME}/{EXT_PKG_DIR}awscli-1.27.23-py3-none-any.whl,
                                        s3://{S3_BUCKET_NAME}/{EXT_PKG_DIR}botocore-1.29.23-py3-none-any.whl,
                                        s3://{S3_BUCKET_NAME}/{EXT_PKG_DIR}redshift_connector-2.0.909-py3-none-any.whl,
                                        s3://{S3_BUCKET_NAME}/{EXT_PKG_DIR}boto3-1.26.23-py3-none-any.whl,
                                        s3://{S3_BUCKET_NAME}/{EXT_PKG_DIR}s3transfer-0.6.0-py3-none-any.whl
                                        """)
                                                   }
                                 )

# Run Glue Job
run_id = glue_client.start_job_run(JobName=GLUE_ETL_JOB)

# Estabish DWH Connection
conn = redshift_connector.connect(
    host=DWH_ENDPOINT,
    database=DWH_DB,
    user=DWH_DB_USER,
    password=DWH_DB_PASSWORD,
)

conn.autocommit = True
cur = conn.cursor()

# Exract data for visualization
# US Totals
cur.execute(f"""
            UNLOAD
            ('SELECT * FROM covid_dw.public.us_totals')
            TO 's3://hernanjs-covid19-de/queries/us_totals'
            CREDENTIALS 'aws_iam_role={redshift_roleArn}'
            REGION '{AWS_REGION_NAME}'
            DELIMITER AS ','
            HEADER
            ADDQUOTES
            NULL AS ''
            PARALLEL OFF
            """
            )
# State Totals
cur.execute(f"""
            UNLOAD
            ('SELECT * FROM covid_dw.public.state_totals')
            TO 's3://hernanjs-covid19-de/queries/state_totals'
            CREDENTIALS 'aws_iam_role={redshift_roleArn}'
            REGION '{AWS_REGION_NAME}'
            DELIMITER AS ','
            HEADER
            ADDQUOTES
            NULL AS ''
            PARALLEL OFF
            """
            )
# Daily State
cur.execute(f"""
            UNLOAD
            ('SELECT * FROM covid_dw.public.states_daily')
            TO 's3://hernanjs-covid19-de/queries/states_daily'
            CREDENTIALS 'aws_iam_role={redshift_roleArn}'
            REGION '{AWS_REGION_NAME}'
            DELIMITER AS ','
            HEADER
            ADDQUOTES
            NULL AS ''
            PARALLEL OFF
            """
            )

cur.close()
conn.close()

download_to_local(S3_BUCKET_NAME, "queries/state_daily000", "output/state_daily.csv")
download_to_local(S3_BUCKET_NAME, "queries/state_totals000", "output/state_totals.csv")
download_to_local(S3_BUCKET_NAME, "queries/us_totals000", "output/us_totals.csv")

# Resource Cleanup
# Glue
for crawler in crawlers:
    print(f"Deleting {crawler}...")
    glue_client.delete_crawler(Name=crawler)

glue_client.delete_database(Name=GLUE_DB)
glue_client.delete_job(JobName=GLUE_ETL_JOB)
# Redshift
redshift_client.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
