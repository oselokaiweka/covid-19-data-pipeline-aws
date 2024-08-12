import sys
import time
import boto3
import logging
import configparser
from pathlib import Path
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s %(levelname) %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    try:
        # Access Glue job parameters
        args = getResolvedOptions(
            sys.argv, [
                'JOB_NAME', 
                'CONFIG_BUCKET_NAME', 
                'CONFIG_FILE_KEY',
            ]
        )
        
        # Specify the s3 bucket and file pathS from Glue Job parameters
        config_bucket_name = args['CONFIG_BUCKET_NAME']  
        config_file_key = args['CONFIG_FILE_KEY'] 
        
        # Initialize s3 client to access config file
        config_s3_client = boto3.client('s3') # Grant glue access with IAM role
        
        # Read the config file from s3
        config_obj = config_s3_client.get_object(Bucket=config_bucket_name, Key=config_file_key)
        config_data = config_obj['Body'].read().decode('utf-8')
        
        # Parse the config file
        config = configparser.ConfigParser()
        config.read_string(config_data)
        
        # Access parameters stored in config file
        aws_key = config.get('AWS', 'KEY')
        aws_secret = config.get('AWS', 'SECRET')
        job_bucket_name = config.get('S3', 'JOB_BUCKET_NAME')
        job_staging_s3Path = config.get('S3', 'JOB_STAGING_PATH')
        job_staging_s3Prefix = config.get('S3', 'JOB_STAGING_PREFIX')
        job_temp_path = config.get('S3', 'JOB_TEMP_PATH')
        schema_name = config.get('GLUE', 'SCHEMA_NAME')
        job_region = config.get('S3', 'JOB_REGION')
        
        
        
        # Initialize S3 client to create and/or access staging s3 bucket
        job_s3_client = boto3.client(
            's3', 
            region_name=job_region,
            aws_access_key_id=aws_key, 
            aws_secret_access_key=aws_secret
        )
        
        # Initialize Glue client to access and retrieve schema table list
        glue_client = boto3.client(
            'glue', 
            region_name=job_region,
            aws_access_key_id=aws_key, 
            aws_secret_access_key=aws_secret
        )
        
        # Initialize Athena client to query data and download query results
        athena_client = boto3.client(
            'athena',
            region_name=job_region,
            aws_access_key_id=aws_key, 
            aws_secret_access_key=aws_secret
        )
import time
import configparser

import boto3
import psycopg2
import pandas as pd
from botocore.exceptions import ClientError

from pyspark.sql import SparkSession
  
spark = SparkSession.builder.master("local[*]").appName("Join").getOrCreate()
config = configparser.ConfigParser()
config.read_file(open('covid19-analytics.config'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

TARGET_OUTPUT_BUCKET=config.get('S3', 'TARGET_OUTPUT_BUCKET')
TARGET_OUTPUT_S3 = config.get('S3', 'TARGET_OUTPUT_S3')
TARGET_OUTPUT_DIR=config.get('S3', 'TARGET_OUTPUT_DIR')
TARGET_REGION = config.get('S3', 'TARGET_REGION')
TMP_DIR = config.get('FILE_PATHS', 'TMP_DIR')

DWH_CLUSTER_TYPE = config.get('DWH', 'DWH_CLUSTER_TYPE')
DWH_NUM_NODES = config.get('DWH', 'DWH_NUM_NODES')
DWH_NODE_TYPE = config.get('DWH', 'DWH_NODE_TYPE')
DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DWH_DB = config.get('DWH', 'DWH_DB')
DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
DWH_PORT = config.get('DWH', 'DWH_PORT')
DWH_IAM_ROLE_NAME = config.get('DWH', 'DWH_IAM_ROLE_NAME')
OUTPUT_S3_CLIENT = boto3.client(
    's3', 
    region_name=TARGET_REGION,
    aws_access_key_id=KEY, 
    aws_secret_access_key=SECRET
)

redshift_client = boto3.client(
    'redshift',
    region_name=TARGET_REGION,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)

ec2_client = boto3.resource(
    'ec2',
    region_name=TARGET_REGION,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)

iam_client = boto3.client(
    'iam',
    region_name=TARGET_REGION,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)
enigma_jhu = pd.read_csv(f'{TMP_DIR}/enigma_jhu.csv')
testing_data_states_daily = pd.read_csv(f'{TMP_DIR}/testing-datastates_daily.csv')

factCovid_1 = enigma_jhu[['fips', 'province_state', 'country_region', 'confirmed', 'deaths', 'recovered', 'active' ]]
factCovid_2 = testing_data_states_daily[['fips', 'date', 'positive', 'negative', 'hospitalizedcurrently', 'hospitalized', 'hospitalizeddischarged' ]]
factCovid = pd.merge(factCovid_1, factCovid_2, on='fips', how='inner')
print(len(factCovid))

factCovid = factCovid.drop_duplicates(keep='first')
dimHospital = pd.read_csv(f'{TMP_DIR}/hospital-bedsjson.csv')
dimHospital =  dimHospital[['fips', 'state_name', 'latitude', 'longtitude', 'hq_address', 'hospital_name', 'hospital_type', 'hq_city', 'hq_state']]
dimHospital = dimHospital.rename(columns={'longtitude': 'longitude'})

dimHospital = dimHospital.drop_duplicates(keep='first')

dimHospital['latitude'] = pd.to_numeric(dimHospital['latitude'], errors= 'coerce')
dimHospital['longitude'] = pd.to_numeric(dimHospital['longitude'], errors= 'coerce')
dimDate = pd.read_csv(f'{TMP_DIR}/testing-datastates_daily.csv')
dimDate = dimDate[['fips', 'date']]

dimDate['date'] = pd.to_datetime(dimDate['date'], format='%Y%m%d')
dimDate['year'] = dimDate['date'].dt.year
dimDate['month'] = dimDate['date'].dt.month
dimDate["day_of_week"] = dimDate['date'].dt.dayofweek

dimDate = dimDate.drop_duplicates(keep='first')

dimDate['fips'] = dimDate['fips'].astype(float)
dimDate['date'] = pd.to_datetime(dimDate['date'], errors= 'coerce')
dimDate['date'] = dimDate['date'].astype('datetime64[ns]')
enigma_jhu = spark.read.csv(
    f'{TMP_DIR}/enigma_jhu.csv', 
    header=True, 
    inferSchema=True
)

ny_times_us_county = spark.read.csv(
    f'{TMP_DIR}/us_county.csv', 
    header=True, 
    inferSchema=True
)

dimRegion_1 = enigma_jhu.select('fips', 'province_state', 'country_region', 'latitude', 'longitude')
dimRegion_2 = ny_times_us_county.select('fips', 'county', 'state')

dimRegion_1 = dimRegion_1.repartition(4, 'fips')
dimRegion_2 = dimRegion_2.repartition(4, 'fips')
dimRegion_2 = dimRegion_2.withColumnRenamed('fips', 'fips2')

dimRegion = dimRegion_1.join(
    dimRegion_2, 
    dimRegion_1["fips"] == dimRegion_2["fips2"], 
    "inner"
)

dimRegion = dimRegion.drop('fips2')
print(dimRegion.count())

dimRegion = dimRegion.distinct()
print(dimRegion.count())

dimRegion = dimRegion.toPandas()
dimRegion['fips'] = dimRegion['fips'].astype(float)

dimRegion['latitude'] = pd.to_numeric(dimRegion['latitude'], errors= 'coerce')
dimRegion['longitude'] = pd.to_numeric(dimRegion['longitude'], errors= 'coerce')
factCovid.to_csv(f"{TMP_DIR}/factCovid.csv"
OUTPUT_S3_CLIENT.upload_file(
    f"{TMP_DIR}/factCovid.csv",
    Bucket=TARGET_OUTPUT_S3,
    Key=f'{TARGET_OUTPUT_DIR}/factCovid.csv',
)

dimHospital.to_csv(f"{TMP_DIR}/dimHospital.csv")
OUTPUT_S3_CLIENT.upload_file(
    f"{TMP_DIR}/dimHospital.csv",
    Bucket=TARGET_OUTPUT_S3,
    Key=f'{TARGET_OUTPUT_DIR}/dimHospital.csv',
)
                 
dimDate.to_csv(f"{TMP_DIR}/dimDate.csv")
OUTPUT_S3_CLIENT.upload_file(
    f"{TMP_DIR}/dimDate.csv",
    Bucket=TARGET_OUTPUT_S3,
    Key=f'{TARGET_OUTPUT_DIR}/dimDate.csv',
)
                 
dimRegion.to_csv(f"{TMP_DIR}/dimRegion.csv")
OUTPUT_S3_CLIENT.upload_file(
    f"{TMP_DIR}/dimRegion.csv",
    Bucket=TARGET_OUTPUT_S3,
    Key=f'{TARGET_OUTPUT_DIR}/dimRegion.csv',
)
                 
# Construct CREATE TABLE SQL dynamically from pandas dataframe
factCovid_sql = f"{pd.io.sql.get_schema(factCovid.reset_index(), 'factCovid')};"
# Construct CREATE TABLE SQL dynamically from pandas dataframe
staging_factCovid_sql = f"{pd.io.sql.get_schema(factCovid.reset_index(), 'staging_factCovid')};"


# Construct CREATE TABLE SQL dynamically from pandas dataframe
dimHospital_sql = f"{pd.io.sql.get_schema(dimHospital.reset_index(), 'dimHospital')};"
# Construct CREATE TABLE SQL dynamically from pandas dataframe
staging_dimHospital_sql =  f"{pd.io.sql.get_schema(dimHospital.reset_index(), 'staging_dimHospital')};"


# Construct CREATE TABLE SQL dynamically from pandas dataframe
dimDate_sql = f"{pd.io.sql.get_schema(dimDate.reset_index(), 'dimDate')};"
# Construct CREATE TABLE SQL dynamically from pandas dataframe
staging_dimDate_sql =  f"{pd.io.sql.get_schema(dimDate.reset_index(), 'staging_dimDate')};"


# Construct CREATE TABLE SQL dynamically from pandas dataframe
dimRegion_sql = f"{pd.io.sql.get_schema(dimRegion.reset_index(), 'dimRegion')};"
# Construct CREATE TABLE SQL dynamically from pandas dataframe
staging_dimRegion_sql =  f"{pd.io.sql.get_schema(dimRegion.reset_index(), 'staging_dimRegion')};"
# Method implements retries while obtaining redshift properties in case creating cluster is yet complete
def get_redshift_props(redshift_client, cluster_identifier):
    retries = 30
    retry_delay = 30 # Delay between retries in seconds
    for attempt in range(retries):
        try:
            clusterProps = redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
            if clusterProps['ClusterAvailabilityStatus'] == 'Available':
                return clusterProps
            elif clusterProps['ClusterAvailabilityStatus'] != 'Available':
                if attempt < retries -1:
                    print(f"Cluster '{cluster_identifier}' not ready. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
        except redshift_client.exceptions.ClusterNotFoundFault as e:
            if attempt < retries -1:
                print(f"Cluster '{cluster_identifier}' not found. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay/3)
            else:
                raise e # Raise the last exception if the retries are exhausted
def pretty_redshift_props(props):
    pd.set_option('display.max.colwidth', 0)
    keysToShow = ['ClusterIdentifier', 'ClusterStatus', 'NodeType', 'NumberOfNodes', 'DBName', 'MasterUsername', 'Endpoint', 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=['Parameter', 'value'])

clusterProps = get_redshift_props(redshift_client, DWH_CLUSTER_IDENTIFIER)
if clusterProps:
    DWH_ENDPOINT = clusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = clusterProps['IamRoles'][0]['IamRoleArn']
try:
    vpc = ec2_client.Vpc(id=clusterProps['VpcId'])
    default_SG = list(vpc.security_groups.all())[0]
    print(default_SG)

    default_SG.authorize_ingress(
        GroupName=default_SG.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT),
    )
except ClientError as e:
    # Check for duplicate rule errors
    error_code = e.response['Error']['Code']
    if error_code == 'InvalidPermission.Duplicate':
        print('Security group rule exists, no further actions required')
    else:
        raise e
except Exception as e:
    raise e
try:
    conn = psycopg2.connect(
        host=DWH_ENDPOINT,
        dbname=DWH_DB,
        user=DWH_DB_USER,
        password=DWH_DB_PASSWORD,
        port=int(DWH_PORT)
    )
except Exception as e:
    print(e)

conn.set_session(autocommit=True)

try:
    cur = conn.cursor()
except Exception as e:
    print("Error: Could not obtain database cursor")
    print(e)
# Create tables
try:
    cur.execute(staging_factCovid_sql)
    cur.execute(factCovid_sql)
except Exception as e:
    print(e)

try:
    cur.execute(staging_dimHospital_sql)
    cur.execute(dimHospital_sql)
except Exception as e:
    print(e)

try:
    cur.execute(staging_dimDate_sql)
    cur.execute(dimDate_sql)
except Exception as e:
    print(e)
    
try:
    cur.execute(staging_dimRegion_sql)
    cur.execute(dimRegion_sql)
except Exception as e:
    print(e)
try:
    cur.execute(
    f"""
    copy staging_dimhospital
    from '{TARGET_OUTPUT_BUCKET}dimHospital.csv'
    credentials 'aws_iam_role={DWH_ROLE_ARN}'
    delimiter ','
    region '{TARGET_REGION}'
    IGNOREHEADER 1
    EMPTYASNULL
    BLANKSASNULL
    MAXERROR 100
    """
    )
except ClientError as error:
    print(error)
except Exception as e:
    print(e)
    
    
try:
    cur.execute(
    f"""
    copy staging_factCovid
    from '{TARGET_OUTPUT_BUCKET}factCovid.csv'
    credentials 'aws_iam_role={DWH_ROLE_ARN}'
    delimiter ','
    region '{TARGET_REGION}'
    IGNOREHEADER 1
    EMPTYASNULL
    BLANKSASNULL
    MAXERROR 100
    """
    )
except ClientError as error:
    print(error)
except Exception as e:
    print(e)
    
    
try:
    cur.execute(
    f"""
    copy staging_dimdate
    from '{TARGET_OUTPUT_BUCKET}dimDate.csv'
    credentials 'aws_iam_role={DWH_ROLE_ARN}'
    delimiter ','
    region '{TARGET_REGION}'
    IGNOREHEADER 1
    EMPTYASNULL
    BLANKSASNULL
    MAXERROR 100
    """
    )
except ClientError as error:
    print(error)
except Exception as e:
    print(e)
    

try:
    cur.execute(
    f"""
    copy staging_dimRegion
    from '{TARGET_OUTPUT_BUCKET}dimRegion.csv'
    credentials 'aws_iam_role={DWH_ROLE_ARN}'
    delimiter ','
    region '{TARGET_REGION}'
    IGNOREHEADER 1
    EMPTYASNULL
    BLANKSASNULL
    MAXERROR 100
    """
    )
except ClientError as error:
    print(error)
except Exception as e:
    print(e)
job.commit()