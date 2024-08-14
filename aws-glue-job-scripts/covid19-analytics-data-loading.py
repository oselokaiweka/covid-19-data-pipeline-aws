import sys
import time
import boto3
import logging
import configparser
import pandas as pd
import awswrangler as wr
import psycopg2
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# sys.argv += ["--JOB_NAME", "covid19-analytics-data-warehouse-loading"]
# sys.argv += ["--CONFIG_BUCKET_NAME", "jk-config-s3"]
# sys.argv += ["--CONFIG_FILE_KEY", "covid19-analytics-config/covid19-analytics.config"]

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s %(levelname) %(message)s'
)
logger = logging.getLogger(__name__)


# Access Glue job parameters
args = getResolvedOptions(
    sys.argv, [
        'JOB_NAME', 
        'CONFIG_BUCKET_NAME', 
        'CONFIG_FILE_KEY',
    ]
)

# Initialize job, setup parameters,logging and other environment settings
job.init(args['JOB_NAME'],args)

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
job_region = config.get('S3', 'JOB_REGION')
job_bucket_name = config.get('S3', 'JOB_BUCKET_NAME')
job_output_prefix = config.get('S3', 'JOB_OUTPUT_PREFIX')
job_output_path = config.get('S3', 'JOB_OUTPUT_PATH')
crawler_name = config.get('GLUE', 'CRAWLER2')
crawler_roleArn = config.get('GLUE', 'CRAWLER_ROLE')
schema_name = config.get('GLUE', 'SCHEMA_NAME2')
db_password = config.get('DWH', 'DWH_DB_PASSWORD')
cluster_identifier = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')


# Set up the boto3 session
session = boto3.Session(
    region_name=job_region,
    aws_access_key_id=aws_key, 
    aws_secret_access_key=aws_secret
)

# Initialize required clients
glue_client = session.client('glue')
redshift_client = session.client('redshift')
ec2_client = session.resource('ec2')

# Check if crawler exists, creates crawler if not found
try:
    crawler_response = glue_client.get_crawler(Name=crawler_name)
    logger.info("Crawler '%s' already exists.", crawler_name)
    print('Crawler exists')
except ClientError as e:
    logger.warning("'%s' glue crawler object not found. Creating crawler...", crawler_name)

    try:
        crawler_response = glue_client.create_crawler(
            Name=crawler_name,
            Role=crawler_roleArn,
            DatabaseName=schema_name,
            TablePrefix='',  # No prefix needed
            RecrawlPolicy={'RecrawlBehavior': 'CRAWL_EVERYTHING'},
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE', 
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            },
            Targets={
                'S3Targets': [
                    {
                        'Path': fr"s3://{job_bucket_name}/{job_output_prefix}/",
                        'Exclusions': ['**.json'] # Double ** matches all .json files in current folder and all subfolders
                    }
                ]
            }
        )
        logger.info("Glue crawler '%s' has been created successfully.", crawler_name)
        print('Crawler created')

    except ClientError as create_error:
        logger.error("Failed to create crawler '%s'. Error: %s", crawler_name, str(create_error), exc_info=True)

# Start crawler
glue_client.start_crawler(Name=crawler_name)
logger.info("\n'%s' crawler has been started.", crawler_name)
print('crawler started')

# Wait for crawler to finish cataloging target data
while True:
    crawler_status = glue_client.get_crawler(Name=crawler_name)['Crawler']['State']
    if crawler_status in ['READY', 'SUCCEEDED']:
        break
    time.sleep(15)  # Wait for 15 seconds before checking again

# Fetch list of tables created by crawler
tables = glue_client.get_tables(DatabaseName=schema_name)['TableList']
table_count = len(tables)
print(table_count)
logger.info("\nTotal of %s created by crawler", table_count)



# Loop through tables

dfs = {} # Store each {table_name : dataframe}
s3_paths = {} # Store each {table_name : s3_path}

for table in tables:
    
    # Use GlueContext to create a dynamic frame 
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=schema_name, 
        table_name=table['Name'], 
        transformation_ctx=f"dynamic_frame_{table['Name']}"
    )
    
    # Map data loccation to dynamic frame
    table_metadata = glue_client.get_table(DatabaseName=schema_name, Name=table['Name'])
    s3_path = table_metadata['Table']['StorageDescriptor']['Location']
    s3_paths[fr"{table['Name']}"] = s3_path
    
    # Convert to PySpark Dataframe to leverage awswrangler
    df = dynamic_frame.toDF()
    dfs[fr"{table['Name']}"] = df
    
for table_name, df in dfs.items():
    print(table_name)
    df.show(3)
    
for table_name, s3_path in s3_paths.items():
    print(f"{table_name}: {s3_path}")




# Establiish connection
# Get data warehouse configurations (cp for cluster properties) 
cp = redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
dbname = cp['DBName']
user = cp['MasterUsername']
host = cp['Endpoint']['Address']
port = int(cp['Endpoint']['Port'])
vpc_id = cp['VpcId']
role_arn = cp['IamRoles'][0]['IamRoleArn']

# Create security group VPC ingress rule for where cluster resides
try:
    vpc = ec2_client.Vpc(id=vpc_id)
    default_SG = list(vpc.security_groups.all())[0]
    default_SG.authorize_ingress(
        GroupName=default_SG.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=port,
        ToPort=port,
    )
except ClientError as e:
    # Check is security group rule already exists
    if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
        print('Security group rule exists, no further actions required')
    else:
        raise e

# Connect to Redshift data warehouse
conn = psycopg2.connect(
    host=host,
    dbname=dbname,
    user=user,
    password=db_password,
    port=port
)

conn.set_session(autocommit=True) 
cursor = conn.cursor()

# Copy Dataframe to redshift tables already created in previous job
for table_name, df in dfs.items():
    
    cursor.execute(
        fr"""
        copy {table_name[:-4]}
        from '{s3_paths[table_name]}'
        credentials 'aws_iam_role={role_arn}'
        delimiter ','
        region '{job_region}'
        IGNOREHEADER 1
        EMPTYASNULL
        BLANKSASNULL
        """
        )

job.commit()