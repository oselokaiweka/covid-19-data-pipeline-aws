import sys
import boto3
import configparser
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions


# Access Glue job parameters
args = getResolvedOptions(
    sys.argv, [
        'JOB_NAME', 
        'CONFIG_BUCKET_NAME', 
        'CONFIG_FILE_KEY'
    ]
)

# Specify the s3 bucket and file path from Glue Job parameters
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

src_bucket_name = config.get('S3', 'SRC_BUCKET_NAME')  # Public data source
src_bucket_prefixes = config.get('S3', 'SRC_BUCKET_PREFIXES')  # Set of locations in s3
src_bucket_region = config.get('S3', 'SRC_BUCKET_REGION')

job_bucket_name = config.get('S3', 'JOB_BUCKET_NAME')
job_region = config.get('S3', 'JOB_REGION')

# Initialize s3 client to access source data s3 bucket
src_s3_client = boto3.client('s3', region_name=src_bucket_region)

# Initialize s3 client to create and/or access raw data s3 bucket
job_s3_client = boto3.client(
    's3', 
    region_name=job_region,
    aws_access_key_id=aws_key, 
    aws_secret_access_key=aws_secret
)

# Method to create s3 bucket if it doesn't exist
def create_s3_if_not_exists(bucket_name, bucket_region, s3_client):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"S3 bucket '{bucket_name}' already exists.")
        
    except ClientError as e:
        error_code = e.response['Error']['Code'] 
        if error_code == '404': 
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': bucket_region
                }
            )
            print(f"S3 bucket '{bucket_name}' has been created.")
        else:
            print(e)
    except Exception as e:
        print(e)

# Function to copy files from source s3 bucket to target s3 bucket
def copy_objects_from_s3_to_s3(
    src_bucket_name, 
    src_bucket_prefixes, 
    target_bucket_name, 
    src_s3_client, 
    target_s3_client
):
    src_bucket_prefixes = src_bucket_prefixes.split(',')  # Convert to comma dilimited list
    
    for src_bucket_prefix in src_bucket_prefixes:
        src_bucket_prefix = src_bucket_prefix.strip()
        print(f"\nAccessing src_bucket_prefix: '{src_bucket_prefix}' >>>")

        paginator = src_s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=src_bucket_name, Prefix=src_bucket_prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    copy_source = {'Bucket': src_bucket_name, 'Key': obj['Key']}
                    target_key = f"raw_data/{obj['Key']}" 

                    try:
                        target_s3_client.head_object(Bucket=target_bucket_name, Key=target_key)
                        print(f"Skipping {target_key}, already exists")
                        continue 

                    except ClientError as e:
                        if e.response['Error']['Code'] == '404': 
                            print(f"Copying {obj['Key']} to {target_bucket_name}/{target_key}")
                            try:
                                target_s3_client.copy_object(
                                    CopySource=copy_source, 
                                    Bucket=target_bucket_name,
                                    Key=target_key
                                )
                            except ClientError as e:
                                print(f"ClientError: {e}")
                            except Exception as e:
                                print(f"Exception: {e}")
            else:
                print(f"No content in '{src_bucket_prefix}'\n")
                
                
# Function to create s3 bucket 
create_s3_if_not_exists(
    job_bucket_name, 
    job_region, 
    job_s3_client
)               

# Function to copy data from source s3 to target s3
copy_objects_from_s3_to_s3(
    src_bucket_name, 
    src_bucket_prefixes, 
    job_bucket_name, 
    src_s3_client, 
    job_s3_client
)