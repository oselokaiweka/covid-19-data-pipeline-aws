import re
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
                'TEMPDIR'
            ]
        )
        
        # Specify the s3 bucket and file pathS from Glue Job parameters
        config_bucket_name = args['CONFIG_BUCKET_NAME']  
        config_file_key = args['CONFIG_FILE_KEY'] 
        glue_tmp_dir = args['TEMPDIR']
        
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
        job_rawData_prefix = config.get('S3', 'JOB_RAWDATA_PREFIX')
        
        crawler_name = config.get('GLUE', 'CRAWLER_NAME')
        crawler_roleArn = config.get('GLUE', 'CRAWLER_ROLE')
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
        
        # Check if crawler exists, creates crawler if not found
        try:
            crawler_response = glue_client.get_crawler(Name=crawler_name)
            logger.info("Crawler '%s' already exists.", crawler_name)
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
                            {'Path': f"s3://{job_bucket_name}/{job_rawData_prefix}"}
                        ]
                    }
                )
                logger.info("Glue crawler '%s' has been created successfully.", crawler_name)
            except ClientError as create_error:
                logger.error("Failed to create crawler '%s'. Error: %s", crawler_name, str(create_error), exc_info=True)

        # Start crawler
        glue_client.start_crawler(Name=crawler_name)
        logger.info("\n'%s' crawler has been started.", crawler_name)
        
        # Wait for crawler to finish cataloging target data
        while True:
            crawler_status = glue_client.get_crawler(Name=crawler_name)['Crawler']['State']
            if crawler_status in ['READY', 'SUCCEEDED']:
                break
            time.sleep(15)  # Wait for 15 seconds before checking again
            
        # Fetch list of tables created by crawler
        tables = glue_client.get_tables(DatabaseName=schema_name)['TableList']
        table_count = len(tables)
        logger.info("\nTotal of %s created by crawler", table_count)
        
        # Rename tables created by crawler
        for table in tables:
            current_table_name = table['Name']
            file_path = table['StorageDescriptor']['Location']
            print(file_path)

            # Extract new table name from file path as defined in function
            new_table_name = extract_table_name(job_rawData_prefix, file_path, job_s3_client)
            
            # When the function returns a None, then the table does note match desired set
            if new_table_name == None:
                try:
                    glue_client.delete_table(DatabaseName=schema_name, Name=current_table_name)
                    print(current_table_name + ' deleted')
                except ClientError as e:
                    print(current_table_name + ' not deleted')
                    logger.warning("\nFailed to delete .json file table: '%s'\nError: %s",current_table_name, str(e))
                
            else:
                try:
                    glue_client.update_table(
                        DatabaseName=schema_name,
                        TableInput={
                            'Name': new_table_name,
                            'StorageDescriptor': table['StorageDescriptor'],
                            'TableType': table['TableType'],
                            'Description': table.get('Description', '')  # Preserve existing description if any
    
                        }
                    )
                    logger.info("\n'%s' sccessfully renamed to '%s'", current_table_name, new_table_name)
                except ClientError as e:
                    logger.error("\nUnable to rename %s Error: %s", current_table_name, str(e), exc_info=True)
            
  
    except Exception as e:
        logger.error("\nFailed to complete the Glue job. Error: %s", str(e), exc_info=True)



def extract_table_name(base_folder, full_path, s3_client):
    # Parse bucket name and prefix from full_path
    parsed_url = re.match(r's3://([^/]+)/(.+)', full_path)
    bucket_name = parsed_url.group(1)
    prefix = parsed_url.group(2).rstrip('/') + '/'  # Ensure the prefix ends with '/'
    
    # List objects in the S3 path
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    file_name = None
    
    # Find the first non-directory object (file)
    for obj in response.get('Contents', []):
        if not obj['Key'].endswith('/'):
            file_name = obj['Key'].split('/')[-1].split('.')[0]
            break
    
    if file_name:
        first_sub_folder = prefix.split('/')[1]  # Get the first sub-folder
        new_table_name = f'{first_sub_folder}_{file_name}' # Concatenate
        return new_table_name
    else:
        return None


if __name__ == "__main__":
    main()

