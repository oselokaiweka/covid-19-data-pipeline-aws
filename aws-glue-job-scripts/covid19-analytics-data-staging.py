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
        
        
        # Initialize boto3 session
        session = boto3.Session(
            region_name=job_region,
            aws_access_key_id=aws_key, 
            aws_secret_access_key=aws_secret
        )
        
        # Initialize required clients 
        job_s3_client = session.client('s3')
        glue_client = session.client('glue')
        athena_client = session.client('athena')
        
        # Function retrieves athena tables within the given schema
        tables = glue_client.get_tables(DatabaseName=schema_name)['TableList']

        for table in tables:
            table_name = table['Name']
            query = f"select * from {table_name};"
            # Function executes the query_athena_and_fetch_results() function asynchronously
            query_athena_and_fetch_results(
                athena_client=athena_client,
                job_s3_client=job_s3_client, 
                database=schema_name,  
                query=query,
                table=table_name,
                job_bucket_name=job_bucket_name,
                job_staging_s3Prefix=job_staging_s3Prefix,
                job_temp_path=job_temp_path
            )

    except Exception as e:
        logger.error(f"Failed to complete the Glue job. Error: {str(e)}", exc_info=True)
        
    
# Function to execute query athena and retrieve table records
def query_athena_and_fetch_results(
        athena_client,
        job_s3_client, 
        database, 
        query,
        table,
        job_bucket_name,
        job_staging_s3Prefix,
        job_temp_path):
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={
                'OutputLocation': job_temp_path,
                'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'},
            }
        )
        query_execution_id = response['QueryExecutionId']
    
        # Loop till query execution is complete
        while True:
            response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                
            state = response['QueryExecution']['Status']['State']
            if state == 'SUCCEEDED':
                logger.info(f"\n{query_execution_id} query has completed successfuly")
    
                
                results_path = f'{job_temp_path.rstrip("/")}/{query_execution_id}.csv'
                print(results_path)
                staged_file = f'{job_staging_s3Prefix.rstrip("/")}/{table}.csv'
                print(staged_file)
                
                job_s3_client.copy_object(
                    Bucket=job_bucket_name, 
                    CopySource=results_path[4:], # remove 's3:/', starts with '/'
                    Key=staged_file
                )
                logger.info(f"\n{staged_file} downloaded successfuly")
                
                
                # Delete query result temp file in temp folder       
                job_s3_client.delete_objects(
                Bucket=job_bucket_name, 
                    Delete={
                        'Objects': [
                            {'Key': results_path[5:][results_path[5:].find('/') + 1:]}, # slice off s3://bucket_name
                            {'Key': fr"{results_path[5:][results_path[5:].find('/') + 1:]}.metadata"}
                        ],
                        'Quiet': True
                    }
                )
        
                return
            
            elif state in ['FAILED', 'CANCELLED']:
                raise Exception(f"Query {state.lower()}. Reason: {response['QueryExecution']['Status']['StateChangeReason']}")
                
            else:
                logger.info(f"/n{query_execution_id} query is still running, waiting 3 seconds...")
                time.sleep(3)
                
    except ClientError as e:
        logger.error(f"Error during Athena query execution: {str(e)}", exc_info=True)
        raise
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

