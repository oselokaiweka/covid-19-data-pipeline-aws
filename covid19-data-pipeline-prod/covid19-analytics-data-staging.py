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
        job_staging_s3Path = config.get('S3', 'JOB_STAGING_PATH')
        job_staging_s3Prefix = config.get('S3', 'JOB_STAGING_PREFIX')
        
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
        
        # Function retrieves athena tables within the given schema
        tables = list_athena_tables(glue_client, schema_name)

        # Function executes the query_athena_and_fetch_results() function asynchronously
        # with ThreadedPoolExecutor of 4 workers for faster download
        download_table_data(
            tables, 
            max_workers=4, 
            athena_client=athena_client, 
            job_s3_client=job_s3_client,
            database=schema_name, 
            job_bucket_name=job_bucket_name, 
            job_staging_s3Prefix=job_staging_s3Prefix,
            job_staging_s3Path=job_staging_s3Path, 
            glue_tmp_dir=glue_tmp_dir
        )
  
    except Exception as e:
        logger.error(f"Failed to complete the Glue job. Error: {str(e)}", exc_info=True)



# Function returns list of Athena tables in covid19-analytics-staging-db
def list_athena_tables(glue_client, schema_name) -> list:
    tables = []
    try:
        paginator = glue_client.get_paginator('get_tables')
        # Use paginator to handle potentially large number of tables
        for page in paginator.paginate(DatabaseName=schema_name):
            for table in page['TableList']:
                tables.append(table['Name'])
        logger.info(f"Successfully retrieved list of tables from Athena.")
    except ClientError as e:
        logger.error(f"Failed to retrieve athena schema tables: {str(e)}")
    return tables
    
   
    
# Function to execute query athena and retrieve table records
def query_athena_and_fetch_results(
        athena_client,
        job_s3_client, 
        database, 
        query,
        table,
        job_bucket_name,
        job_staging_s3Prefix,
        job_staging_s3Path,
        glue_tmp_dir):
    
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={
                'OutputLocation': job_staging_s3Path,
                'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'},
            }
        )
        query_execution_id = response['QueryExecutionId']
    
        # Loop till query execution is complete
        while True:
            try:
                response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            except ClientError as e:
                logger.error(f"\nQuery Error: \n{e}")
                
            state = response['QueryExecution']['Status']['State']
            if state == 'SUCCEEDED':
                logger.info(f"\n{query_execution_id} query has completed successfuly")
    
                
                results_path = f'{glue_tmp_dir}/{query_execution_id}.csv'
                local_filename = f'{job_staging_s3Prefix.rstrip("/")}/{table}.csv'
    
                if Path(local_filename).exists():
                    logger.info(f"{local_filename} already exists, skip download")
                else:
                    try:
                        job_s3_client.download_file(job_bucket_name, results_path, local_filename)
                        logger.info(f"\n{local_filename} downloaded successfuly")
                    except ClientError as e:
                        logger.error(f"Download Error: \n{e}", exc_info=True)
                        
                try:
                    job_s3_client.delete_objects(
                        Bucket=job_bucket_name, 
                        Delete={
                            'Objects': [
                                {'Key': results_path},
                                {'Key': f'{results_path}.metadata'}
                            ],
                            'Quiet': True
                        }
                    )
                except ClientError as e:
                    logger.error(f"S3 cleanup Error: \n{e}", exc_info=True)
    
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
        logger.Error(f"Unexpected error: {str(e)}", exc_info=True)
        raise



def download_table_data(
    tables, 
    max_workers, 
    athena_client, 
    job_s3_client, 
    database, 
    job_bucket_name, 
    job_staging_s3Prefix, 
    job_staging_s3Path, 
    glue_tmp_dir):
        
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_table = {
            executor.submit(
                query_athena_and_fetch_results,
                athena_client=athena_client,
                job_s3_client=job_s3_client, 
                database=database,  
                query=f'SELECT * FROM "{table}";',
                table=table,
                job_bucket_name=job_bucket_name,
                job_staging_s3Prefix=job_staging_s3Prefix,
                job_staging_s3Path=job_staging_s3Path,
                glue_tmp_dir=glue_tmp_dir
            ): table for table in tables
        }
        for future in as_completed(future_to_table):
            table = future_to_table[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"Failed to download csv for {table}: {str(e)}")
                

if __name__ == "__main__":
    main()

