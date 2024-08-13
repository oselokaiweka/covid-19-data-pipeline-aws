import sys
import time
import boto3
import logging
import psycopg2
import configparser
import pandas as pd
import awswrangler as wr
from io import StringIO
from pathlib import Path
from pyspark.sql import SparkSession
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from concurrent.futures import ThreadPoolExecutor, as_completed
  
spark = SparkSession.builder.master("local[*]").appName("Join").getOrCreate()

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
        job_output_s3Path = config.get('S3', 'JOB_OUTPUT_PATH')
        job_temp_path = config.get('S3', 'JOB_TEMP_PATH')
        schema_name = config.get('GLUE', 'SCHEMA_NAME')
        job_region = config.get('S3', 'JOB_REGION')
        
        
        # Set up the boto3 session
        session = boto3.Session(
            region_name=job_region,
            aws_access_key_id=aws_key, 
            aws_secret_access_key=aws_secret
        )
        
        # Initialize required boto3 clients
        job_s3_client = session.client('s3')
        glue_client = session.client('glue')
        redshift_client = session.client('redshift')
        ec2_client = session.resource('ec2')
        iam_client = session.client('iam')
        
        # Call function to load data into dataframes
        factCovid, dimHospital, dimRegion, dimDate = load_files_into_dataframes(job_staging_s3Path=job_staging_s3Path)
        print(factCovid.head(2))
        print(dimRegion.head(2))
        print(dimHospital.head(2))
        print(dimDate.head(2))
        
        dfs_dict = {
            'factCovid': factCovid,
            'dimHospital': dimHospital,
            'dimRegion': dimRegion,
            'dimDate': dimDate
        }
        
        # Load dataframes to s3 output bucket
        write_dfs_to_bucket(dfs_dict, job_output_s3Path)  
        
        # Generate redshift create table statements from dataframes
        sql_statements = create_redshift_tables(dfs_dict)

        
    except Exception as e:
        logger.error('Failed to process data and load into dataframes. Error: %s', e)
        raise
    
# Define function to load csv data into dataframes
def load_files_into_dataframes(job_staging_s3Path):
    
    # FACT TABLE >>>
    # Initilizing two dataframes to create the fact table according to star schema data model
    enigma_jhu = wr.s3.read_csv(fr"{job_staging_s3Path.rstrip('/')}/enigma_jhu.csv")
    rearc_covid_19_testing_data = wr.s3.read_csv(fr"{job_staging_s3Path.rstrip('/')}/rearc_covid_19_testing_data.csv")
    rearc_covid_19_testing_data['date'] = pd.to_datetime(rearc_covid_19_testing_data['date'], errors= 'coerce')

    
    # Select required columns and deduplicate records
    factCovid_1 = enigma_jhu[['fips', 'province_state', 'country_region', 'confirmed', 'deaths', 'recovered', 'active' ]].drop_duplicates(keep='first')
    factCovid_2 = rearc_covid_19_testing_data[['fips', 'date', 'positive', 'negative', 'hospitalizedcurrently', 'hospitalized', 'hospitalizeddischarged' ]].drop_duplicates(keep='first')
    
    # Merge (join) tables
    factCovid = pd.merge(factCovid_1, factCovid_2, on='fips', how='inner')


    # DIMENSION TABLE 1 >>>
    # Initializing dimHospital dataframe to create a dimension table
    rearc_usa_hospital_beds = wr.s3.read_csv(fr"{job_staging_s3Path.rstrip('/')}/rearc_usa_hospital_beds.csv")
    
    # Select required columns and deduplicate records
    dimHospital =  rearc_usa_hospital_beds[['fips', 'state_name', 'latitude', 'longtitude', 'hq_address', 'hospital_name', 'hospital_type', 'hq_city', 'hq_state']].drop_duplicates(keep='first')
    dimHospital = dimHospital.rename(columns={'longtitude': 'longitude'}) # fix typo in column name
    
    # Nulling invalid data types in numeric columns
    dimHospital['latitude'] = pd.to_numeric(dimHospital['latitude'], errors= 'coerce')
    dimHospital['longitude'] = pd.to_numeric(dimHospital['longitude'], errors= 'coerce')
    
    
    # DIMENSION TABLE 2 >>>
    # Initializing dimDate dataframe and dropping duplicates if any
    dimDate = rearc_covid_19_testing_data[['fips', 'date']].drop_duplicates(keep='first')
    
    # Modifying date format and adding year, mmonth and dayofweek numeric columns
    dimDate['date'] = pd.to_datetime(dimDate['date'], format='%Y%m%d')
    dimDate['year'] = dimDate['date'].dt.year
    dimDate['month'] = dimDate['date'].dt.month
    dimDate["day_of_week"] = dimDate['date'].dt.dayofweek
    
    # Modifying data types to match redshift data types, and nulling invalid data types
    dimDate['fips'] = dimDate['fips'].astype(float)
    dimDate['date'] = dimDate['date'].astype('datetime64[ns]')
    
    
    # DIMENSION TABLE 2 >>>
    # Initializing 2 dataframes with pyspark to handle larger data processing
    enigma_jhu = spark.read.csv(
        fr"{job_staging_s3Path.rstrip('/')}/enigma_jhu.csv", 
        header=True, 
        inferSchema=True
    )
    
    enigma_nytimes_data_in_usa = spark.read.csv(
        fr"{job_staging_s3Path.rstrip('/')}/enigma_nytimes_data_in_usa.csv", 
        header=True, 
        inferSchema=True
    )
    # Selecting required columns and dropping duplicate values
    dimRegion_1 = enigma_jhu.select('fips', 'province_state', 'country_region', 'latitude', 'longitude').distinct()
    dimRegion_2 = enigma_nytimes_data_in_usa.select('fips', 'county', 'state').distinct()
    
    # Redistribtion data across 5 partitions based on 'fips' column value to optimize join operation
    dimRegion_1 = dimRegion_1.repartition(5, 'fips')
    dimRegion_2 = dimRegion_2.repartition(5, 'fips')
    dimRegion_2 = dimRegion_2.withColumnRenamed('fips', 'fips2') 
    
    # Perform join (merge) operation on two dataframes on fips1 = fips2
    dimRegion = dimRegion_1.join(
        dimRegion_2, 
        dimRegion_1["fips"] == dimRegion_2["fips2"], 
        "inner"
    )
    # fips1 and fips2 columns are identical so dropping fips2
    dimRegion = dimRegion.drop('fips2')
    
    # Convert to pandas dataframe and modify fips colum data type to float
    dimRegion = dimRegion.toPandas()
    dimRegion['fips'] = dimRegion['fips'].astype(float) 
    
    # Force values with invalid data type to null in respective columns
    dimRegion['latitude'] = pd.to_numeric(dimRegion['latitude'], errors= 'coerce')
    dimRegion['longitude'] = pd.to_numeric(dimRegion['longitude'], errors= 'coerce')
    
    return factCovid, dimHospital, dimRegion, dimDate


def write_dfs_to_bucket(dfs_dict, s3path):
    for df_name, df in dfs_dict.items():
        wr.s3.to_csv(df, fr"{s3path}/{df_name}.csv", index=False)
    logger.info("Files succesfully uploated to output s3 bucket")
 
    
def create_redshift_tables(dfs_dict):
    # Construct CREATE TABLE SQL statements dynamically from pandas dataframe
    statements = {}
    for df_name, df in dfs_dict.items():
        # Generate the CREATE TABLE SQL statement
        sql = pd.io.sql.get_schema(df.reset_index(), df_name) + ";"
        sql_stage = pd.io.sql.get_schema(df.reset_index(), fr"{df_name}_stage") + ";"
        # Add the statement to the dictionary
        statements[fr"{df_name}Sql"] = sql
        statements[fr"{df_name}_stageSql"] = sql_stage
    # Print out all the statements (for debugging or logging purposes)
    for table_name, create_statement in statements.items():
        print(f"Table: {table_name}\nSQL Statement:\n{create_statement}\n")
    
    return statements

        

 
   

if __name__ == "__main__":
    main()