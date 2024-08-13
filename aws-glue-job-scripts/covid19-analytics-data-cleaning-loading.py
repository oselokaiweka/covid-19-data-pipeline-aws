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
        
        # Initialize redshift client to create tables from dataframes structures and copy data
        redshift_client = boto3.client(
            'redshift',
            region_name=job_region,
            aws_access_key_id=aws_key, 
            aws_secret_access_key=aws_secret
        )

        # Initialize ec2 client to acess VPC Endpoint
        ec2_client = boto3.resource(
            'ec2',
            region_name=job_region,
            aws_access_key_id=aws_key, 
            aws_secret_access_key=aws_secret
        )

        # Initialize iam client to obtain role arn for redshift
        iam_client = boto3.client(
            'iam',
            region_name=job_region,
            aws_access_key_id=aws_key, 
            aws_secret_access_key=aws_secret
        )
        
        # Call function to load data into dataframes
        factCovid, dimHospital, dimRegion, dimDate = load_files_into_dataframes(job_staging_s3Path=job_staging_s3Path)
        print(factCovid.head(2))
        print(dimRegion.head(2))
        print(dimHospital.head(2))
        print(dimDate.head(2))
        
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
    
if __name__ == "__main__":
    main()