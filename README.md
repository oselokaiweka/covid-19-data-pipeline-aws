# Data Pipeline Project


## Project Overview
When building ETL pipelines, there are many services you need to incorporate in your architecture, each with numerous settings you need to configure.  Developing your pipeline as code introduces easy infrastructure management, less mistakes, better version control, flexibility in technologies employed and the cost benefits of developing locally.

This ETL pipeline leverages on AWS SDK for python (Boto3) to build a robust data pipeline as code, tying together various AWS services and third party frameworks, and finally deployed using AWS Glue ETL. Even though the architecture can be more concise, the goal is to explore the use of simple python code to setup cloud infrastructure, extract data, transform data and load data while building your knowledge about AWS services.

## Table of Contents
•	Introduction

•	Understanding The Data

•	Architecture Diagram

•	Setup Instructions

•	ETL Job Overview

•	Configuration Details

•	Code Snippets and Screenshots

•	CloudWatch Monitoring

•	Conclusion

•	Further Reading/References


## Understanding the data
The project data is sourced from AWS COVID-19 data lake via AWS Data Exchange. The AWS COVID-19 data lake is a centraized repository that offers free up-to-date and curated dataset focused on the spread and characteristiics of the coronavirus. The data lake contains datasets such as COVID-19 case tracking, testing data, hospital bed availability and other research data. 

The goal is to create a data warehouse star schema with one facts table that contains Covid-19 case tracking data and three dimensions tables thatt contain information about locations, dates and hospital records for each facts record using the 'fips' ID column to join the records. See the before and after Entity Relationship Diagram ERD, below. 
 
![covid19-data-model drawio](https://github.com/user-attachments/assets/fd7692ff-eaac-4fa9-80a6-9340bfc1db81)

_Figure 1. Top: Extrracted Data Model, Bottom: Transformed Data Model_

## Architecture overview
The process involves setting up S3 buckets for storage and extracting covid-19 data from data lake to landing bucket, setting up Redshift data warehouse cluster, cataloging raw data with glue crawlers, querying data with Athena then staging filtered data, transforming data using PySpark, Pandas and AWS Wrangler, cataloging cleaned data and dynamically building database tables in Redshift based on a star schema design, and then finally loading data with Redshift copy command.  


![COVID19-ETL-ARCHITECTURE drawio](https://github.com/user-attachments/assets/635eda25-85ba-4c43-a4f8-de0389ffbed5)
                          _Figure 2. AWS Glue workflow architecture_


#
## Setup Instructions
1.	**Cloud-Based IDE Recommendation:**
	
    Use a cloud-based IDE such as GitHub CodeSpaces or AWS Cloud9.

    Advantages include ease of collaboration, automatic environment setup, integrated tools for managing AWS resources.

2.  **Required Libraries:**
    Ensure the following libraries are installed:

        boto3==1.20.24
        awswrangler==2.10.0
        pandas==1.3.3
        psycopg2-binary==2.4.0

3.	**Config File Details:**
    Create a covid19-analytics.config file to store AWS credentials and environment variables. Store this file in a secure and encrypted S3 bucket. In the Glue ETL console, navigate to job details and configure the job parameters with the config file bucket name and the file key. The script uses the getResolvedOptions function to access Glue parameters in order to access the config file and retrieve environment variables including your credentials. Remember to create an IAM role with the appriopriate policies that when granted to AWS Glue, will allow access your S3 buckets and other relevant services. 

    Below is an example of what the IAM role policy will look like:

        {
          "Version": "2012-10-17", 
          "Statement": [
            {
              "Effect": "Allow", 
              "Action": [
                "s3:GetObject",
                "s3:PutObject"
              ],
              "Resource": "arn:aws:s3:::your-bucket-name/*"
            }
          ]
        }

## ETL Job Overview
### JOB 1: Bucket Setup
**•	Purpose:** Create and configure S3 buckets for raw and processed data.

**• View the full code:** https://github.com/oselokaiweka/covid-19-data-pipeline-aws/blob/main/aws-glue-job-scripts/covid19-analytics-setup.py

**•	Key Steps:**

  1. Initialize S3 clients.
  
  2. Create S3 buckets if they do not exist.
  
  3. Copy data from source to target buckets.
  
  4. Code Evolution: Transition from print statements to logging for better monitoring.
    
### JOB 2: Redshift Setup
**•	Purpose:** Set up the Redshift cluster and configure IAM roles.

**• View the full code:** https://github.com/oselokaiweka/covid-19-data-pipeline-aws/blob/main/aws-glue-job-scripts/covid19-analytics-redshift-cluster.py

**•	Key Steps:**

  1. Initialize Redshift and IAM clients.
  
  2. Create Redshift cluster.
  
  3. Configure IAM roles for Redshift to access S3.
  
  4. Code Evolution: Introduction of AWS Wrangler for efficient data handling.

### JOB 3: Crawl Raw Data
**•	Purpose:** Use Glue Crawlers to catalog raw data in S3.

**• View the full code:** https://github.com/oselokaiweka/covid-19-data-pipeline-aws/blob/main/aws-glue-job-scripts/covid19-analytics-glue-data-catalog.py

**•	Key Steps:**

  1. Initialize Glue client.
  
  2. Create and start Glue Crawler.
  
  3. Monitor Crawler status and retrieve cataloged tables.
  
  4. Code Evolution: Use of GlueContext for better integration with Glue services.

### JOB 4: Stage Data
**•	Purpose:** Stage data in S3 for further processing.

**• View the full code:** https://github.com/oselokaiweka/covid-19-data-pipeline-aws/blob/main/aws-glue-job-scripts/covid19-analytics-data-staging.py

**•	Key Steps:**

  1. Execute Athena queries to retrieve data.
  
  2. Store query results in S3.
  
  3. Code Evolution: Implementation of job.commit() for ensuring job completion.

### JOB 5: Clean Data
**•	Purpose:** Clean and transform data using Spark and Pandas.

**• View the full code:** https://github.com/oselokaiweka/covid-19-data-pipeline-aws/blob/main/aws-glue-job-scripts/covid19-analytics-data-cleaning-loading.py

**•	Key Steps:**

  1. Load data into DataFrames.
  
  2. Perform data cleaning and transformation.
  
  3. Save cleaned data to S3.
  
  4. Code Evolution: Enhanced error handling and retry mechanisms.

### JOB 6: Load Data
**•	Purpose:** Load cleaned data into Redshift.

**• View the full code:** https://github.com/oselokaiweka/covid-19-data-pipeline-aws/blob/main/aws-glue-job-scripts/covid19-analytics-data-loading.py

**•	Key Steps:**

1. Initialize Redshift connection.
2. Copy data from S3 to Redshift tables.
3. Code Evolution: Improved logging and monitoring.
