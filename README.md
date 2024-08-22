# Data Pipeline Project


## Project Overview
When building ETL pipelines, there are many services you need to incorporate in your architecture, each with numerous settings you need to configure.  Developing your pipeline as code introduces easy infrastructure management, less mistakes, better version control, flexibility in technologies employed and the cost benefits of developing locally.

This ETL pipeline leverages on AWS SDK for python (Boto3) to build a robust data pipeline as code, tying together various AWS services and third party frameworks, and finally deployed using AWS Glue ETL. Even though the architecture can be more concise, the goal is to explore the use of simple python code to setup cloud infrastructure, extract data, transform data and load data while building your knowledge about AWS services.

## Table of Contents
•	Introduction

•	Architecture Diagram

•	Setup Instructions

•	ETL Job Overview

•	Configuration Details

•	Code Snippets and Screenshots

•	CloudWatch Monitoring

•	Conclusion

•	Further Reading/References

## Architecture overview
The process involves setting up S3 buckets for storage and extracting covid-19 data from data lake to landing bucket, setting up Redshift data warehouse cluster, cataloging raw data with glue crawlers, querying data with Athena then staging filtered data, transforming data using PySpark, Pandas and AWS Wrangler, cataloging cleaned data and dynamically building database tables in Redshift based on a star schema design, and then finally loading data with Redshift copy command.  


![COVID19-ETL-ARCHITECTURE drawio](https://github.com/user-attachments/assets/635eda25-85ba-4c43-a4f8-de0389ffbed5)
                          _Figure 1. AWS Glue workflow architecture_


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

**•	Key Steps:**

  1. Initialize S3 clients.
  
  2. Create S3 buckets if they do not exist.
  
  3. Copy data from source to target buckets.
  
  4. Code Evolution: Transition from print statements to logging for better monitoring.
    
### JOB 2: Redshift Setup
**•	Purpose:** Set up the Redshift cluster and configure IAM roles.

**•	Key Steps:**

  1. Initialize Redshift and IAM clients.
  
  2. Create Redshift cluster.
  
  3. Configure IAM roles for Redshift to access S3.
  
  4. Code Evolution: Introduction of AWS Wrangler for efficient data handling.

### JOB 3: Crawl Raw Data
**•	Purpose:** Use Glue Crawlers to catalog raw data in S3.

**•	Key Steps:**

  1. Initialize Glue client.
  
  2. Create and start Glue Crawler.
  
  3. Monitor Crawler status and retrieve cataloged tables.
  
  4. Code Evolution: Use of GlueContext for better integration with Glue services.

### JOB 4: Stage Data
**•	Purpose:** Stage data in S3 for further processing.

**•	Key Steps:**

  1. Execute Athena queries to retrieve data.
  
  2. Store query results in S3.
  
  3. Code Evolution: Implementation of job.commit() for ensuring job completion.

### JOB 5: Clean Data
**•	Purpose:** Clean and transform data using Spark and Pandas.

**•	Key Steps:**

  1. Load data into DataFrames.
  
  2. Perform data cleaning and transformation.
  
  3. Save cleaned data to S3.
  
  4. Code Evolution: Enhanced error handling and retry mechanisms.

### JOB 6: Load Data
**•	Purpose:** Load cleaned data into Redshift.

**•	Key Steps:**

1. Initialize Redshift connection.
2. Copy data from S3 to Redshift tables.
3. Code Evolution: Improved logging and monitoring.
