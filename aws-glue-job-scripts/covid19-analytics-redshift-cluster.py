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

# Data Warehouse configuration
cluster_type = config.get('DWH', 'DWH_CLUSTER_TYPE')
num_nodes = config.get('DWH', 'DWH_NUM_NODES')
node_type = config.get('DWH', 'DWH_NODE_TYPE')
cluster_identifier = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
db_name = config.get('DWH', 'DWH_DB')
db_user = config.get('DWH', 'DWH_DB_USER')
db_password = config.get('DWH', 'DWH_DB_PASSWORD')
cluster_port = config.get('DWH', 'DWH_PORT')
iam_role_name = config.get('DWH', 'DWH_IAM_ROLE_NAME')

job_region = config.get('S3', 'JOB_REGION')

# Initialize IAM client to retrieve iam role for the redshift cluster
iam_client = boto3.client(
    'iam',
    region_name=job_region,
    aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret
)

# Initialize redshift client to create, access or delete cluster
redshift_client = boto3.client(
    'redshift',
    region_name=job_region,
    aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret
)

# Configure iam role for redshift to access s3 via console
s3AccessRoleArn = iam_client.get_role(RoleName=iam_role_name)['Role']['Arn'] # Retrieve roleArn

# 
try:
    response = redshift_client.create_cluster(
        # redshift sdk create_cluster documentation:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift/client/create_cluster.html
        ClusterType=cluster_type,
        NodeType=node_type,

        # Identifiers & Credentials
        DBName=db_name,
        ClusterIdentifier=cluster_identifier,
        MasterUsername=db_user,
        MasterUserPassword=db_password,
        Port=int(cluster_port), # Type cast as integer
        NumberOfNodes=int(num_nodes), # Type cast as integer
        Tags=[
            {'Key': 'PROJECT', 'Value': 'COVID19-DATA-ANALYTICS'},
        ],
        # Roles
        IamRoles=[s3AccessRoleArn],
    )
except ClientError as e:
    print(e)
except Exception as e:
    print(e)