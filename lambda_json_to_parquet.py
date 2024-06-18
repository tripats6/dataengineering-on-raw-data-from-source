import awswrangler as wr
import pandas as pd
import urllib.parse
import os
import boto3

# Reading environment variables
os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']

def create_glue_database(database_name):
    glue_client = boto3.client('glue')
    try:
        # Create the Glue database if it doesn't exist
        response = glue_client.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'Database for storing cleaned YouTube data'
            }
        )
        print(f"Database {database_name} created successfully.")
    except glue_client.exceptions.AlreadyExistsException:
        # Handle the case where the database already exists
        print(f"Database {database_name} already exists.")
    except Exception as e:
        # Log and raise any other exceptions that occur
        print(f"Error creating database {database_name}: {str(e)}")
        raise e

def lambda_handler(event, context):
    # Extract the S3 bucket and key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        # Ensure the Glue database exists
        create_glue_database(os_input_glue_catalog_db_name)

        # Read the JSON file from S3 into a DataFrame
        df_raw = wr.s3.read_json(f's3://{bucket}/{key}')

        # Normalize the JSON structure to flatten nested fields
        df_step_1 = pd.json_normalize(df_raw['items'])

        # Write the cleaned DataFrame to S3 in Parquet format
        wr_response = wr.s3.to_parquet(
            df=df_step_1,
            path=os_input_s3_cleansed_layer,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation
        )

        return wr_response
    except Exception as e:
        # Log any exceptions that occur during processing
        print(e)
        print(f'Error getting object {key} from bucket {bucket}. Make sure they exist and your bucket is in the same region as this function.')
        raise e
