# src/common_lib/config_loader.py
import json
import os
import boto3
import traceback

S3_CLIENT = boto3.client('s3')
CONFIG_BUCKET = os.environ.get('CONFIG_BUCKET_NAME') 
CONFIG_S3_PREFIX = os.environ.get('CONFIG_S3_PREFIX', 'it-system-config/service_configs/') # Phải có / ở cuối

def load_service_config(service_name: str):
    if not CONFIG_BUCKET:
        print("ERROR Config Loader: CONFIG_BUCKET_NAME environment variable not set.")
        return None
    if not service_name:
        print("ERROR Config Loader: service_name is empty.")
        return None

    prefix_to_use = CONFIG_S3_PREFIX
    if prefix_to_use and not prefix_to_use.endswith('/'): # Đảm bảo có /
        prefix_to_use += '/'
        
    config_key = f"{prefix_to_use}{service_name}.json"
    
    print(f"Config Loader: Attempting to load config from s3://{CONFIG_BUCKET}/{config_key}")
    try:
        response = S3_CLIENT.get_object(Bucket=CONFIG_BUCKET, Key=config_key)
        config_content = response['Body'].read().decode('utf-8')
        print(f"Config Loader: Successfully loaded config for service '{service_name}'.")
        return json.loads(config_content)
    except S3_CLIENT.exceptions.NoSuchKey:
        print(f"ERROR Config Loader: Config file not found for '{service_name}' at s3://{CONFIG_BUCKET}/{config_key}")
        return None
    except json.JSONDecodeError as json_err:
        print(f"ERROR Config Loader: Invalid JSON for '{service_name}' at s3://{CONFIG_BUCKET}/{config_key}. Error: {json_err}")
        return None
    except Exception as e:
        print(f"ERROR Config Loader: Unexpected error loading config for '{service_name}': {e}")
        traceback.print_exc()
        return None