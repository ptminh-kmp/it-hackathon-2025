# src/s3_clean_data_trigger_lambda/app.py
import json
import os
import boto3
from urllib.parse import unquote_plus
import traceback
from datetime import datetime

SFN_CLIENT = boto3.client('stepfunctions')
STATE_MACHINE_ARN = os.environ.get('STATE_MACHINE_ARN_CLEAN_DATA') # ARN của State Machine mới
CLEAN_DATA_ROOT_S3_PREFIX = os.environ.get('CLEAN_DATA_ROOT_S3_PREFIX', "clean_data/") # Phải có / ở cuối

def lambda_handler(event, context):
    print(f"S3CleanDataTriggerLambda started. Event: {json.dumps(event)}")

    if not STATE_MACHINE_ARN:
        error_msg = "ERROR S3CleanDataTriggerLambda: STATE_MACHINE_ARN_CLEAN_DATA env var not set."
        print(error_msg)
        return {"statusCode": 500, "body": error_msg}

    for record in event.get('Records', []):
        if record.get('eventSource') != 'aws:s3':
            continue

        s3_bucket_name = record['s3']['bucket']['name']
        flag_file_key = unquote_plus(record['s3']['object']['key']) # Ví dụ: clean_data/20250515143000/_BATCH_PROCESSED.txt

        # Chỉ trigger bởi file cờ cụ thể, ví dụ _BATCH_PROCESSED.txt
        if not flag_file_key.endswith("_BATCH_PROCESSED.txt"):
            print(f"S3CleanDataTriggerLambda: File '{flag_file_key}' is not a _BATCH_PROCESSED.txt file. Skipping.")
            continue

        print(f"S3CleanDataTriggerLambda: Detected flag file: s3://{s3_bucket_name}/{flag_file_key}")

        # Suy ra đường dẫn thư mục batch và BatchID
        # flag_file_key = "clean_data/20250515143000/_BATCH_PROCESSED.txt"
        # batch_folder_path sẽ là "clean_data/20250515143000/"
        # batch_id sẽ là "20250515143000"
        
        batch_folder_path = os.path.dirname(flag_file_key)
        if not batch_folder_path.endswith('/'):
            batch_folder_path += "/"
            
        batch_id = os.path.basename(os.path.dirname(flag_file_key)) # Lấy tên thư mục làm BatchID

        if not batch_folder_path.startswith(CLEAN_DATA_ROOT_S3_PREFIX) or not batch_id:
            print(f"ERROR S3CleanDataTriggerLambda: Could not determine valid batch folder or BatchID from '{flag_file_key}' using CLEAN_DATA_ROOT_S3_PREFIX '{CLEAN_DATA_ROOT_S3_PREFIX}'. Skipping.")
            continue
            
        sfn_input = {
            "BatchID": batch_id,
            "S3Bucket": s3_bucket_name,
            "BatchFolderPath": batch_folder_path # Ví dụ: "clean_data/20250515143000/"
        }

        execution_name = f"CleanDataProcessing-{batch_id}-{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

        try:
            print(f"S3CleanDataTriggerLambda: Starting Step Functions execution '{execution_name}' for BatchID '{batch_id}' with input: {json.dumps(sfn_input)}")
            response = SFN_CLIENT.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                name=execution_name, 
                input=json.dumps(sfn_input)
            )
            print(f"S3CleanDataTriggerLambda: Successfully started Step Functions execution. ARN: {response['executionArn']}")
        except Exception as e:
            print(f"ERROR S3CleanDataTriggerLambda: Failed to start Step Functions for BatchID '{batch_id}'. Error: {e}")
            traceback.print_exc()

    return {'statusCode': 200, 'body': json.dumps('S3CleanDataTriggerLambda finished.')}