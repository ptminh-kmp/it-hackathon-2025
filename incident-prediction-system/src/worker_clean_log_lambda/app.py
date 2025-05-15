# src/worker_clean_log_lambda/app.py
import json
import os
from urllib.parse import unquote_plus
import boto3
import traceback
from datetime import datetime, timezone

from common_lib.config_loader import load_service_config # Sử dụng common_lib
from common_lib.service_specific_parsers import (
    process_esb_clean_logs,
    process_apigw_clean_logs,
    process_roc_clean_logs
)

S3_CLIENT = boto3.client('s3')
DYNAMODB_RESOURCE = boto3.resource('dynamodb')
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME_WORKER', 'IncidentPredictionBatchStates')
DYNAMODB_TABLE = DYNAMODB_RESOURCE.Table(DYNAMODB_TABLE_NAME)

# CLEAN_DATA_ROOT_S3_PREFIX cần được biết để tạo key tương đối chính xác cho DynamoDB
CLEAN_DATA_ROOT_S3_PREFIX = os.environ.get('CLEAN_DATA_ROOT_S3_PREFIX', "clean_data/")

CLEAN_LOG_PROCESSOR_MAP = {
    "service_esb": process_esb_clean_logs,
    "service_apigw": process_apigw_clean_logs,
    "service_roc": process_roc_clean_logs,
    # "unknown_service": process_generic_clean_log, 
}

def lambda_handler(event, context):
    print(f"WorkerCleanLogLambda started. Event: {json.dumps(event)}")

    batch_id = event.get('BatchID') # Sẽ là "incidentN"
    s3_bucket_name = event.get('S3Bucket')
    clean_log_file_info = event.get('CleanLogFile', {})
    s3_object_key_full = clean_log_file_info.get('S3Key') # Ví dụ: "clean_data/incident1/service_esb_file1.json"
    service_name = clean_log_file_info.get('ServiceNameFromFile', 'unknown_service')

    if not all([batch_id, s3_bucket_name, s3_object_key_full, service_name != 'unknown_service']):
        error_msg = f"Missing BatchID, S3Bucket, S3Key, or valid ServiceNameFromFile. Got ServiceName: {service_name}"
        print(f"ERROR WorkerCleanLogLambda: {error_msg}")
        raise ValueError(error_msg)

    print(f"WorkerCleanLogLambda: Processing clean file '{s3_object_key_full}' for Service '{service_name}', BatchID '{batch_id}'")

    # Tạo Sort Key cho DynamoDB: key của file log tương đối so với thư mục batch
    # Ví dụ: nếu s3_object_key_full là "clean_data/incident1/service_esb/esb_data.json"
    # và batch_folder_path là "clean_data/incident1/"
    # thì relative_s3_key_for_dynamo sẽ là "service_esb/esb_data.json"
    # Hoặc đơn giản là tên file nếu file nằm trực tiếp trong thư mục batch_id
    
    # Đường dẫn đầy đủ đến thư mục batch, ví dụ: "clean_data/incident1/"
    current_batch_folder_path = f"{CLEAN_DATA_ROOT_S3_PREFIX.rstrip('/')}/{batch_id}/"
    
    relative_s3_key_for_dynamo = None
    if s3_object_key_full.startswith(current_batch_folder_path):
        relative_s3_key_for_dynamo = s3_object_key_full[len(current_batch_folder_path):]
    else:
        # Nếu không khớp, có thể dùng tên file làm fallback, nhưng nên cảnh báo
        print(f"WARNING WorkerCleanLogLambda: Full S3 key '{s3_object_key_full}' does not start with expected batch folder path '{current_batch_folder_path}'. Using basename as fallback for DynamoDB sort key.")
        relative_s3_key_for_dynamo = os.path.basename(s3_object_key_full)


    service_config = load_service_config(service_name) 
    if not service_config:
        print(f"Warning WorkerCleanLogLambda: No specific config for service '{service_name}'. Using default/minimal processing.")
        service_config = {"service_name": service_name} 
    
    processor_function = CLEAN_LOG_PROCESSOR_MAP.get(service_name)
    if not processor_function:
        error_message = f"WorkerCleanLogLambda: No CLEAN log processor for service '{service_name}'. Skipping."
        print(f"ERROR WorkerCleanLogLambda: {error_message}")
        # Ghi lỗi vào DynamoDB
        DYNAMODB_TABLE.put_item(Item={
            'BatchID': batch_id, 'S3ObjectKey': relative_s3_key_for_dynamo, 'ServiceName': service_name,
            'FullS3Key': s3_object_key_full, 'ProcessingTimestamp': datetime.now(timezone.utc).isoformat(),
            'Status': 'ERROR_NO_PROCESSOR_FOR_CLEAN_LOG', 'ErrorMessage': error_message
        })
        raise ValueError(error_message)

    try:
        s3_response = S3_CLIENT.get_object(Bucket=s3_bucket_name, Key=s3_object_key_full)
        log_content_clean = s3_response['Body'].read().decode('utf-8', errors='replace')
        
        extracted_phenomena = processor_function(log_content_clean, service_config) 
        
        item_to_save = {
            'BatchID': batch_id,
            'S3ObjectKey': relative_s3_key_for_dynamo, # Key tương đối
            'ServiceName': service_name,
            'FullS3Key': s3_object_key_full,
            'ProcessingTimestamp': datetime.now(timezone.utc).isoformat(),
            'CleanLogPhenomenaJSON': json.dumps(extracted_phenomena, ensure_ascii=False),
            'Status': 'PROCESSED_CLEAN_LOG'
        }
        DYNAMODB_TABLE.put_item(Item=item_to_save)
        print(f"WorkerCleanLogLambda: Successfully processed clean log '{s3_object_key_full}' for BatchID '{batch_id}'.")
        
        return {
            "status": "success_clean_log", 
            "processed_s3_key": s3_object_key_full,
            "service_name": service_name
        }

    except Exception as e:
        # ... (xử lý lỗi và ghi vào DynamoDB như cũ) ...
        error_message = f"WorkerCleanLogLambda: Critical error processing clean file {s3_object_key_full} for '{service_name}': {str(e)}"
        print(f"ERROR WorkerCleanLogLambda: {error_message}")
        traceback.print_exc()
        try:
            DYNAMODB_TABLE.put_item(Item={
                'BatchID': batch_id, 'S3ObjectKey': relative_s3_key_for_dynamo or os.path.basename(s3_object_key_full),
                'ServiceName': service_name, 'FullS3Key': s3_object_key_full,
                'ProcessingTimestamp': datetime.now(timezone.utc).isoformat(),
                'Status': 'ERROR_PROCESSING_CLEAN_LOG', 'ErrorMessage': error_message,
                'ErrorTraceback': traceback.format_exc()
            })
        except Exception as db_error: print(f"ERROR WorkerCleanLogLambda: Could not write ERROR state to DynamoDB: {db_error}")
        raise e
