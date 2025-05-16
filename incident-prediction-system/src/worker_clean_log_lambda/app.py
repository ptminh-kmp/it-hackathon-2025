# src/worker_clean_log_lambda/app.py
import json
import os
from urllib.parse import unquote_plus
import boto3
import traceback
from datetime import datetime, timezone

# Các import này sẽ được Lambda Layer cung cấp
from common_lib.config_loader import load_service_config
from common_lib.service_specific_parsers import (
    process_esb_logs, # Giả sử đây là process_esb_clean_logs
    process_apigw_logs, # Giả sử đây là process_apigw_clean_logs
    process_roc_logs    # Giả sử đây là process_roc_clean_logs
)

S3_CLIENT = boto3.client('s3')
DYNAMODB_RESOURCE = boto3.resource('dynamodb')
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME_WORKER', 'IncidentPredictionBatchStates')
DYNAMODB_TABLE = DYNAMODB_RESOURCE.Table(DYNAMODB_TABLE_NAME)

BATCH_ROOT_S3_PREFIX = os.environ.get('BATCH_ROOT_S3_PREFIX', "clean_data/")
# Biến môi trường MỚI cho nơi lưu trữ chi tiết xử lý trên S3
PROCESSED_DETAILS_S3_BUCKET = os.environ.get('PROCESSED_DETAILS_S3_BUCKET')
PROCESSED_DETAILS_S3_PREFIX = os.environ.get('PROCESSED_DETAILS_S3_PREFIX', "processed_log_details/")

CLEAN_LOG_PROCESSOR_MAP = {
    "service_esb": process_esb_logs, # Nên đổi tên hàm cho rõ là xử lý clean data
    "service_apigw": process_apigw_logs,
    "service_roc": process_roc_logs,
}

MAX_SUMMARY_ITEMS_FOR_DYNAMODB = int(os.environ.get("MAX_SUMMARY_ITEMS_FOR_DYNAMODB", "3")) # Số item tối đa cho tóm tắt lỗi/thông tin trong DynamoDB

def create_summary_from_phenomena(phenomena: dict, service_name: str) -> dict:
    """
    Tạo một bản tóm tắt nhỏ gọn từ extracted_phenomena để lưu vào DynamoDB.
    """
    if not isinstance(phenomena, dict):
        print(f"Warning WorkerCleanLogLambda: phenomena for {service_name} is not a dict, cannot create summary.")
        return {"service_name": service_name, "summary_error": "Invalid phenomena format"}

    # Lấy một vài ví dụ lỗi để đưa vào tóm tắt
    error_examples = []
    errors_summary_list = phenomena.get("errors_summary", [])
    if isinstance(errors_summary_list, list):
        for err_sum_item in errors_summary_list[:MAX_SUMMARY_ITEMS_FOR_DYNAMODB]:
            if isinstance(err_sum_item, str):
                error_examples.append(err_sum_item)
            elif isinstance(err_sum_item, dict): # Nếu errors_summary chứa dict
                error_examples.append(str(err_sum_item.get("message", err_sum_item))[:250] + "...")


    # Lấy một vài ví dụ other_key_info
    critical_info_examples = []
    other_key_info_list = phenomena.get("other_key_info", [])
    if isinstance(other_key_info_list, list):
        critical_info_examples = [str(info)[:250]+"..." for info in other_key_info_list[:MAX_SUMMARY_ITEMS_FOR_DYNAMODB]]


    summary = {
        "service_name": phenomena.get("service_name", service_name), # Ưu tiên service_name từ phenomena
        "total_error_events_in_file": len(errors_summary_list) if isinstance(errors_summary_list, list) else 0,
        "error_examples_in_summary": error_examples,
        "metrics_summary": phenomena.get("metrics_summary", {}), # Giữ lại toàn bộ metrics_summary vì nó thường nhỏ gọn
        "critical_info_examples_in_summary": critical_info_examples,
        "phenomena_keys_present": list(phenomena.keys()) # Để biết các key chính có trong file detail
    }
    return summary

def lambda_handler(event, context):
    print(f"WorkerCleanLogLambda started. Event: {json.dumps(event)}")

    batch_id = event.get('BatchID')
    s3_bucket_name_source = event.get('S3Bucket') 
    clean_log_file_info = event.get('CleanLogFile', {})
    s3_object_key_full_source = clean_log_file_info.get('S3Key')
    service_name = clean_log_file_info.get('ServiceNameFromFile', 'unknown_service')

    if not all([batch_id, s3_bucket_name_source, s3_object_key_full_source, service_name != 'unknown_service']):
        error_msg = f"WorkerCleanLogLambda: Missing BatchID, S3Bucket, S3Key, or valid ServiceNameFromFile. Got ServiceName: {service_name}. Event: {event}"
        print(f"ERROR WorkerCleanLogLambda: {error_msg}")
        raise ValueError(error_msg)

    if not PROCESSED_DETAILS_S3_BUCKET: # Kiểm tra biến môi trường mới
        error_msg = "WorkerCleanLogLambda: PROCESSED_DETAILS_S3_BUCKET env var not set. Cannot store detailed phenomena."
        print(f"ERROR WorkerCleanLogLambda: {error_msg}")
        raise EnvironmentError(error_msg) # Dùng lỗi cụ thể hơn


    print(f"WorkerCleanLogLambda: Processing clean file '{s3_object_key_full_source}' for Service '{service_name}', BatchID '{batch_id}'")

    current_batch_folder_path_in_clean_data = f"{BATCH_ROOT_S3_PREFIX.rstrip('/')}/{batch_id}/"
    relative_s3_key_for_dynamo = None
    if s3_object_key_full_source.startswith(current_batch_folder_path_in_clean_data):
        relative_s3_key_for_dynamo = s3_object_key_full_source[len(current_batch_folder_path_in_clean_data):]
    else:
        relative_s3_key_for_dynamo = os.path.basename(s3_object_key_full_source)
        print(f"Warning WorkerCleanLogLambda: Full S3 key '{s3_object_key_full_source}' does not precisely match expected batch path '{current_batch_folder_path_in_clean_data}'. Using basename '{relative_s3_key_for_dynamo}' for DynamoDB sort key.")

    service_config = load_service_config(service_name)
    if not service_config:
        error_message = f"WorkerCleanLogLambda: No config found for service '{service_name}' (from key '{s3_object_key_full_source}')."
        # Ghi lỗi vào DynamoDB
        DYNAMODB_TABLE.put_item(Item={
            'BatchID': batch_id, 'S3ObjectKey': relative_s3_key_for_dynamo, 'ServiceName': service_name,
            'FullSourceS3Key': s3_object_key_full_source, 'ProcessingTimestamp': datetime.now(timezone.utc).isoformat(),
            'Status': 'ERROR_NO_CONFIG_WORKER', 'ErrorMessage': error_message
        })
        raise ValueError(error_message)
    
    processor_function = CLEAN_LOG_PROCESSOR_MAP.get(service_name)
    if not processor_function:
        error_message = f"WorkerCleanLogLambda: No CLEAN log processor function mapped for service '{service_name}'."
        DYNAMODB_TABLE.put_item(Item={
            'BatchID': batch_id, 'S3ObjectKey': relative_s3_key_for_dynamo, 'ServiceName': service_name,
            'FullSourceS3Key': s3_object_key_full_source, 'ProcessingTimestamp': datetime.now(timezone.utc).isoformat(),
            'Status': 'ERROR_NO_PROCESSOR_WORKER', 'ErrorMessage': error_message
        })
        raise ValueError(error_message)

    s3_uri_for_details = None # Khởi tạo để có thể lưu vào DynamoDB ngay cả khi S3 put lỗi
    try:
        s3_response = S3_CLIENT.get_object(Bucket=s3_bucket_name_source, Key=s3_object_key_full_source)
        log_content_clean = s3_response['Body'].read().decode('utf-8', errors='replace')
        
        extracted_phenomena_full = processor_function(log_content_clean, service_config)
        
        # Tạo S3 key cho file chi tiết
        detail_file_name_parts = os.path.splitext(relative_s3_key_for_dynamo.replace('/', '_'))
        detail_file_name = f"{detail_file_name_parts[0]}_details.json"
        
        s3_key_for_details = f"{PROCESSED_DETAILS_S3_PREFIX.rstrip('/')}/{batch_id}/{service_name}/{detail_file_name}"
        s3_uri_for_details = f"s3://{PROCESSED_DETAILS_S3_BUCKET}/{s3_key_for_details}"

        try:
            S3_CLIENT.put_object(
                Bucket=PROCESSED_DETAILS_S3_BUCKET, Key=s3_key_for_details,
                Body=json.dumps(extracted_phenomena_full, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            print(f"WorkerCleanLogLambda: Successfully saved full phenomena to {s3_uri_for_details}")
        except Exception as s3_put_e:
            s3_put_error_msg = f"Failed to save full phenomena to S3 at {s3_uri_for_details}. Error: {s3_put_e}"
            print(f"ERROR WorkerCleanLogLambda: {s3_put_error_msg}")
            traceback.print_exc()
            # Ghi lỗi S3 put vào DynamoDB và vẫn raise lỗi để Step Functions retry/catch
            DYNAMODB_TABLE.put_item(Item={
                'BatchID': batch_id, 'S3ObjectKey': relative_s3_key_for_dynamo, 'ServiceName': service_name,
                'FullSourceS3Key': s3_object_key_full_source, 'ProcessingTimestamp': datetime.now(timezone.utc).isoformat(),
                'Status': 'ERROR_S3_PUT_DETAILS', 'ErrorMessage': s3_put_error_msg,
                'DetailedPhenomenaS3URI': "FAILED_TO_CREATE"
            })
            raise s3_put_e 

        phenomena_summary_for_dynamodb = create_summary_from_phenomena(extracted_phenomena_full, service_name)

        item_to_save = {
            'BatchID': batch_id,
            'S3ObjectKey': relative_s3_key_for_dynamo, 
            'ServiceName': service_name,
            'FullSourceS3Key': s3_object_key_full_source,
            'ProcessingTimestamp': datetime.now(timezone.utc).isoformat(),
            'PhenomenaSummaryJSON': json.dumps(phenomena_summary_for_dynamodb, ensure_ascii=False),
            'DetailedPhenomenaS3URI': s3_uri_for_details, 
            'Status': 'PROCESSED_WITH_S3_DETAILS'
        }
        
        item_size_bytes = len(json.dumps(item_to_save).encode('utf-8'))
        print(f"WorkerCleanLogLambda: Estimated DynamoDB item summary size for {relative_s3_key_for_dynamo}: {item_size_bytes} bytes.")
        MAX_DYNAMODB_ITEM_SIZE = 390 * 1024 
        if item_size_bytes > MAX_DYNAMODB_ITEM_SIZE:
            dynamo_size_error_msg = f"SUMMARY item size {item_size_bytes} bytes exceeds DynamoDB limit for {relative_s3_key_for_dynamo}. This indicates summary is still too large."
            print(f"ERROR WorkerCleanLogLambda: {dynamo_size_error_msg}")
            item_to_save['PhenomenaSummaryJSON'] = json.dumps({"error": "Summary too large for DynamoDB.", "original_error_count": phenomena_summary_for_dynamodb.get("total_error_events_in_file", "N/A") })
            item_to_save['Status'] = 'ERROR_DYNAMO_SUMMARY_SIZE'
            # Ghi item lỗi này vào DynamoDB
            DYNAMODB_TABLE.put_item(Item=item_to_save)
            raise ValueError(dynamo_size_error_msg) # Dừng ở đây nếu tóm tắt cũng quá lớn


        DYNAMODB_TABLE.put_item(Item=item_to_save)
        print(f"WorkerCleanLogLambda: Successfully processed '{s3_object_key_full_source}', saved summary to DynamoDB and details to S3.")
        
        return {
            "status": "success_worker_s3_details", 
            "processed_s3_key": s3_object_key_full_source,
            "service_name": service_name,
            "detail_s3_uri": s3_uri_for_details # Trả về S3 URI của file detail
        }

    except Exception as e:
        error_message = f"WorkerCleanLogLambda: Critical error processing clean file {s3_object_key_full_source} for '{service_name}': {str(e)}"
        print(f"ERROR WorkerCleanLogLambda: {error_message}")
        traceback.print_exc()
        try:
            DYNAMODB_TABLE.put_item(Item={
                'BatchID': batch_id, 
                'S3ObjectKey': relative_s3_key_for_dynamo or os.path.basename(s3_object_key_full_source),
                'ServiceName': service_name, 
                'FullSourceS3Key': s3_object_key_full_source,
                'ProcessingTimestamp': datetime.now(timezone.utc).isoformat(),
                'Status': 'ERROR_PROCESSING_CLEAN_LOG', 
                'DetailedPhenomenaS3URI': s3_uri_for_details if s3_uri_for_details else "NOT_CREATED_DUE_TO_ERROR",
                'ErrorMessage': error_message,
                'ErrorTraceback': traceback.format_exc()
            })
        except Exception as db_error: print(f"ERROR WorkerCleanLogLambda: Could not write CRITICAL ERROR state to DynamoDB: {db_error}")
        raise e # Raise lại lỗi để Step Functions Map state biết iteration này thất bại
