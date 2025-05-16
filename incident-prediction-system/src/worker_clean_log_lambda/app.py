# src/worker_clean_log_lambda/app.py
import json
import os
from urllib.parse import unquote_plus
import boto3
import traceback
from datetime import datetime, timezone

# Các import này sẽ được Lambda Layer cung cấp
# Đảm bảo rằng common_lib nằm trong sys.path của Lambda Layer
# (thường là thư mục python/ trong file zip của Layer, và code common_lib nằm trong python/common_lib/)
from common_lib.config_loader import load_service_config
from common_lib.service_specific_parsers import (
    process_esb_clean_logs, # Đây là hàm xử lý clean data của ESB
    process_apigw_clean_logs, # Đây là hàm xử lý clean data của APIGW
    process_roc_clean_logs    # Đây là hàm xử lý clean data của ROC
    # Thêm các processor khác nếu có
)

S3_CLIENT = boto3.client('s3')
DYNAMODB_RESOURCE = boto3.resource('dynamodb') # Sửa thành resource để dùng Table object
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME_WORKER', 'IncidentPredictionBatchStates')
DYNAMODB_TABLE = DYNAMODB_RESOURCE.Table(DYNAMODB_TABLE_NAME)


BATCH_ROOT_S3_PREFIX = os.environ.get('BATCH_ROOT_S3_PREFIX', "clean_data/") # Nơi chứa các thư mục batch incidentN
# Biến môi trường cho nơi lưu trữ chi tiết xử lý trên S3
PROCESSED_DETAILS_S3_BUCKET = os.environ.get('PROCESSED_DETAILS_S3_BUCKET')
PROCESSED_DETAILS_S3_PREFIX = os.environ.get('PROCESSED_DETAILS_S3_PREFIX', "processed_log_details/")

CLEAN_LOG_PROCESSOR_MAP = {
    "service_esb": process_esb_clean_logs, # Sử dụng hàm đã được cập nhật để xử lý clean data
    "service_apigw": process_apigw_clean_logs, # Sử dụng hàm đã được cập nhật
    "service_roc": process_roc_clean_logs,     # Sử dụng hàm đã được cập nhật
    # "unknown_service": process_generic_clean_log, # Fallback nếu cần
}

MAX_SUMMARY_ITEMS_FOR_DYNAMODB = int(os.environ.get("MAX_SUMMARY_ITEMS_FOR_DYNAMODB", "5"))
MAX_METRIC_KEYS_IN_SUMMARY = int(os.environ.get("MAX_METRIC_KEYS_IN_SUMMARY", "10"))
MAX_STRING_LENGTH_IN_SUMMARY = int(os.environ.get("MAX_STRING_LENGTH_IN_SUMMARY", "200"))


def create_summary_from_phenomena(phenomena: dict, service_name: str) -> dict:
    """
    Tạo một bản tóm tắt nhỏ gọn từ extracted_phenomena để lưu vào DynamoDB.
    Hàm này được cập nhật để giới hạn kích thước hiệu quả hơn.
    """
    if not isinstance(phenomena, dict):
        print(f"Warning WorkerCleanLogLambda: phenomena for {service_name} is not a dict, cannot create summary.")
        return {"service_name": service_name, "_summary_error": "Invalid phenomena format"}

    # Tóm tắt errors_summary
    error_examples = []
    errors_summary_list = phenomena.get("errors_summary", [])
    if isinstance(errors_summary_list, list):
        for err_sum_item in errors_summary_list[:MAX_SUMMARY_ITEMS_FOR_DYNAMODB]:
            item_str = str(err_sum_item)
            error_examples.append(item_str[:MAX_STRING_LENGTH_IN_SUMMARY] + ("..." if len(item_str) > MAX_STRING_LENGTH_IN_SUMMARY else ""))

    # Tóm tắt other_key_info
    critical_info_examples = []
    other_key_info_list = phenomena.get("other_key_info", [])
    if isinstance(other_key_info_list, list):
        critical_info_examples = [(str(info)[:MAX_STRING_LENGTH_IN_SUMMARY] + ("..." if len(str(info)) > MAX_STRING_LENGTH_IN_SUMMARY else str(info))) for info in other_key_info_list[:MAX_SUMMARY_ITEMS_FOR_DYNAMODB]]

    # Tóm tắt metrics_summary
    original_metrics_summary = phenomena.get("metrics_summary", {})
    compact_metrics_summary = {}
    if isinstance(original_metrics_summary, dict):
        for key, value in original_metrics_summary.items():
            if isinstance(value, dict) and len(value) > MAX_METRIC_KEYS_IN_SUMMARY: # Nếu là dict đếm (ví dụ từ count_by_field_value)
                print(f"WorkerCleanLogLambda: Metric '{key}' for {service_name} has {len(value)} items, truncating to top {MAX_METRIC_KEYS_IN_SUMMARY}.")
                sorted_items = sorted(value.items(), key=lambda item: item[1], reverse=True)
                top_n_items = dict(sorted_items[:MAX_METRIC_KEYS_IN_SUMMARY])
                others_sum = sum(val for _, val in sorted_items[MAX_METRIC_KEYS_IN_SUMMARY:]) # Chỉ sum nếu value là số
                if others_sum > 0:
                    top_n_items["_others_count_"] = others_sum # Tên key rõ ràng hơn
                compact_metrics_summary[key] = top_n_items
            elif isinstance(value, list) and len(value) > MAX_METRIC_KEYS_IN_SUMMARY: # Nếu là list dài
                 print(f"WorkerCleanLogLambda: Metric list '{key}' for {service_name} has {len(value)} items, truncating to first {MAX_METRIC_KEYS_IN_SUMMARY}.")
                 compact_metrics_summary[key] = [str(v)[:MAX_STRING_LENGTH_IN_SUMMARY] for v in value[:MAX_METRIC_KEYS_IN_SUMMARY]]
                 compact_metrics_summary[key].append(f"... (and {len(value) - MAX_METRIC_KEYS_IN_SUMMARY} more)")
            else: # Giữ nguyên các metric khác (ví dụ: số, chuỗi ngắn, dict/list nhỏ)
                if isinstance(value, str) and len(value) > MAX_STRING_LENGTH_IN_SUMMARY * 2: # Giới hạn chuỗi dài trong metric
                    compact_metrics_summary[key] = value[:MAX_STRING_LENGTH_IN_SUMMARY*2] + "..."
                else:
                    compact_metrics_summary[key] = value
    
    summary = {
        "service_name": phenomena.get("service_name", service_name),
        "total_error_events_in_file": len(errors_summary_list) if isinstance(errors_summary_list, list) else phenomena.get("metrics_summary", {}).get("esb_total_error_lines_in_file", 0),
        "error_examples_in_summary": error_examples,
        "metrics_summary_compact": compact_metrics_summary, # Sử dụng metrics đã tóm gọn
        "critical_info_examples_in_summary": critical_info_examples,
        "_phenomena_keys_in_detail": list(phenomena.keys()) # Các key chính có trong file S3 detail
    }
    return summary


def lambda_handler(event, context):
    """
    WorkerCleanLogLambda: Xử lý một file log ĐÃ CLEAN.
    Input event từ Step Functions Map state:
    {
      "BatchID": "incidentN", 
      "S3Bucket": "write-team19-bucket",
      "CleanLogFile": {"S3Key": "clean_data/incidentN/service_esb_file1.json", "ServiceNameFromFile": "service_esb"}
    }
    """
    print(f"WorkerCleanLogLambda started. Event: {json.dumps(event)}")

    batch_id = event.get('BatchID')
    s3_bucket_name_source = event.get('S3Bucket') 
    clean_log_file_info = event.get('CleanLogFile', {})
    s3_object_key_full_source = clean_log_file_info.get('S3Key')
    service_name = clean_log_file_info.get('ServiceNameFromFile', 'unknown_service')

    # Kiểm tra input cơ bản
    if not all([batch_id, s3_bucket_name_source, s3_object_key_full_source]):
        error_msg = f"WorkerCleanLogLambda: Missing BatchID, S3Bucket, or S3Key in input. Event: {event}"
        print(f"ERROR WorkerCleanLogLambda: {error_msg}")
        raise ValueError(error_msg) # Raise lỗi để Step Functions Map state biết iteration này thất bại
    if service_name == 'unknown_service':
        error_msg = f"WorkerCleanLogLambda: ServiceNameFromFile is 'unknown_service' or missing for S3Key '{s3_object_key_full_source}'. Event: {event}"
        print(f"ERROR WorkerCleanLogLambda: {error_msg}")
        # Ghi nhận lỗi này vào DynamoDB nếu có thể để dễ truy vết file nào bị bỏ qua
        # Hoặc raise ValueError tùy theo chiến lược xử lý lỗi của bạn
        # For now, let's try to proceed but log it as a potential issue for aggregation
        # Better: Mark as error and stop for this file.
        DYNAMODB_TABLE.put_item(Item={
            'BatchID': batch_id, 'S3ObjectKey': os.path.basename(s3_object_key_full_source) + "_UNKNOWNSERVICE",
            'ServiceName': service_name, 'FullSourceS3Key': s3_object_key_full_source,
            'ProcessingTimestamp': datetime.now(timezone.utc).isoformat(),
            'Status': 'ERROR_UNKNOWN_SERVICE_NAME_WORKER', 'ErrorMessage': error_msg
        })
        raise ValueError(error_msg)


    # Kiểm tra biến môi trường cho S3 details
    if not PROCESSED_DETAILS_S3_BUCKET:
        error_msg = "WorkerCleanLogLambda: PROCESSED_DETAILS_S3_BUCKET env var not set. Cannot store detailed phenomena."
        print(f"ERROR WorkerCleanLogLambda: {error_msg}")
        # Ghi lỗi vào DynamoDB
        DYNAMODB_TABLE.put_item(Item={
            'BatchID': batch_id, 'S3ObjectKey': os.path.basename(s3_object_key_full_source),
            'ServiceName': service_name, 'FullSourceS3Key': s3_object_key_full_source,
            'ProcessingTimestamp': datetime.now(timezone.utc).isoformat(),
            'Status': 'ERROR_CONFIG_S3_DETAILS_WORKER', 'ErrorMessage': error_msg
        })
        raise EnvironmentError(error_msg)


    print(f"WorkerCleanLogLambda: Processing clean file '{s3_object_key_full_source}' for Service '{service_name}', BatchID '{batch_id}'")

    # Tạo Sort Key cho DynamoDB: key của file log tương đối so với thư mục batch
    current_batch_folder_path_in_clean_data = f"{BATCH_ROOT_S3_PREFIX.rstrip('/')}/{batch_id}/"
    relative_s3_key_for_dynamo = None
    if s3_object_key_full_source.startswith(current_batch_folder_path_in_clean_data):
        relative_s3_key_for_dynamo = s3_object_key_full_source[len(current_batch_folder_path_in_clean_data):]
    else:
        # Nếu key không khớp prefix mong đợi, có thể là lỗi cấu hình hoặc logic gọi.
        # Dùng tên file làm fallback nhưng cảnh báo rõ.
        relative_s3_key_for_dynamo = os.path.basename(s3_object_key_full_source)
        print(f"WARNING WorkerCleanLogLambda: Full S3 key '{s3_object_key_full_source}' does not start with expected batch path '{current_batch_folder_path_in_clean_data}'. Using basename '{relative_s3_key_for_dynamo}' for DynamoDB sort key. This might indicate an issue.")

    service_config = load_service_config(service_name)
    if not service_config:
        error_message = f"WorkerCleanLogLambda: No config found for service '{service_name}' (from key '{s3_object_key_full_source}')."
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

    s3_uri_for_details_file = None # Khởi tạo
    try:
        s3_response = S3_CLIENT.get_object(Bucket=s3_bucket_name_source, Key=s3_object_key_full_source)
        log_content_clean = s3_response['Body'].read().decode('utf-8', errors='replace')
        
        # extracted_phenomena_full là dict đầy đủ chứa tất cả thông tin phân tích từ file clean log
        extracted_phenomena_full = processor_function(log_content_clean, service_config)
        if not isinstance(extracted_phenomena_full, dict): # Parser phải trả về dict
            raise TypeError(f"Processor for {service_name} did not return a dictionary.")

        
        # Tạo S3 key cho file chi tiết
        # Đảm bảo tên file không chứa ký tự không hợp lệ cho S3 key và dễ đọc
        sanitized_relative_key = relative_s3_key_for_dynamo.replace('/', '_').replace('\\', '_')
        detail_file_name_base, detail_file_ext = os.path.splitext(sanitized_relative_key)
        detail_file_name = f"{detail_file_name_base}_details.json" # Thêm _details để phân biệt
        
        # Cấu trúc thư mục con trong processed_details: batch_id/service_name/filename_details.json
        s3_key_for_details_file = f"{PROCESSED_DETAILS_S3_PREFIX.rstrip('/')}/{batch_id}/{service_name}/{detail_file_name}"
        s3_uri_for_details_file = f"s3://{PROCESSED_DETAILS_S3_BUCKET}/{s3_key_for_details_file}"

        try:
            S3_CLIENT.put_object(
                Bucket=PROCESSED_DETAILS_S3_BUCKET, 
                Key=s3_key_for_details_file,
                Body=json.dumps(extracted_phenomena_full, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            print(f"WorkerCleanLogLambda: Successfully saved full phenomena to {s3_uri_for_details_file}")
        except Exception as s3_put_e:
            s3_put_error_msg = f"Failed to save full phenomena to S3 at {s3_uri_for_details_file}. Error: {s3_put_e}"
            print(f"ERROR WorkerCleanLogLambda: {s3_put_error_msg}")
            traceback.print_exc()
            # Ghi lỗi S3 put vào DynamoDB và vẫn raise lỗi để Step Functions retry/catch
            DYNAMODB_TABLE.put_item(Item={
                'BatchID': batch_id, 'S3ObjectKey': relative_s3_key_for_dynamo, 'ServiceName': service_name,
                'FullSourceS3Key': s3_object_key_full_source, 'ProcessingTimestamp': datetime.now(timezone.utc).isoformat(),
                'Status': 'ERROR_S3_PUT_DETAILS_WORKER', 'ErrorMessage': s3_put_error_msg,
                'DetailedPhenomenaS3URI': "FAILED_TO_CREATE_S3_DETAIL_FILE"
            })
            raise s3_put_e 

        # Tạo bản tóm tắt để lưu vào DynamoDB
        phenomena_summary_for_dynamodb = create_summary_from_phenomena(extracted_phenomena_full, service_name)

        item_to_save = {
            'BatchID': batch_id,
            'S3ObjectKey': relative_s3_key_for_dynamo, 
            'ServiceName': service_name,
            'FullSourceS3Key': s3_object_key_full_source, 
            'ProcessingTimestamp': datetime.now(timezone.utc).isoformat(),
            'PhenomenaSummaryJSON': json.dumps(phenomena_summary_for_dynamodb, ensure_ascii=False), 
            'DetailedPhenomenaS3URI': s3_uri_for_details_file, 
            'Status': 'PROCESSED_S3_DETAILS_V2' # Trạng thái mới rõ ràng hơn
        }
        
        item_size_bytes = len(json.dumps(item_to_save, ensure_ascii=False).encode('utf-8'))
        print(f"WorkerCleanLogLambda: Estimated DynamoDB item summary size for {relative_s3_key_for_dynamo}: {item_size_bytes} bytes.")
        
        MAX_DYNAMODB_ITEM_SIZE = 390 * 1024 # Để một chút buffer, giới hạn là 400KB
        if item_size_bytes > MAX_DYNAMODB_ITEM_SIZE:
            dynamo_size_error_msg = f"SUMMARY item size {item_size_bytes} bytes exceeds DynamoDB limit for {relative_s3_key_for_dynamo}. This indicates summary is still too large despite efforts."
            print(f"ERROR WorkerCleanLogLambda: {dynamo_size_error_msg}")
            # Nếu tóm tắt vẫn quá lớn, chỉ lưu S3 URI và thông báo lỗi
            item_to_save['PhenomenaSummaryJSON'] = json.dumps({
                "_error_summary_too_large": True, 
                "message": dynamo_size_error_msg,
                "original_error_count_in_file": phenomena_summary_for_dynamodb.get("total_error_events_in_file", "N/A") 
            })
            item_to_save['Status'] = 'ERROR_DYNAMO_SUMMARY_SIZE_WORKER'
            # Ghi item lỗi này vào DynamoDB
            DYNAMODB_TABLE.put_item(Item=item_to_save)
            raise ValueError(dynamo_size_error_msg) # Dừng ở đây nếu tóm tắt cũng quá lớn

        DYNAMODB_TABLE.put_item(Item=item_to_save)
        print(f"WorkerCleanLogLambda: Successfully processed '{s3_object_key_full_source}', saved summary to DynamoDB and details to S3.")
        
        return { # Output cho Step Functions Map state
            "status": "success_worker_s3_details", 
            "processed_s3_key": s3_object_key_full_source,
            "service_name": service_name,
            "detail_s3_uri": s3_uri_for_details_file 
        }

    except Exception as e:
        error_message = f"WorkerCleanLogLambda: Critical error processing clean file '{s3_object_key_full_source}' for service '{service_name}': {str(e)}"
        print(f"ERROR WorkerCleanLogLambda: {error_message}")
        traceback.print_exc()
        try:
            DYNAMODB_TABLE.put_item(Item={
                'BatchID': batch_id, 
                'S3ObjectKey': relative_s3_key_for_dynamo or os.path.basename(s3_object_key_full_source), # Fallback
                'ServiceName': service_name, 
                'FullSourceS3Key': s3_object_key_full_source,
                'ProcessingTimestamp': datetime.now(timezone.utc).isoformat(),
                'Status': 'ERROR_CRITICAL_WORKER', 
                'DetailedPhenomenaS3URI': s3_uri_for_details_file if s3_uri_for_details_file else "NOT_CREATED_DUE_TO_CRITICAL_ERROR",
                'ErrorMessage': error_message,
                'ErrorTraceback': traceback.format_exc()
            })
        except Exception as db_error: 
            print(f"ERROR WorkerCleanLogLambda: Could not write CRITICAL ERROR state to DynamoDB during exception handling: {db_error}")
        raise e # Raise lại lỗi để Step Functions Map state biết iteration này thất bại
