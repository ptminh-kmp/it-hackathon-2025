# src/worker_clean_log_lambda/app.py
import json
import os
from urllib.parse import unquote_plus
import boto3
import traceback
from datetime import datetime, timezone

# Các import này sẽ được Lambda Layer cung cấp
from common_lib.config_loader import load_service_config
from common_lib.service_specific_parsers import ( # Chứa các hàm process_SERVICE_clean_logs
    process_esb_clean_logs, # Bạn sẽ cần tạo hàm này
    process_apigw_clean_logs, # Bạn sẽ cần tạo hàm này
    process_roc_clean_logs    # Bạn sẽ cần tạo hàm này
    # Hoặc một hàm generic nếu cấu trúc clean data rất đồng nhất
    # from common_lib.service_specific_parsers import process_generic_clean_log 
)

S3_CLIENT = boto3.client('s3')
DYNAMODB_RESOURCE = boto3.resource('dynamodb')
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME_WORKER', 'IncidentPredictionBatchStates')
DYNAMODB_TABLE = DYNAMODB_RESOURCE.Table(DYNAMODB_TABLE_NAME)

# Mapping tên dịch vụ với hàm xử lý log ĐÃ CLEAN tương ứng
# Key của map này phải khớp với `ServiceNameFromFile` được suy ra bởi ListCleanLogFilesLambda
CLEAN_LOG_PROCESSOR_MAP = {
    "service_esb": process_esb_clean_logs,
    "service_apigw": process_apigw_clean_logs,
    "service_roc": process_roc_clean_logs,
    # "unknown_service": process_generic_clean_log, # Một fallback nếu cần
}

def lambda_handler(event, context):
    """
    WorkerCleanLogLambda: Xử lý một file log ĐÃ CLEAN.
    Input event từ Step Functions Map state:
    {
      "BatchID": "YYYYmmddHHMMSS",
      "S3Bucket": "write-team19-bucket",
      "CleanLogFile": {"S3Key": "clean_data/YYYYmmddHHMMSS/service_esb_file1.json", "ServiceNameFromFile": "service_esb"}
    }
    """
    print(f"WorkerCleanLogLambda started. Event: {json.dumps(event)}")

    batch_id = event.get('BatchID')
    s3_bucket_name = event.get('S3Bucket')
    clean_log_file_info = event.get('CleanLogFile', {})
    s3_object_key_full = clean_log_file_info.get('S3Key')
    # Lấy service_name từ input, do ListCleanLogFilesLambda đã suy ra
    service_name = clean_log_file_info.get('ServiceNameFromFile', 'unknown_service')


    if not all([batch_id, s3_bucket_name, s3_object_key_full, service_name != 'unknown_service']):
        error_msg = f"Missing BatchID, S3Bucket, S3Key, or valid ServiceNameFromFile in input. Got ServiceName: {service_name}"
        print(f"ERROR WorkerCleanLogLambda: {error_msg}")
        raise ValueError(error_msg)

    print(f"WorkerCleanLogLambda: Processing clean file '{s3_object_key_full}' for Service '{service_name}', BatchID '{batch_id}'")

    # Key cho DynamoDB, có thể chỉ là tên file vì nó nằm trong thư mục BatchID rồi
    # Hoặc giữ nguyên key đầy đủ nếu bạn muốn
    relative_s3_key_for_dynamo = os.path.basename(s3_object_key_full) # Chỉ lấy tên file

    service_config = load_service_config(service_name) # Vẫn có thể hữu ích để lấy tên chuẩn, keywords, etc.
    if not service_config:
        # Có thể không cần config nếu dữ liệu đã clean hoàn toàn và bạn không áp dụng rule nào nữa
        # Hoặc tạo một config mặc định
        print(f"Warning WorkerCleanLogLambda: No specific config for service '{service_name}'. Using default/minimal processing.")
        service_config = {"service_name": service_name} # Config tối thiểu
    
    processor_function = CLEAN_LOG_PROCESSOR_MAP.get(service_name)
    if not processor_function:
        # Nếu không có processor cụ thể, bạn có thể log lỗi hoặc dùng một processor mặc định
        error_message = f"WorkerCleanLogLambda: No specific CLEAN log processor function mapped for service '{service_name}'. Skipping."
        print(f"ERROR WorkerCleanLogLambda: {error_message}")
        # Ghi lỗi vào DynamoDB nếu muốn theo dõi file nào không xử lý được
        DYNAMODB_TABLE.put_item(Item={
            'BatchID': batch_id, 'S3ObjectKey': relative_s3_key_for_dynamo, 'ServiceName': service_name,
            'FullS3Key': s3_object_key_full, 'ProcessingTimestamp': datetime.now(timezone.utc).isoformat(),
            'Status': 'ERROR_NO_PROCESSOR', 'ErrorMessage': error_message
        })
        raise ValueError(error_message) # Để Step Functions biết lỗi

    try:
        s3_response = S3_CLIENT.get_object(Bucket=s3_bucket_name, Key=s3_object_key_full)
        # Giả sử file log đã clean là JSON, hoặc text thuần túy. Điều chỉnh nếu cần.
        # Nếu là JSON, dùng json.loads(log_content)
        # Nếu là text, bạn cần logic parse riêng trong hàm processor_function
        log_content_clean = s3_response['Body'].read().decode('utf-8', errors='replace')
        
        # Hàm processor_function sẽ nhận nội dung đã clean và config
        # Nó cần trả về một dict "extracted_phenomena" từ dữ liệu clean
        extracted_phenomena = processor_function(log_content_clean, service_config) 
        
        item_to_save = {
            'BatchID': batch_id,
            'S3ObjectKey': relative_s3_key_for_dynamo,
            'ServiceName': service_name,
            'FullS3Key': s3_object_key_full,
            'ProcessingTimestamp': datetime.now(timezone.utc).isoformat(),
            'CleanLogPhenomenaJSON': json.dumps(extracted_phenomena, ensure_ascii=False), # Lưu các hiện tượng đã trích xuất
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
        error_message = f"WorkerCleanLogLambda: Critical error processing clean file {s3_object_key_full} for '{service_name}': {str(e)}"
        print(f"ERROR WorkerCleanLogLambda: {error_message}")
        traceback.print_exc()
        try:
            DYNAMODB_TABLE.put_item(Item={
                'BatchID': batch_id, 'S3ObjectKey': relative_s3_key_for_dynamo,
                'ServiceName': service_name, 'FullS3Key': s3_object_key_full,
                'ProcessingTimestamp': datetime.now(timezone.utc).isoformat(),
                'Status': 'ERROR_PROCESSING_CLEAN_LOG', 'ErrorMessage': error_message,
                'ErrorTraceback': traceback.format_exc()
            })
        except Exception as db_error: print(f"ERROR WorkerCleanLogLambda: Could not write ERROR state to DynamoDB: {db_error}")
        raise e