# src/list_clean_log_files_lambda/app.py
import json
import os
import boto3
import traceback

S3_CLIENT = boto3.client('s3')

def lambda_handler(event, context):
    """
    Input: event = {"BatchID": "...", "S3Bucket": "...", "BatchFolderPath": "clean_data/YYYYmmddHHMMSS/"}
    Output: {"BatchID": "...", "S3Bucket": "...", "BatchFolderPath": "...", 
             "CleanLogFiles": [{"S3Key": "clean_data/YYYYmmddHHMMSS/service_esb_file1.json", "ServiceName": "service_esb"}, ...]} 
             (ServiceName được suy ra từ tên file nếu có thể)
    """
    print(f"ListCleanLogFilesLambda started. Event: {json.dumps(event)}")

    batch_id = event.get('BatchID')
    s3_bucket_name = event.get('S3Bucket')
    batch_folder_path = event.get('BatchFolderPath') # Ví dụ: "clean_data/20250515143000/"

    if not all([batch_id, s3_bucket_name, batch_folder_path]):
        error_msg = "Missing BatchID, S3Bucket, or BatchFolderPath in input."
        print(f"ERROR ListCleanLogFilesLambda: {error_msg}")
        raise ValueError(error_msg)

    clean_log_files_to_process = []
    continuation_token = None
    try:
        while True:
            list_kwargs = {
                'Bucket': s3_bucket_name,
                'Prefix': batch_folder_path # List tất cả file trong thư mục batch này
            }
            if continuation_token:
                list_kwargs['ContinuationToken'] = continuation_token
            
            response = S3_CLIENT.list_objects_v2(**list_kwargs)
            
            for obj in response.get('Contents', []):
                obj_key = obj['Key']
                # Bỏ qua file cờ và các "thư mục"
                if obj_key.endswith('_BATCH_PROCESSED.txt') or obj_key.endswith('/'):
                    continue

                # Suy ra ServiceName từ tên file (ví dụ: "service_esb_file1.json" -> "service_esb")
                # Bạn cần một quy ước đặt tên file log đã clean để làm điều này.
                # Ví dụ: tên file bắt đầu bằng tên service.
                service_name_from_file = "unknown_service" # Mặc định
                filename = os.path.basename(obj_key)
                # Logic ví dụ: nếu tên file là "service_esb_xxxxx.json" hoặc "esb_xxxxx.json"
                if filename.lower().startswith("service_esb") or filename.lower().startswith("esb_"):
                    service_name_from_file = "service_esb"
                elif filename.lower().startswith("service_apigw") or filename.lower().startswith("apigw_"):
                    service_name_from_file = "service_apigw"
                elif filename.lower().startswith("service_roc") or filename.lower().startswith("roc_"):
                    service_name_from_file = "service_roc"
                # Thêm các quy tắc khác cho các service khác
                
                clean_log_files_to_process.append({
                    "S3Key": obj_key,
                    "ServiceNameFromFile": service_name_from_file # Để WorkerLambda biết service nào
                }) 
            
            if response.get('IsTruncated'):
                continuation_token = response.get('NextContinuationToken')
            else:
                break
        
        print(f"ListCleanLogFilesLambda: Found {len(clean_log_files_to_process)} clean log files in '{batch_folder_path}' for BatchID '{batch_id}'.")
        
        output = {
            "BatchID": batch_id,
            "S3Bucket": s3_bucket_name,
            "BatchFolderPath": batch_folder_path,
            "CleanLogFiles": clean_log_files_to_process
        }
        return output

    except Exception as e:
        print(f"ERROR ListCleanLogFilesLambda: Failed to list files for BatchID '{batch_id}'. Error: {e}")
        traceback.print_exc()
        raise e