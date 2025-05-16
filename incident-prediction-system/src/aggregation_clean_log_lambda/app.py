# src/aggregation_clean_log_lambda/app.py
import json
import os
import boto3
import traceback
from datetime import datetime, timezone

# Các import này sẽ được Lambda Layer cung cấp
from common_lib.knowledge_base_client import retrieve_from_knowledge_base
from common_lib.bedrock_client import synthesize_prediction_with_llm # Đảm bảo _construct_llm_prompt đã được cập nhật
from common_lib.ec2_api_client import send_prediction_to_ec2

DYNAMODB_RESOURCE = boto3.resource('dynamodb')
S3_CLIENT = boto3.client('s3') # Thêm S3 client
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME_AGG', 'IncidentPredictionBatchStates')
DYNAMODB_TABLE = DYNAMODB_RESOURCE.Table(DYNAMODB_TABLE_NAME)

SNS_CLIENT = boto3.client('sns')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')

def lambda_handler(event, context):
    print(f"AggregationCleanLogLambda started. Event: {json.dumps(event)}")

    list_files_result = event.get("ListFilesResult", {})
    batch_id = list_files_result.get('BatchID')
    # expected_file_count được dùng để kiểm tra xem có bao nhiêu file đã được worker "xem xét" (kể cả lỗi)
    expected_file_count_from_list = len(list_files_result.get("CleanLogFiles", [])) if list_files_result.get("CleanLogFiles") else 0

    if not batch_id:
        error_msg = "AggregationCleanLogLambda: Missing BatchID in input event (expected from ListFilesResult.BatchID)."
        print(f"ERROR AggregationCleanLogLambda: {error_msg}")
        raise ValueError(error_msg)

    print(f"AggregationCleanLogLambda: Processing BatchID '{batch_id}'. Expected total files listed: {expected_file_count_from_list}")

    all_service_phenomena_full_details = [] # Sẽ chứa nội dung chi tiết (đã parse từ JSON trên S3)
    processed_item_count_in_db = 0 # Đếm số item tìm thấy trong DB cho batch này
    successfully_read_details_count = 0 # Đếm số file detail đọc thành công từ S3
    
    try:
        last_evaluated_key = None
        while True:
            query_kwargs = {'KeyConditionExpression': boto3.dynamodb.conditions.Key('BatchID').eq(batch_id)}
            if last_evaluated_key: query_kwargs['ExclusiveStartKey'] = last_evaluated_key
            
            response_db = DYNAMODB_TABLE.query(**query_kwargs)
            
            for item in response_db.get('Items', []):
                processed_item_count_in_db += 1
                # Chỉ xử lý những item mà worker đã xử lý thành công và có S3 URI đến file chi tiết
                if item.get('Status') == 'PROCESSED_WITH_S3_DETAILS' and 'DetailedPhenomenaS3URI' in item:
                    s3_uri_detail = item['DetailedPhenomenaS3URI']
                    try:
                        if s3_uri_detail.startswith("s3://"):
                            parts = s3_uri_detail[5:].split('/', 1)
                            detail_bucket = parts[0]
                            detail_key = parts[1]
                            
                            print(f"AggregationCleanLogLambda: Fetching detailed phenomena from {s3_uri_detail}")
                            s3_detail_response = S3_CLIENT.get_object(Bucket=detail_bucket, Key=detail_key)
                            phenomena_detail_content = s3_detail_response['Body'].read().decode('utf-8')
                            phenomena_detail_dict = json.loads(phenomena_detail_content) # Đây là extracted_phenomena_full từ worker
                            
                            # Thêm thông tin nguồn để dễ truy vết nếu cần
                            phenomena_detail_dict['_source_s3_key_full_original_log'] = item.get('FullSourceS3Key', 'N/A')
                            phenomena_detail_dict['_service_name_from_dynamo'] = item.get('ServiceName', 'N/A') # ServiceName đã được Worker lưu
                            all_service_phenomena_full_details.append(phenomena_detail_dict)
                            successfully_read_details_count +=1
                        else:
                            print(f"WARNING AggregationCleanLogLambda: Invalid S3 URI format for details in DynamoDB item: {s3_uri_detail} for S3ObjectKey {item.get('S3ObjectKey')}")
                    except Exception as s3_read_e:
                        print(f"ERROR AggregationCleanLogLambda: Failed to read/parse detail from {s3_uri_detail} for S3ObjectKey {item.get('S3ObjectKey')}. Error: {s3_read_e}")
                        traceback.print_exc()
                        # Ghi nhận lỗi này nhưng không dừng toàn bộ quá trình
                        all_service_phenomena_full_details.append({
                            "_error_reading_s3_detail": True,
                            "service_name": item.get('ServiceName', 'UnknownDueToS3Error'),
                            "source_s3_key_full_original_log": item.get('FullSourceS3Key', 'N/A'),
                            "error_message": f"Failed to process S3 detail: {str(s3_read_e)}"
                        })
                elif item.get('Status', '').startswith('ERROR_'): # Lỗi từ Worker
                    print(f"WARNING AggregationCleanLogLambda: Worker reported error for S3ObjectKey '{item.get('S3ObjectKey')}' in BatchID '{batch_id}'. Msg: {item.get('ErrorMessage')}")
                    # Có thể thêm thông tin lỗi này vào một list riêng để báo cáo
            
            last_evaluated_key = response_db.get('LastEvaluatedKey')
            if not last_evaluated_key: break
        
        print(f"AggregationCleanLogLambda: Retrieved details for {successfully_read_details_count} items from S3 for BatchID '{batch_id}'. Total items found in DB for batch: {processed_item_count_in_db}")

        if expected_file_count_from_list > 0 and successfully_read_details_count < expected_file_count_from_list:
            print(f"WARNING AggregationCleanLogLambda: Number of successfully retrieved details ({successfully_read_details_count}) is less than expected file count ({expected_file_count_from_list}). Some workers might have failed or S3 detail files are missing/corrupt.")

        if not all_service_phenomena_full_details:
            print(f"AggregationCleanLogLambda: No processed phenomena details found (after S3 read) for BatchID '{batch_id}'. Cannot proceed with prediction.")
            # Gửi thông báo lỗi nếu muốn
            if SNS_TOPIC_ARN:
                 SNS_CLIENT.publish(TopicArn=SNS_TOPIC_ARN, Message=f"Aggregation failed for Batch {batch_id}: No detailed data found from workers.", Subject=f"[AI Batch Predictor ERROR] No data for Batch {batch_id}")
            return {"status": "no_detail_data_to_predict", "batch_id": batch_id}

    except Exception as db_e:
        print(f"ERROR AggregationCleanLogLambda: Failed to query DynamoDB for BatchID '{batch_id}'. Error: {db_e}")
        traceback.print_exc()
        raise db_e 

    # Tổng hợp thông tin từ `all_service_phenomena_full_details`
    # Cấu trúc `aggregated_state_for_llm` mà `_construct_llm_prompt` mong đợi:
    # {
    #     "batch_id": "...",
    #     "overall_summary": "...",
    #     "service_phenomena": [ // Đây là list các `extracted_phenomena_full` từ mỗi file log
    #         { "service_name": "service_esb", "errors_summary": [...], "metrics_summary": {...}, ... },
    #         { "service_name": "service_apigw", ... }
    #     ]
    # }
    aggregated_state_for_llm = {
        "batch_id": batch_id,
        "overall_summary": f"Phân tích tổng hợp từ {successfully_read_details_count} file log (trong số {expected_file_count_from_list} file dự kiến) cho batch {batch_id}.",
        "service_phenomena": all_service_phenomena_full_details # Truyền trực tiếp list các dict phenomena đầy đủ
    }
    
    print(f"AggregationCleanLogLambda: Aggregated phenomena prepared for LLM for BatchID '{batch_id}'.")

    # Tạo Query tổng hợp cho Knowledge Base (sử dụng aggregated_state_for_llm để tạo query)
    kb_query_parts_agg = [f"Hệ thống đang có các vấn đề tổng hợp từ batch log '{batch_id}' như sau:"]
    for i, service_data in enumerate(aggregated_state_for_llm["service_phenomena"][:3]): # Lấy 3 service đầu
        service_name_display = service_data.get("service_name", "Dịch vụ không xác định")
        errors_summary_list = service_data.get("errors_summary", [])
        metrics_summary_dict = service_data.get("metrics_summary", {})
        kb_query_parts_agg.append(f"\n- Dịch vụ {i+1}: {service_name_display}")
        if errors_summary_list:
            kb_query_parts_agg.append(f"  - Ghi nhận {len(errors_summary_list)} tóm tắt lỗi/hiện tượng. Ví dụ: '{errors_summary_list[0] if errors_summary_list else ''}'")
        if metrics_summary_dict:
            kb_query_parts_agg.append(f"  - Metrics tóm tắt: {str(list(metrics_summary_dict.items())[:2])}") # Ví dụ 2 metric đầu
    kb_query_parts_agg.append("\nDựa trên tình hình tổng thể này, hãy tìm các sự cố trong quá khứ có các dấu hiệu tương tự và cung cấp thông tin liên quan.")
    kb_query_aggregated = "\n".join(kb_query_parts_agg)
    print(f"AggregationCleanLogLambda: Knowledge Base Retrieve Query (Aggregated): {kb_query_aggregated[:500]}...")

    retrieved_chunks_raw_agg = retrieve_from_knowledge_base(kb_query_aggregated, number_of_results=3) # Lấy 3 chunks

    print(f"AggregationCleanLogLambda: Synthesizing final prediction using LLM for BatchID '{batch_id}'.")
    # Hàm _construct_llm_prompt trong bedrock_client.py cần được cập nhật để xử lý đúng cấu trúc này
    final_llm_prediction_text = synthesize_prediction_with_llm(
        aggregated_state_for_llm, # Truyền cấu trúc mới này
        retrieved_chunks_raw_agg
    )

    # Chuẩn bị payload cuối cùng và gửi đến EC2 API
    final_payload_to_ec2 = {
        "batch_id": batch_id,
        "aggregated_system_summary": { # Tóm tắt để gửi cho EC2
            "total_processed_logs_success": successfully_read_details_count,
            "expected_log_files": expected_file_count_from_list,
            "services_with_issues_count": len([s for s in aggregated_state_for_llm["service_phenomena"] if s.get("errors_summary") or s.get("metrics_summary")])
        },
        "retrieved_knowledge_chunks_summary": [],
        "final_llm_prediction": final_llm_prediction_text
    }
    if retrieved_chunks_raw_agg:
        for chunk_data in retrieved_chunks_raw_agg[:3]: 
            final_payload_to_ec2["retrieved_knowledge_chunks_summary"].append({
                "source_s3_uri": os.path.basename(chunk_data.get('location', {}).get('s3Location', {}).get('uri', 'N/A')),
                "retrieval_score": chunk_data.get('score'),
                "excerpt": chunk_data.get('content', {}).get('text', '')[:250] + "..."
            })
            
    print(f"AggregationCleanLogLambda: Attempting to send final aggregated payload to EC2 API for BatchID '{batch_id}'.")
    api_call_successful = send_prediction_to_ec2(final_payload_to_ec2)

    if SNS_TOPIC_ARN:
        # ... (Logic gửi SNS message như cũ, tùy chỉnh nội dung cho phù hợp với batch)
        sns_message = f"Phân tích tổng hợp (S3 Details) hoàn tất cho BatchID: {batch_id}\n"
        # ... (Thêm các thông tin tóm tắt vào sns_message) ...
        sns_message += f"Dự đoán từ LLM:\n{final_llm_prediction_text[:1000]}...\n"
        sns_message += f"Trạng thái gửi EC2 API: {'Thành công' if api_call_successful else 'Thất bại'}"
        
        print(f"AggregationCleanLogLambda: Sending SNS for BatchID '{batch_id}'.")
        try:
            SNS_CLIENT.publish(TopicArn=SNS_TOPIC_ARN, Message=sns_message, Subject=f"[AI Batch Predictor - S3 Details] Hoàn tất Batch {batch_id}")
        except Exception as sns_e:
            print(f"ERROR AggregationCleanLogLambda: SNS publish failed: {sns_e}")


    return {
        'statusCode': 200,
        'body': json.dumps({
            "message": f"AggregationCleanLogLambda finished for BatchID {batch_id}. Successfully processed {successfully_read_details_count} detail files.",
            "api_call_successful": api_call_successful,
            "final_prediction_excerpt": final_llm_prediction_text[:200] + "..." if final_llm_prediction_text else "N/A"
        }, ensure_ascii=False)
    }
