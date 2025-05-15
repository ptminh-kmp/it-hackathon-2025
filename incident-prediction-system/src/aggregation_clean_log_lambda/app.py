# src/aggregation_clean_log_lambda/app.py
import json
import os
import boto3
import traceback
from datetime import datetime, timezone

# Các import này sẽ được Lambda Layer cung cấp
from common_lib.knowledge_base_client import retrieve_from_knowledge_base
from common_lib.bedrock_client import synthesize_prediction_with_llm
from common_lib.ec2_api_client import send_prediction_to_ec2

DYNAMODB_RESOURCE = boto3.resource('dynamodb')
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME_AGG', 'IncidentPredictionBatchStates')
DYNAMODB_TABLE = DYNAMODB_RESOURCE.Table(DYNAMODB_TABLE_NAME)

SNS_CLIENT = boto3.client('sns') # Nếu muốn gửi SNS từ đây
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')

def lambda_handler(event, context):
    """
    AggregationCleanLogLambda: Trigger bởi Step Functions.
    Input: Tương tự AggregationLambda cũ, chứa BatchID và thông tin về ListFilesResult
    """
    print(f"AggregationCleanLogLambda started. Event: {json.dumps(event)}")

    list_files_result = event.get("ListFilesResult", {}) # Output từ bước ListCleanLogFilesLambda
    batch_id = list_files_result.get('BatchID')
    expected_file_count = len(list_files_result.get("CleanLogFiles", [])) if list_files_result.get("CleanLogFiles") else 0

    if not batch_id:
        error_msg = "Missing BatchID in input event."
        print(f"ERROR AggregationCleanLogLambda: {error_msg}")
        raise ValueError(error_msg)

    print(f"AggregationCleanLogLambda: Processing BatchID '{batch_id}'. Expected clean files: {expected_file_count}")

    all_service_phenomena_from_db = []
    processed_items_count_from_db = 0
    try:
        # Query DynamoDB
        last_evaluated_key = None
        while True:
            query_kwargs = {'KeyConditionExpression': boto3.dynamodb.conditions.Key('BatchID').eq(batch_id)}
            if last_evaluated_key: query_kwargs['ExclusiveStartKey'] = last_evaluated_key
            
            response_db = DYNAMODB_TABLE.query(**query_kwargs)
            
            for item in response_db.get('Items', []):
                processed_items_count_from_db += 1
                if item.get('Status') == 'PROCESSED_CLEAN_LOG' and 'CleanLogPhenomenaJSON' in item:
                    try:
                        phenomena = json.loads(item['CleanLogPhenomenaJSON'])
                        # Thêm thông tin nguồn để dễ truy vết
                        phenomena['_source_s3_key_full'] = item.get('FullS3Key', 'N/A')
                        phenomena['_service_name_from_worker'] = item.get('ServiceName', 'N/A') 
                        all_service_phenomena_from_db.append(phenomena)
                    except json.JSONDecodeError:
                        print(f"WARNING AggregationCleanLogLambda: Could not parse CleanLogPhenomenaJSON for S3Key '{item.get('S3ObjectKey')}' in BatchID '{batch_id}'.")
                elif item.get('Status', '').startswith('ERROR_'): # Bắt các lỗi từ worker
                    print(f"WARNING AggregationCleanLogLambda: Worker reported error for S3Key '{item.get('S3ObjectKey')}'. Msg: {item.get('ErrorMessage')}")
            
            last_evaluated_key = response_db.get('LastEvaluatedKey')
            if not last_evaluated_key: break
        
        print(f"AggregationCleanLogLambda: Retrieved {len(all_service_phenomena_from_db)} 'PROCESSED_CLEAN_LOG' states from DynamoDB for BatchID '{batch_id}'. Total items found in DB: {processed_items_count_from_db}")

        if expected_file_count > 0 and len(all_service_phenomena_from_db) < expected_file_count:
            print(f"WARNING AggregationCleanLogLambda: Number of processed states ({len(all_service_phenomena_from_db)}) < expected ({expected_file_count}).")

        if not all_service_phenomena_from_db:
            print(f"AggregationCleanLogLambda: No processed phenomena found for BatchID '{batch_id}'. Cannot predict.")
            return {"status": "no_data_to_predict", "batch_id": batch_id}

    except Exception as db_e: # Lỗi đọc DynamoDB
        print(f"ERROR AggregationCleanLogLambda: Failed to query DynamoDB for BatchID '{batch_id}'. Error: {db_e}")
        traceback.print_exc()
        raise db_e 

    # Tổng hợp thông tin từ all_service_phenomena_from_db
    # `aggregated_system_state` sẽ chứa các "hiện tượng" đã được trích xuất từ log clean
    # Ví dụ:
    aggregated_system_state_for_llm = {
        "batch_id": batch_id,
        "overall_summary": f"Phân tích từ {len(all_service_phenomena_from_db)} file log đã làm sạch cho batch {batch_id}.",
        "service_phenomena": [] # List các dict, mỗi dict là hiện tượng của 1 service
    }
    
    for phenomena_data in all_service_phenomena_from_db:
        # Giả sử phenomena_data là dict mà hàm process_SERVICE_clean_logs trả về
        # Ví dụ: {"service_name": "service_esb", "detected_errors": [...], "key_metrics": {...}}
        aggregated_system_state_for_llm["service_phenomena"].append({
            "service_name": phenomena_data.get("_service_name_from_worker", phenomena_data.get("service_name", "Unknown")),
            "source_file": os.path.basename(phenomena_data.get("_source_s3_key_full", "")),
            # Thêm các trường cụ thể từ `phenomena_data` mà bạn muốn LLM biết
            "errors_summary": phenomena_data.get("errors_summary", []), # Ví dụ
            "metrics_summary": phenomena_data.get("metrics_summary", {}) # Ví dụ
        })

    print(f"AggregationCleanLogLambda: Aggregated phenomena for BatchID '{batch_id}'.")

    # Tạo Query tổng hợp cho Knowledge Base
    kb_query_parts_agg = [f"Hệ thống đang có các dấu hiệu tổng hợp từ batch log '{batch_id}' đã được làm sạch như sau:"]
    for service_data in aggregated_system_state_for_llm["service_phenomena"][:3]: # Lấy 3 service đầu
        kb_query_parts_agg.append(f"- Dịch vụ {service_data['service_name']} (từ file {service_data['source_file']}):")
        if service_data.get("errors_summary"):
            kb_query_parts_agg.append(f"  - Lỗi: {str(service_data['errors_summary'][:2])}") # Ví dụ 2 lỗi đầu
        if service_data.get("metrics_summary"):
            kb_query_parts_agg.append(f"  - Metrics: {str(service_data['metrics_summary'])}")
    kb_query_parts_agg.append("\nDựa trên các hiện tượng đã được trích xuất này, hãy tìm các sự cố trong quá khứ có các dấu hiệu tương tự và cung cấp thông tin liên quan.")
    kb_query_aggregated = "\n".join(kb_query_parts_agg)
    print(f"AggregationCleanLogLambda: KB Retrieve Query (Aggregated): {kb_query_aggregated[:500]}...")

    retrieved_chunks_raw_agg = retrieve_from_knowledge_base(kb_query_aggregated, number_of_results=3) # Lấy 3 chunks

    # Chuẩn bị input và gọi synthesize_prediction_with_llm
    # `synthesize_prediction_with_llm` cần một dict `current_system_state` có các key như "service_name", "errors", "metrics_violations"
    # Ta sẽ "ánh xạ" `aggregated_system_state_for_llm` sang cấu trúc đó
    llm_input_pseudo_state = {
        "service_name": f"Hệ thống tổng hợp (Batch: {batch_id})",
        "errors": [], # Tập hợp lỗi từ tất cả các service_phenomena nếu cần
        "metrics_violations": [], # Tập hợp metrics từ tất cả
        "raw_log_summary": [f"Tổng hợp từ {len(aggregated_system_state_for_llm['service_phenomena'])} services. Ví dụ: Service {aggregated_system_state_for_llm['service_phenomena'][0]['service_name']} có lỗi: {aggregated_system_state_for_llm['service_phenomena'][0].get('errors_summary')}" if aggregated_system_state_for_llm['service_phenomena'] else "Không có hiện tượng cụ thể."]
        # Quan trọng: Truyền chi tiết hơn từ `aggregated_system_state_for_llm["service_phenomena"]` vào prompt của `synthesize_prediction_with_llm`
        # Hoặc sửa đổi `synthesize_prediction_with_llm` để chấp nhận cấu trúc `aggregated_system_state_for_llm`
    }
    # Cách tốt hơn là sửa đổi hàm _construct_llm_prompt trong bedrock_client.py để nó hiểu cấu trúc mới này
    # Hiện tại, chúng ta sẽ truyền `aggregated_system_state_for_llm` trực tiếp và sửa prompt sau nếu cần
    
    print(f"AggregationCleanLogLambda: Synthesizing final prediction for BatchID '{batch_id}'.")
    final_llm_prediction_text = synthesize_prediction_with_llm(
        aggregated_system_state_for_llm, # TRUYỀN CẤU TRÚC MỚI NÀY
        retrieved_chunks_raw_agg
    )
    # LƯU Ý: Bạn cần sửa hàm `_construct_llm_prompt` trong `bedrock_client.py`
    # để nó có thể đọc và diễn giải cấu trúc của `aggregated_system_state_for_llm`
    # thay vì `current_system_state` với key "errors", "metrics_violations" trực tiếp.

    # Chuẩn bị payload cuối cùng và gửi đến EC2 API
    final_payload_to_ec2 = {
        "batch_id": batch_id,
        "aggregated_phenomena_summary": aggregated_system_state_for_llm["service_phenomena"],
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
            
    print(f"AggregationCleanLogLambda: Sending final payload to EC2 API for BatchID '{batch_id}'.")
    api_call_successful = send_prediction_to_ec2(final_payload_to_ec2)

    if SNS_TOPIC_ARN:
        # ... (Logic gửi SNS tương tự như AggregationLambda cũ, tùy chỉnh nội dung) ...
        pass

    return {
        'statusCode': 200,
        'body': json.dumps({
            "message": f"AggregationCleanLogLambda finished for BatchID {batch_id}.",
            "api_call_successful": api_call_successful
        })
    }