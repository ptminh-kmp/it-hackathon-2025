# src/aggregation_clean_log_lambda/app.py
import json
import os
import boto3
import traceback
from datetime import datetime, timezone

# Các import này sẽ được Lambda Layer cung cấp
from common_lib.knowledge_base_client import retrieve_from_knowledge_base
from common_lib.bedrock_client import synthesize_prediction_with_llm
from common_lib.ec2_api_client import send_prediction_to_ec2 # Hàm này đã có error handling riêng

DYNAMODB_RESOURCE = boto3.resource('dynamodb')
S3_CLIENT = boto3.client('s3')
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME_AGG', 'IncidentPredictionBatchStates')
DYNAMODB_TABLE = DYNAMODB_RESOURCE.Table(DYNAMODB_TABLE_NAME)

SNS_CLIENT = boto3.client('sns')
# SNS_TOPIC_ARN giờ đây sẽ là SNS_SUCCESS_TOPIC_ARN
SNS_SUCCESS_TOPIC_ARN = os.environ.get('SNS_SUCCESS_TOPIC_ARN') # ARN cho thông báo thành công
# SNS_FAILURE_TOPIC_ARN vẫn có thể được dùng nếu có lỗi nghiêm trọng trước khi có dự đoán
SNS_FAILURE_TOPIC_ARN = os.environ.get('SNS_FAILURE_TOPIC_ARN')


def lambda_handler(event, context):
    print(f"AggregationCleanLogLambda started. Event: {json.dumps(event, ensure_ascii=False)}")

    list_files_output = event.get("ListFilesOutput", {})
    batch_id = list_files_output.get('BatchID')
    expected_file_count_from_list = len(list_files_output.get("CleanLogFiles", [])) if list_files_output.get("CleanLogFiles") else 0

    if not batch_id:
        error_msg = "AggregationCleanLogLambda: Missing BatchID in input event (expected from ListFilesOutput.BatchID)."
        print(f"ERROR: {error_msg}")
        if SNS_FAILURE_TOPIC_ARN:
            try:
                SNS_CLIENT.publish(TopicArn=SNS_FAILURE_TOPIC_ARN, Message=f"Aggregation FAILED for unknown BatchID. Event: {json.dumps(event, ensure_ascii=False)}", Subject="[AI Batch Predictor ERROR] Unknown BatchID in Aggregation")
            except Exception as sns_e:
                print(f"ERROR sending initial failure SNS: {sns_e}")
        raise ValueError(error_msg)

    print(f"AggregationCleanLogLambda: Processing BatchID '{batch_id}'. Expected total files listed by SFN: {expected_file_count_from_list}")

    all_service_phenomena_full_details = []
    processed_item_count_in_db = 0
    successfully_read_details_count = 0
    worker_reported_errors = []
    
    # --- Bước 1: Đọc và tổng hợp dữ liệu từ DynamoDB và S3 ---
    try:
        last_evaluated_key = None
        while True:
            query_kwargs = {'KeyConditionExpression': boto3.dynamodb.conditions.Key('BatchID').eq(batch_id)}
            if last_evaluated_key: query_kwargs['ExclusiveStartKey'] = last_evaluated_key
            
            response_db = DYNAMODB_TABLE.query(**query_kwargs)
            items_in_page = response_db.get('Items', [])
            
            for item in items_in_page:
                processed_item_count_in_db += 1
                item_s3_object_key_for_sort = item.get('S3ObjectKey', 'UnknownS3ObjectKey')

                if item.get('Status') == 'PROCESSED_S3_DETAILS_V2' and 'DetailedPhenomenaS3URI' in item:
                    s3_uri_detail = item['DetailedPhenomenaS3URI']
                    try:
                        if s3_uri_detail and s3_uri_detail.startswith("s3://"):
                            parts = s3_uri_detail[5:].split('/', 1)
                            detail_bucket = parts[0]
                            detail_key = parts[1]
                            
                            s3_detail_response = S3_CLIENT.get_object(Bucket=detail_bucket, Key=detail_key)
                            phenomena_detail_content = s3_detail_response['Body'].read().decode('utf-8')
                            phenomena_detail_dict = json.loads(phenomena_detail_content)
                            
                            phenomena_detail_dict['_source_s3_key_full_original_log'] = item.get('FullSourceS3Key', 'N/A')
                            phenomena_detail_dict['_service_name_from_dynamo'] = item.get('ServiceName', 'N/A')
                            all_service_phenomena_full_details.append(phenomena_detail_dict)
                            successfully_read_details_count +=1
                        else:
                            msg = f"Invalid S3 URI format: {s3_uri_detail} for S3ObjectKey {item_s3_object_key_for_sort}"
                            print(f"WARNING AggregationCleanLogLambda: {msg}")
                            worker_reported_errors.append(f"S3ObjectKey '{item_s3_object_key_for_sort}': {msg}")
                    except Exception as s3_read_e:
                        msg = f"Failed to read/parse detail from {s3_uri_detail} for S3ObjectKey {item_s3_object_key_for_sort}. Error: {s3_read_e}"
                        print(f"ERROR AggregationCleanLogLambda: {msg}")
                        traceback.print_exc()
                        worker_reported_errors.append(f"S3ObjectKey '{item_s3_object_key_for_sort}': {msg}")
                elif item.get('Status', '').startswith('ERROR_'):
                    msg = f"Worker reported error for S3ObjectKey '{item_s3_object_key_for_sort}'. Status: {item.get('Status')}, Msg: {item.get('ErrorMessage')}"
                    print(f"WARNING AggregationCleanLogLambda: {msg}")
                    worker_reported_errors.append(msg)
            
            last_evaluated_key = response_db.get('LastEvaluatedKey')
            if not last_evaluated_key: break
        
        print(f"AggregationCleanLogLambda: Retrieved details for {successfully_read_details_count} items from S3 for BatchID '{batch_id}'. Total items in DB: {processed_item_count_in_db}.")
        if worker_reported_errors:
             print(f"AggregationCleanLogLambda: Encountered {len(worker_reported_errors)} issues from workers or S3 detail fetch for BatchID '{batch_id}'. First issue: {worker_reported_errors[0]}")

        if not all_service_phenomena_full_details:
            no_data_msg = f"No successfully processed phenomena details found (after S3 read) for BatchID '{batch_id}'. Cannot proceed with prediction."
            print(f"AggregationCleanLogLambda: {no_data_msg}")
            if SNS_FAILURE_TOPIC_ARN:
                 SNS_CLIENT.publish(TopicArn=SNS_FAILURE_TOPIC_ARN, Message=f"Aggregation FAILED for Batch {batch_id}: {no_data_msg}\nWorker/S3Fetch Issues: {json.dumps(worker_reported_errors, ensure_ascii=False)}", Subject=f"[AI Batch Predictor ERROR] No Detail Data for Batch {batch_id}")
            return {"status": "no_detail_data_to_predict_from_s3", "batch_id": batch_id, "worker_errors_or_s3_fetch_issues": worker_reported_errors}

    except Exception as db_e:
        error_message = f"Failed to query DynamoDB or process S3 details for BatchID '{batch_id}'. Error: {db_e}"
        print(f"ERROR AggregationCleanLogLambda: {error_message}")
        traceback.print_exc()
        if SNS_FAILURE_TOPIC_ARN:
            SNS_CLIENT.publish(TopicArn=SNS_FAILURE_TOPIC_ARN, Message=f"Aggregation FAILED for Batch {batch_id} during DB/S3 read: {error_message}\nTraceback: {traceback.format_exc()}", Subject=f"[AI Batch Predictor CRITICAL ERROR] DB/S3 Read Batch {batch_id}")
        raise db_e 

    # --- Bước 2: Chuẩn bị dữ liệu cho LLM và Knowledge Base ---
    aggregated_state_for_llm = {
        "batch_id": batch_id,
        "overall_summary": f"Phân tích tổng hợp từ {successfully_read_details_count} file log (trong số {expected_file_count_from_list} file dự kiến) cho batch '{batch_id}'.",
        "service_phenomena": all_service_phenomena_full_details
    }
    total_errors_summarized_in_batch = sum(len(sp.get("errors_summary",[])) for sp in all_service_phenomena_full_details)
    print(f"AggregationCleanLogLambda: System state aggregated for LLM. Total error summaries collected in batch: {total_errors_summarized_in_batch}")

    kb_query_parts_agg = [f"Hệ thống đang có các vấn đề tổng hợp từ các log của batch '{batch_id}' như sau:"]
    # ... (Logic tạo kb_query_aggregated như trước, dựa trên aggregated_state_for_llm)
    for i, service_data in enumerate(aggregated_state_for_llm["service_phenomena"][:3]): 
        service_name_display = service_data.get("service_name", service_data.get("_service_name_from_dynamo", "Dịch vụ không xác định"))
        errors_summary_list = service_data.get("errors_summary", [])
        metrics_summary_dict = service_data.get("metrics_summary", {})
        source_file_display = os.path.basename(service_data.get("_source_s3_key_full_original_log","ẩn"))
        kb_query_parts_agg.append(f"\n- Từ nguồn log '{source_file_display}' (Dịch vụ: {service_name_display}):")
        if errors_summary_list:
            kb_query_parts_agg.append(f"  - Ghi nhận {len(errors_summary_list)} tóm tắt lỗi/hiện tượng. Ví dụ: '{errors_summary_list[0] if errors_summary_list else ''}'")
        if metrics_summary_dict:
            main_metrics_for_query = {k:v for k,v in list(metrics_summary_dict.items())[:2]}
            if main_metrics_for_query:
                kb_query_parts_agg.append(f"  - Metrics đáng chú ý: {json.dumps(main_metrics_for_query, ensure_ascii=False)}")
    if not aggregated_state_for_llm["service_phenomena"]:
         kb_query_parts_agg.append("- Không có hiện tượng cụ thể nào được ghi nhận từ các file log trong batch này.")
    kb_query_parts_agg.append("\nDựa trên các hiện tượng tổng thể này, hãy tìm các sự cố trong quá khứ có các dấu hiệu tương tự và cung cấp thông tin liên quan về nguyên nhân, giải pháp đã thực hiện liên quan nhất.")
    kb_query_aggregated = "\n".join(kb_query_parts_agg)
    # ...

    retrieved_chunks_raw_agg = retrieve_from_knowledge_base(kb_query_aggregated, number_of_results=3)

    # --- Bước 3: Gọi LLM để lấy dự đoán ---
    print(f"AggregationCleanLogLambda: Synthesizing final prediction using LLM for BatchID '{batch_id}'.")
    final_llm_prediction_text = synthesize_prediction_with_llm(
        aggregated_state_for_llm, 
        retrieved_chunks_raw_agg,
        use_streaming=True # Hoặc True nếu bạn muốn
    )
    if not final_llm_prediction_text or "Lỗi khi tổng hợp dự đoán" in final_llm_prediction_text:
        error_msg_llm = f"LLM synthesis failed or returned error for BatchID {batch_id}. Prediction: {final_llm_prediction_text}"
        print(f"ERROR AggregationCleanLogLambda: {error_msg_llm}")
        if SNS_FAILURE_TOPIC_ARN:
             SNS_CLIENT.publish(TopicArn=SNS_FAILURE_TOPIC_ARN, Message=f"Aggregation FAILED for Batch {batch_id}: LLM Synthesis Error.\nLLM Output: {final_llm_prediction_text}\nWorker/S3Fetch Issues: {json.dumps(worker_reported_errors, ensure_ascii=False)}", Subject=f"[AI Batch Predictor ERROR] LLM Synthesis Batch {batch_id}")
        # Quyết định xem có nên raise lỗi ở đây không. Hiện tại sẽ cố gắng gửi những gì có.
        # raise ValueError(error_msg_llm)

    # --- Bước 4: Chuẩn bị payload cuối cùng ---
    final_payload_to_ec2 = {
        "batch_id": batch_id,
        "aggregated_system_summary_for_ec2": { 
            "total_log_sources_processed_successfully": successfully_read_details_count,
            "expected_log_sources_in_batch": expected_file_count_from_list,
            "services_with_issues_count_in_batch": len([s for s in aggregated_state_for_llm["service_phenomena"] if s.get("errors_summary") or s.get("metrics_summary")]),
            "total_error_summaries_in_batch": total_errors_summarized_in_batch,
            "worker_reported_errors_count": len(worker_reported_errors)
        },
        "retrieved_knowledge_chunks_summary_for_ec2": [],
        "final_llm_prediction": final_llm_prediction_text # Đây là dự đoán chính
    }
    if retrieved_chunks_raw_agg:
        for chunk_data in retrieved_chunks_raw_agg[:3]: 
            final_payload_to_ec2["retrieved_knowledge_chunks_summary_for_ec2"].append({
                "source_s3_uri": os.path.basename(chunk_data.get('location', {}).get('s3Location', {}).get('uri', 'N/A')),
                "retrieval_score": chunk_data.get('score'),
                "excerpt": chunk_data.get('content', {}).get('text', '')[:250] + "..."
            })
    
    # --- Bước 5: Thực hiện các hành động song song (Gửi SNS và Gọi EC2 API) ---
    api_call_successful = False
    sns_publish_successful = False

    # 5a. Gửi kết quả dự đoán đến EC2 API
    print(f"AggregationCleanLogLambda: Attempting to send final aggregated payload to EC2 API for BatchID '{batch_id}'.")
    try:
        api_call_successful = send_prediction_to_ec2(final_payload_to_ec2) # Hàm này đã có try-except riêng
    except Exception as e_ec2_call:
        # Hàm send_prediction_to_ec2 đã log lỗi, ở đây chỉ ghi nhận thêm
        print(f"ERROR AggregationCleanLogLambda: Exception during send_prediction_to_ec2 call for BatchID '{batch_id}': {e_ec2_call}")
        api_call_successful = False # Đảm bảo trạng thái đúng

    # 5b. Gửi thông báo SNS Success (chứa dự đoán)
    if SNS_SUCCESS_TOPIC_ARN:
        sns_message_parts_success = [
            f"Phân tích tổng hợp (S3 Details) cho BatchID: {batch_id}",
            f"Số file log dự kiến: {expected_file_count_from_list}, Đã đọc chi tiết thành công: {successfully_read_details_count}",
            f"Số lỗi từ worker/S3 fetch (nếu có): {len(worker_reported_errors)}"
        ]
        if worker_reported_errors:
            sns_message_parts_success.append(f"  Một số lỗi worker/S3 fetch: {str(worker_reported_errors[:2])}")

        sns_message_parts_success.append("\nDự đoán từ LLM:")
        sns_message_parts_success.append(final_llm_prediction_text[:1500] + "..." if len(final_llm_prediction_text) > 1500 else final_llm_prediction_text)
        
        if final_payload_to_ec2["retrieved_knowledge_chunks_summary_for_ec2"]:
            sns_message_parts_success.append("\nTham khảo từ Knowledge Base (tối đa 1):")
            first_chunk_summary = final_payload_to_ec2["retrieved_knowledge_chunks_summary_for_ec2"][0]
            sns_message_parts_success.append(f"  - Nguồn: {first_chunk_summary.get('source_s3_uri')}, Score: {first_chunk_summary.get('retrieval_score')}")
        
        sns_message_parts_success.append(f"\nTrạng thái gửi đến EC2 API: {'Thành công' if api_call_successful else 'Thất bại'}")
        
        final_sns_message_success = "\n".join(sns_message_parts_success)
        
        print(f"AggregationCleanLogLambda: Sending SUCCESS notification to SNS for BatchID '{batch_id}'.")
        try:
            SNS_CLIENT.publish(TopicArn=SNS_SUCCESS_TOPIC_ARN, Message=final_sns_message_success, Subject=f"[AI Batch Predictor OK] Phân tích Batch {batch_id}")
            sns_publish_successful = True
        except Exception as sns_e:
            print(f"ERROR AggregationCleanLogLambda: SNS success publish failed for BatchID '{batch_id}': {sns_e}")
            sns_publish_successful = False
    else:
        print("AggregationCleanLogLambda: SNS_SUCCESS_TOPIC_ARN not configured. Skipping success notification.")
        sns_publish_successful = True # Coi như thành công nếu không có nhu cầu gửi


    # Output cuối cùng của Lambda cho Step Functions
    # Step Functions sẽ tự fail nếu Lambda này raise exception chưa được bắt.
    # Nếu không raise exception, Step Functions coi như thành công.
    # Ta có thể trả về trạng thái chi tiết hơn.
    final_lambda_status = "PARTIAL_SUCCESS"
    if api_call_successful and sns_publish_successful :
        final_lambda_status = "SUCCESS"
    elif not api_call_successful and not sns_publish_successful:
        final_lambda_status = "ALL_ACTIONS_FAILED"
    elif not api_call_successful:
        final_lambda_status = "EC2_CALL_FAILED"
    elif not sns_publish_successful:
        final_lambda_status = "SNS_PUBLISH_FAILED"

    return {
        'statusCode': 200, # Luôn trả về 200 nếu không có unhandled exception
        'body': json.dumps({
            "message": f"AggregationCleanLogLambda finished for BatchID {batch_id}. Overall status: {final_lambda_status}",
            "batch_id": batch_id,
            "api_call_successful": api_call_successful,
            "sns_publish_successful": sns_publish_successful, # Nếu bạn muốn theo dõi điều này
            "final_prediction_excerpt": (final_llm_prediction_text[:200] + "..." if final_llm_prediction_text and "Lỗi khi tổng hợp dự đoán" not in final_llm_prediction_text else "Prediction might have failed or was empty.")
        }, ensure_ascii=False)
    }
