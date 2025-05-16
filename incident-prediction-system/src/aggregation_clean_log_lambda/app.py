# src/aggregation_clean_log_lambda/app.py
import json
import os
import boto3
import traceback
from datetime import datetime, timezone

# Các import này sẽ được Lambda Layer cung cấp
# Đảm bảo rằng common_lib nằm trong sys.path của Lambda Layer
# (thường là thư mục python/ trong file zip của Layer, và code common_lib nằm trong python/common_lib/)
from common_lib.knowledge_base_client import retrieve_from_knowledge_base
from common_lib.bedrock_client import synthesize_prediction_with_llm # Đảm bảo _construct_llm_prompt đã được cập nhật
from common_lib.ec2_api_client import send_prediction_to_ec2

DYNAMODB_RESOURCE = boto3.resource('dynamodb')
S3_CLIENT = boto3.client('s3') # Thêm S3 client để đọc file detail
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME_AGG', 'IncidentPredictionBatchStates')
DYNAMODB_TABLE = DYNAMODB_RESOURCE.Table(DYNAMODB_TABLE_NAME)

SNS_CLIENT = boto3.client('sns')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN') # ARN cho thông báo chung của Aggregation
SNS_SUCCESS_TOPIC_ARN = os.environ.get('SNS_SUCCESS_TOPIC_ARN') # ARN cho thông báo thành công (nếu có)
SNS_FAILURE_TOPIC_ARN = os.environ.get('SNS_FAILURE_TOPIC_ARN') # ARN cho thông báo thất bại (nếu có)


def lambda_handler(event, context):
    """
    AggregationCleanLogLambda: Trigger bởi Step Functions.
    Input event từ Step Functions sẽ chứa (ví dụ):
    {
      // ... (có thể có output của Map state nếu ResultSelector không bỏ qua)
      "ListFilesOutput": { // Output từ bước ListCleanLogFilesLambda
          "BatchID": "incident1",
          "S3Bucket": "write-team19-bucket",
          "BatchFolderPath": "clean_data/incident1/",
          "CleanLogFiles": [
              {"S3Key": "clean_data/incident1/service_esb_file1.json", "ServiceNameFromFile": "service_esb"},
              {"S3Key": "clean_data/incident1/service_apigw_file1.json", "ServiceNameFromFile": "service_apigw"}
          ]
      }
    }
    """
    print(f"AggregationCleanLogLambda started. Event: {json.dumps(event, ensure_ascii=False)}")

    # Lấy BatchID và số file dự kiến từ input của Step Functions
    # Input của state này được cấu hình trong Step Function ASL là output của state "ListCleanLogFiles"
    # được lưu vào key "ListFilesOutput" của input hiện tại.
    list_files_output = event.get("ListFilesOutput", {})
    batch_id = list_files_output.get('BatchID')
    expected_file_count_from_list = len(list_files_output.get("CleanLogFiles", [])) if list_files_output.get("CleanLogFiles") else 0

    if not batch_id:
        error_msg = "AggregationCleanLogLambda: Missing BatchID in input event (expected from ListFilesOutput.BatchID)."
        print(f"ERROR: {error_msg}")
        # Không raise lỗi ở đây để có thể gửi SNS thất bại nếu cấu hình
        if SNS_FAILURE_TOPIC_ARN:
            SNS_CLIENT.publish(TopicArn=SNS_FAILURE_TOPIC_ARN, Message=f"Aggregation FAILED for unknown BatchID. Event: {json.dumps(event, ensure_ascii=False)}", Subject="[AI Batch Predictor ERROR] Unknown BatchID")
        raise ValueError(error_msg) # Hoặc trả về lỗi cho Step Functions

    print(f"AggregationCleanLogLambda: Processing BatchID '{batch_id}'. Expected total files listed by SFN: {expected_file_count_from_list}")

    all_service_phenomena_full_details = [] # Sẽ chứa nội dung chi tiết (đã parse từ JSON trên S3)
    processed_item_count_in_db = 0 # Đếm số item tìm thấy trong DB cho batch này
    successfully_read_details_count = 0 # Đếm số file detail đọc thành công từ S3
    worker_reported_errors = [] # Thu thập lỗi từ worker
    
    try:
        # Query DynamoDB để lấy tất cả items cho BatchID
        last_evaluated_key = None
        while True:
            query_kwargs = {
                'KeyConditionExpression': boto3.dynamodb.conditions.Key('BatchID').eq(batch_id)
            }
            if last_evaluated_key:
                query_kwargs['ExclusiveStartKey'] = last_evaluated_key
            
            print(f"AggregationCleanLogLambda: Querying DynamoDB for BatchID '{batch_id}' with LastEvaluatedKey: {last_evaluated_key}")
            response_db = DYNAMODB_TABLE.query(**query_kwargs)
            
            items_in_page = response_db.get('Items', [])
            print(f"AggregationCleanLogLambda: Found {len(items_in_page)} items in current DynamoDB page for BatchID '{batch_id}'.")

            for item in items_in_page:
                processed_item_count_in_db += 1
                item_s3_object_key_for_sort = item.get('S3ObjectKey', 'UnknownS3ObjectKey') # Sort key từ DB

                # Chỉ xử lý những item mà worker đã xử lý thành công và có S3 URI đến file chi tiết
                if item.get('Status') == 'PROCESSED_S3_DETAILS_V2' and 'DetailedPhenomenaS3URI' in item:
                    s3_uri_detail = item['DetailedPhenomenaS3URI']
                    try:
                        if s3_uri_detail and s3_uri_detail.startswith("s3://"):
                            parts = s3_uri_detail[5:].split('/', 1)
                            detail_bucket = parts[0]
                            detail_key = parts[1]
                            
                            print(f"AggregationCleanLogLambda: Fetching detailed phenomena from {s3_uri_detail} for original S3ObjectKey '{item_s3_object_key_for_sort}'")
                            s3_detail_response = S3_CLIENT.get_object(Bucket=detail_bucket, Key=detail_key)
                            phenomena_detail_content = s3_detail_response['Body'].read().decode('utf-8')
                            phenomena_detail_dict = json.loads(phenomena_detail_content) # Đây là extracted_phenomena_full từ worker
                            
                            # Thêm thông tin nguồn để dễ truy vết nếu cần
                            phenomena_detail_dict['_source_s3_key_full_original_log'] = item.get('FullSourceS3Key', 'N/A')
                            phenomena_detail_dict['_service_name_from_dynamo'] = item.get('ServiceName', 'N/A') # ServiceName đã được Worker lưu
                            all_service_phenomena_full_details.append(phenomena_detail_dict)
                            successfully_read_details_count +=1
                        else:
                            msg = f"Invalid S3 URI format for details in DynamoDB item: {s3_uri_detail} for S3ObjectKey {item_s3_object_key_for_sort}"
                            print(f"WARNING AggregationCleanLogLambda: {msg}")
                            worker_reported_errors.append(f"S3ObjectKey '{item_s3_object_key_for_sort}': {msg}")

                    except Exception as s3_read_e:
                        msg = f"Failed to read/parse detail from {s3_uri_detail} for S3ObjectKey {item_s3_object_key_for_sort}. Error: {s3_read_e}"
                        print(f"ERROR AggregationCleanLogLambda: {msg}")
                        traceback.print_exc()
                        worker_reported_errors.append(f"S3ObjectKey '{item_s3_object_key_for_sort}': {msg}")
                        # Không thêm vào all_service_phenomena_full_details nếu đọc S3 lỗi

                elif item.get('Status', '').startswith('ERROR_'): # Lỗi từ Worker
                    msg = f"Worker reported error for S3ObjectKey '{item_s3_object_key_for_sort}'. Status: {item.get('Status')}, Msg: {item.get('ErrorMessage')}"
                    print(f"WARNING AggregationCleanLogLambda: {msg}")
                    worker_reported_errors.append(msg)
                else:
                    print(f"INFO AggregationCleanLogLambda: Skipping DynamoDB item for S3ObjectKey '{item_s3_object_key_for_sort}' with status '{item.get('Status')}' or missing S3 URI.")
            
            last_evaluated_key = response_db.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break # Thoát vòng lặp while nếu không còn trang nào
        
        print(f"AggregationCleanLogLambda: Retrieved and processed details for {successfully_read_details_count} items from S3 for BatchID '{batch_id}'. Total items found in DB for this batch: {processed_item_count_in_db}.")
        if worker_reported_errors:
             print(f"AggregationCleanLogLambda: Encountered {len(worker_reported_errors)} issues/errors reported by workers or during S3 detail fetch for BatchID '{batch_id}'. First error: {worker_reported_errors[0]}")


        # So sánh số lượng file xử lý thành công với số lượng dự kiến từ Step Functions input
        if expected_file_count_from_list > 0 and successfully_read_details_count < expected_file_count_from_list:
            print(f"WARNING AggregationCleanLogLambda: Number of successfully retrieved S3 details ({successfully_read_details_count}) is less than expected file count from SFN input ({expected_file_count_from_list}). Some workers might have failed, S3 detail files might be missing/corrupt, or worker errors occurred.")
        elif expected_file_count_from_list == 0 and successfully_read_details_count > 0:
             print(f"INFO AggregationCleanLogLambda: Expected file count was 0, but found {successfully_read_details_count} processed details. Proceeding.")
        elif expected_file_count_from_list > 0 and successfully_read_details_count >= expected_file_count_from_list:
            print(f"INFO AggregationCleanLogLambda: Successfully retrieved details for all {successfully_read_details_count} (expected {expected_file_count_from_list}) files.")


        if not all_service_phenomena_full_details: # Nếu không có file detail nào được đọc thành công
            no_data_msg = f"No processed phenomena details found (after S3 read) for BatchID '{batch_id}'. Cannot proceed with prediction."
            print(f"AggregationCleanLogLambda: {no_data_msg}")
            if SNS_FAILURE_TOPIC_ARN: # Gửi thông báo thất bại
                 SNS_CLIENT.publish(TopicArn=SNS_FAILURE_TOPIC_ARN, Message=f"Aggregation FAILED for Batch {batch_id}: {no_data_msg}\nWorker Errors: {json.dumps(worker_reported_errors, ensure_ascii=False)}", Subject=f"[AI Batch Predictor ERROR] No Detail Data for Batch {batch_id}")
            return {"status": "no_detail_data_to_predict_from_s3", "batch_id": batch_id, "worker_errors": worker_reported_errors}

    except Exception as db_e: # Lỗi khi query DynamoDB
        error_message = f"Failed to query DynamoDB or process S3 details for BatchID '{batch_id}'. Error: {db_e}"
        print(f"ERROR AggregationCleanLogLambda: {error_message}")
        traceback.print_exc()
        if SNS_FAILURE_TOPIC_ARN:
            SNS_CLIENT.publish(TopicArn=SNS_FAILURE_TOPIC_ARN, Message=f"Aggregation FAILED for Batch {batch_id}: {error_message}\nTraceback: {traceback.format_exc()}", Subject=f"[AI Batch Predictor CRITICAL ERROR] Batch {batch_id}")
        raise db_e # Raise lỗi để Step Functions biết và xử lý retry/catch

    # --- Bắt đầu tổng hợp thông tin từ all_service_phenomena_full_details ---
    # Cấu trúc `aggregated_state_for_llm` mà `_construct_llm_prompt` mong đợi:
    # {
    #     "batch_id": "...",
    #     "overall_summary": "...",
    #     "service_phenomena": [ // Đây là list các `extracted_phenomena_full` từ mỗi file log
    #         { "service_name": "service_esb", "_source_s3_key_full_original_log": "...", "errors_summary": [...], ... },
    #         ...
    #     ]
    # }
    aggregated_state_for_llm = {
        "batch_id": batch_id,
        "overall_summary": f"Phân tích tổng hợp từ {successfully_read_details_count} file log (trong số {expected_file_count_from_list} file dự kiến) cho batch '{batch_id}'.",
        "service_phenomena": all_service_phenomena_full_details # Truyền trực tiếp list các dict phenomena đầy đủ đã đọc từ S3
    }
    
    total_errors_summarized = 0
    for sp in aggregated_state_for_llm["service_phenomena"]:
        total_errors_summarized += len(sp.get("errors_summary",[]))

    print(f"AggregationCleanLogLambda: System state aggregated for LLM for BatchID '{batch_id}'. Total services/files with details: {len(all_service_phenomena_full_details)}, Total error summaries collected: {total_errors_summarized}")

    # Tạo Query tổng hợp cho Knowledge Base
    kb_query_parts_agg = [f"Hệ thống đang có các vấn đề tổng hợp từ các log của batch '{batch_id}' như sau:"]
    # Lấy thông tin từ tối đa 3 service/file đầu tiên để tạo query KB
    for i, service_data in enumerate(aggregated_state_for_llm["service_phenomena"][:3]): 
        service_name_display = service_data.get("service_name", service_data.get("_service_name_from_dynamo", "Dịch vụ không xác định"))
        errors_summary_list = service_data.get("errors_summary", [])
        metrics_summary_dict = service_data.get("metrics_summary", {})
        
        source_file_display = os.path.basename(service_data.get("_source_s3_key_full_original_log","ẩn"))

        kb_query_parts_agg.append(f"\n- Từ nguồn log '{source_file_display}' (Dịch vụ: {service_name_display}):")
        if errors_summary_list:
            kb_query_parts_agg.append(f"  - Ghi nhận {len(errors_summary_list)} tóm tắt lỗi/hiện tượng. Ví dụ nổi bật: '{errors_summary_list[0] if errors_summary_list else 'Không có ví dụ cụ thể.'}'")
        if metrics_summary_dict:
            # Lấy một vài metric chính để đưa vào query
            main_metrics_for_query = {k:v for k,v in list(metrics_summary_dict.items())[:2]} # Ví dụ 2 metric đầu
            if main_metrics_for_query:
                kb_query_parts_agg.append(f"  - Metrics đáng chú ý: {json.dumps(main_metrics_for_query, ensure_ascii=False)}")
    if not aggregated_state_for_llm["service_phenomena"]:
         kb_query_parts_agg.append("- Không có hiện tượng cụ thể nào được ghi nhận từ các file log trong batch này.")

    kb_query_parts_agg.append("\nDựa trên các hiện tượng tổng thể này (nếu có), hãy tìm các sự cố trong quá khứ có các dấu hiệu tương tự trên nhiều dịch vụ (hoặc một dịch vụ chính yếu) và cung cấp thông tin về nguyên nhân, giải pháp đã thực hiện liên quan nhất.")
    kb_query_aggregated = "\n".join(kb_query_parts_agg)
    print(f"AggregationCleanLogLambda: Knowledge Base Retrieve Query (Aggregated, first 500 chars): {kb_query_aggregated[:500]}...")

    retrieved_chunks_raw_agg = retrieve_from_knowledge_base(kb_query_aggregated, number_of_results=3) # Lấy 3 chunks

    # Gọi synthesize_prediction_with_llm
    # Hàm _construct_llm_prompt trong bedrock_client.py đã được cập nhật để xử lý cấu trúc này
    print(f"AggregationCleanLogLambda: Synthesizing final prediction using LLM for BatchID '{batch_id}'.")
    final_llm_prediction_text = synthesize_prediction_with_llm(
        aggregated_state_for_llm, 
        retrieved_chunks_raw_agg,
        use_streaming=False # Hoặc True nếu bạn muốn và đã cấu hình LLM_MAX_TOKENS phù hợp
    )

    # Chuẩn bị payload cuối cùng và gửi đến EC2 API
    final_payload_to_ec2 = {
        "batch_id": batch_id,
        "aggregated_system_summary_for_ec2": { 
            "total_log_sources_processed_successfully": successfully_read_details_count,
            "expected_log_sources": expected_file_count_from_list,
            "services_with_issues_count": len([s for s in aggregated_state_for_llm["service_phenomena"] if s.get("errors_summary") or s.get("metrics_summary")]), # Đếm service có lỗi/metric
            "total_error_summaries_in_batch": total_errors_summarized,
            "worker_reported_errors_count": len(worker_reported_errors)
        },
        "retrieved_knowledge_chunks_summary_for_ec2": [],
        "final_llm_prediction": final_llm_prediction_text
    }
    if retrieved_chunks_raw_agg:
        for chunk_data in retrieved_chunks_raw_agg[:3]: 
            final_payload_to_ec2["retrieved_knowledge_chunks_summary_for_ec2"].append({
                "source_s3_uri": os.path.basename(chunk_data.get('location', {}).get('s3Location', {}).get('uri', 'N/A')),
                "retrieval_score": chunk_data.get('score'),
                "excerpt": chunk_data.get('content', {}).get('text', '')[:250] + "..."
            })
            
    print(f"AggregationCleanLogLambda: Attempting to send final aggregated payload to EC2 API for BatchID '{batch_id}'.")
    api_call_successful = send_prediction_to_ec2(final_payload_to_ec2)

    # Gửi thông báo SNS thành công
    if SNS_SUCCESS_TOPIC_ARN : # Kiểm tra nếu có SNS topic cho success
        sns_message_success = f"Phân tích tổng hợp (S3 Details) thành công cho BatchID: {batch_id}\n\n"
        sns_message_success += f"Số file log dự kiến: {expected_file_count_from_list}, Đã đọc chi tiết thành công: {successfully_read_details_count}\n"
        sns_message_success += f"Số lỗi từ worker (nếu có): {len(worker_reported_errors)}\n"
        sns_message_success += f"Dự đoán từ LLM (trích đoạn):\n{final_llm_prediction_text[:1000]}...\n\n"
        sns_message_success += f"Trạng thái gửi đến EC2 API: {'Thành công' if api_call_successful else 'Thất bại'}"
        
        print(f"AggregationCleanLogLambda: Sending SUCCESS notification to SNS for BatchID '{batch_id}'.")
        try:
            SNS_CLIENT.publish(TopicArn=SNS_SUCCESS_TOPIC_ARN, Message=sns_message_success, Subject=f"[AI Batch Predictor OK] Hoàn tất Batch {batch_id}")
        except Exception as sns_e:
            print(f"ERROR AggregationCleanLogLambda: SNS success publish failed for BatchID '{batch_id}': {sns_e}")
    elif SNS_TOPIC_ARN : # Fallback về SNS_TOPIC_ARN chung nếu success topic không có
        # ... (Logic gửi SNS message tương tự như trước, tùy chỉnh nội dung) ...
        pass


    return {
        'statusCode': 200,
        'body': json.dumps({
            "message": f"AggregationCleanLogLambda finished successfully for BatchID {batch_id}. Successfully processed {successfully_read_details_count} detail files.",
            "api_call_successful": api_call_successful,
            "final_prediction_excerpt": (final_llm_prediction_text[:200] + "..." if final_llm_prediction_text else "N/A")
        }, ensure_ascii=False)
    }
