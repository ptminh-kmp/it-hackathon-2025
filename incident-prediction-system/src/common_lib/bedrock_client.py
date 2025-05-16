# src/common_lib/bedrock_client.py
import boto3
import os
import json
import traceback

# Biến toàn cục cho client, khởi tạo một lần để tái sử dụng
BEDROCK_RUNTIME_CLIENT = None

# Lấy cấu hình model và các tham số từ biến môi trường, với giá trị mặc định
LLM_PREDICTION_MODEL_ARN = os.environ.get('LLM_PREDICTION_MODEL_ARN') # Bắt buộc phải có
ANTHROPIC_VERSION = os.environ.get("ANTHROPIC_VERSION", "bedrock-2023-05-31")
LLM_MAX_TOKENS = int(os.environ.get("LLM_MAX_TOKENS", 8000))
LLM_TEMPERATURE = float(os.environ.get("LLM_TEMPERATURE", 0.1))
LLM_TOP_P = float(os.environ.get("LLM_TOP_P", 0.9))
AWS_REGION_LLM = os.environ.get('AWS_REGION', 'us-east-1') # Mặc định là us-east-1 nếu không được set

def get_bedrock_runtime_client():
    """
    Khởi tạo và trả về Bedrock Runtime client.
    Sử dụng biến toàn cục để tránh khởi tạo lại mỗi lần gọi.
    """
    global BEDROCK_RUNTIME_CLIENT
    if BEDROCK_RUNTIME_CLIENT is None:
        print(f"Bedrock LLM Client: Initializing Bedrock Runtime client for region: {AWS_REGION_LLM}")
        try:
            BEDROCK_RUNTIME_CLIENT = boto3.client(
                service_name='bedrock-runtime',
                region_name=AWS_REGION_LLM
            )
        except Exception as e_client:
            print(f"ERROR Bedrock LLM Client: Failed to initialize Bedrock Runtime client for region {AWS_REGION_LLM}: {e_client}")
            traceback.print_exc()
            raise # Raise lại lỗi để Lambda/Step Functions biết và xử lý
    return BEDROCK_RUNTIME_CLIENT

def _construct_llm_prompt(aggregated_state_input: dict, retrieved_kb_chunks: list) -> str:
    """
    Xây dựng prompt chi tiết cho LLM dựa trên trạng thái tổng hợp từ nhiều service phenomena
    (đã đọc từ S3) và các chunks từ Knowledge Base.

    `aggregated_state_input` có cấu trúc (ví dụ):
    {
        "batch_id": "incident1",
        "overall_summary": "Phân tích từ 3 file log đã xử lý cho batch incident1.",
        "service_phenomena": [ // Đây là list các extracted_phenomena_full từ mỗi file log
            { 
                "service_name": "ESB Service", 
                "_source_s3_key_full_original_log": "clean_data/incident1/service_esb_cleaned_batch1_file1.json",
                "errors_summary": ["Lỗi ESB A: Timeout...", "Lỗi ESB B: NullPointer..."], 
                "metrics_summary": {"esb_total_error_events_in_batch": 10, "esb_errors_by_operation_from_csv": {"OP1": 5, "OP2": 5}},
                "other_key_info": ["Keyword 'timeout' in msg: ..."]
            },
            { 
                "service_name": "API Gateway Service", 
                "_source_s3_key_full_original_log": "clean_data/incident1/apigw_cleaned_batch1_file1.txt",
                "errors_summary": ["APIGW Error: Status 503..."], 
                "metrics_summary": {"apigw_high_latency_requests_count": 5, "apigw_avg_elapsed_time_ms": 2200.50},
                "other_key_info": []
            }
        ]
    }
    """
    prompt_parts = [
        "Bạn là một chuyên gia AI với nhiều kinh nghiệm trong việc phân tích log hệ thống và dự đoán sự cố IT. Nhiệm vụ của bạn là dựa vào các thông tin tổng hợp từ các file log đã được làm sạch của nhiều dịch vụ (mỗi file log đại diện cho một khoảng thời gian hoặc một thành phần cụ thể) và thông tin từ Knowledge Base (nếu có) để đưa ra dự đoán về các sự cố tiềm ẩn, phân tích nguyên nhân có thể và đề xuất các hành động cụ thể.",
        f"\n\n--- THÔNG TIN HIỆN TRẠNG HỆ THỐNG TỔNG HỢP (TỪ BATCH ID: {aggregated_state_input.get('batch_id', 'Không xác định')}) ---",
        aggregated_state_input.get('overall_summary', "Phân tích từ nhiều nguồn log dịch vụ.")
    ]

    service_phenomena_list = aggregated_state_input.get("service_phenomena", [])
    if service_phenomena_list:
        prompt_parts.append("\n**Chi tiết hiện tượng theo từng nguồn log/dịch vụ đã phân tích (tối đa 3 nguồn đầu tiên được liệt kê chi tiết):**")
        for i, service_data_full in enumerate(service_phenomena_list[:3]): 
            original_log_source = os.path.basename(service_data_full.get("_source_s3_key_full_original_log", "Không rõ nguồn log gốc"))
            service_name_display = service_data_full.get("service_name", "Dịch vụ không xác định")
            prompt_parts.append(f"\n**Nguồn/Dịch vụ {i+1}: {service_name_display} (từ file: {original_log_source})**")
            
            errors_summary = service_data_full.get("errors_summary", [])
            if errors_summary:
                prompt_parts.append("  - Tóm tắt các lỗi/hiện tượng bất thường chính:")
                for err_idx, err_sum in enumerate(errors_summary[:3]): 
                    prompt_parts.append(f"    - {err_sum}")
            else:
                prompt_parts.append("  - Không có lỗi/hiện tượng bất thường nào được tóm tắt từ nguồn này.")
            
            metrics_summary = service_data_full.get("metrics_summary", {})
            if metrics_summary:
                prompt_parts.append("  - Tóm tắt các metrics quan trọng:")
                for m_key, m_val in list(metrics_summary.items())[:3]: 
                    prompt_parts.append(f"    - {m_key}: {m_val}")
            
            other_info = service_data_full.get("other_key_info", []) 
            if other_info:
                prompt_parts.append("  - Thông tin quan trọng khác từ nguồn này:")
                for info_idx, info_item in enumerate(other_info[:2]): 
                    prompt_parts.append(f"    - {info_item}")
        
        if len(service_phenomena_list) > 3:
            prompt_parts.append(f"\n... và còn {len(service_phenomena_list) - 3} nguồn log/dịch vụ khác đã được phân tích (không hiển thị chi tiết ở đây để giữ prompt ngắn gọn).")
    else:
        prompt_parts.append("\n(Không có dữ liệu hiện tượng chi tiết từ các dịch vụ/nguồn log được cung cấp trong batch này để phân tích.)")

    if retrieved_kb_chunks:
        prompt_parts.append("\n\n--- THÔNG TIN THAM KHẢO TỪ KNOWLEDGE BASE (SỰ CỐ TƯƠNG TỰ TRONG QUÁ KHỨ) ---")
        for i, chunk in enumerate(retrieved_kb_chunks[:3]): # Giới hạn số lượng chunks đưa vào prompt
            content_text = chunk.get('content', {}).get('text', 'Không có nội dung chi tiết từ KB.')
            s3_uri_full = chunk.get('location', {}).get('s3Location', {}).get('uri', 'Không rõ nguồn')
            s3_filename = os.path.basename(s3_uri_full) if s3_uri_full != 'N/A' else 'N/A'
            score_value = chunk.get('score')
            score_str = f"{score_value:.4f}" if isinstance(score_value, float) else str(score_value)
            prompt_parts.append(f"\n**Sự cố quá khứ tham khảo {i+1} (Nguồn: {s3_filename}, Mức độ liên quan với truy vấn hiện tại: {score_str}):**")
            prompt_parts.append(content_text[:1000] + ("..." if len(content_text) > 1000 else "")) # Giới hạn độ dài mỗi chunk
    else:
        prompt_parts.append("\n\n(Không tìm thấy sự cố nào tương tự trong Knowledge Base hoặc không thể truy vấn được.)")

    prompt_parts.append(
        "\n\n--- YÊU CẦU PHÂN TÍCH VÀ DỰ ĐOÁN ---"
        "\nDựa trên TOÀN BỘ thông tin được cung cấp ở trên (cả hiện trạng hệ thống tổng hợp và các sự cố quá khứ từ Knowledge Base nếu có), hãy thực hiện các yêu cầu sau:"
        "\n1.  **Đánh giá Hiện tượng Chính:** Tóm tắt ngắn gọn và nêu bật các hiện tượng bất thường quan trọng nhất đang xảy ra trên toàn hệ thống."
        "\n2.  **Liên kết với Quá khứ (Nếu có KB data):** Phân tích xem các hiện tượng hiện tại có những điểm tương đồng nào đáng kể với các sự cố đã được ghi nhận trong Knowledge Base. Nếu có, sự cố quá khứ nào là tương đồng nhất và tại sao?"
        "\n3.  **Dự đoán Sự cố Tiềm ẩn:** Liệt kê các sự cố cụ thể (ví dụ: dịch vụ X ngừng hoạt động, hiệu năng dịch vụ Y suy giảm nghiêm trọng, nguy cơ mất dữ liệu Z) có khả năng cao sẽ xảy ra trong tương lai gần (ví dụ: vài giờ tới, trong ngày). Với mỗi sự cố dự đoán, hãy giải thích rõ ràng lý do dựa trên bằng chứng từ log hiện tại và (nếu có) sự tương đồng với sự cố quá khứ."
        "\n4.  **Phân tích Nguyên nhân Gốc rễ (Giả thuyết):** Với mỗi sự cố tiềm ẩn đã dự đoán, đưa ra giả thuyết về (các) nguyên nhân gốc rễ có khả năng nhất. Hãy xem xét mối tương quan giữa các dịch vụ nếu có."
        "\n5.  **Đề xuất Hành động Khẩn cấp:** Đưa ra danh sách các hành động kiểm tra, xác minh, hoặc ngăn chặn cụ thể và ưu tiên mà đội ngũ vận hành nên thực hiện NGAY LẬP TỨC để giảm thiểu rủi ro hoặc khắc phục sớm các sự cố tiềm ẩn đã dự đoán."
        "\n\n**Yêu cầu định dạng:** Trình bày câu trả lời một cách chi tiết, rõ ràng, có cấu trúc theo từng mục trên. Sử dụng tiếng Việt. Hãy nhấn mạnh vào việc đưa ra các phân tích sâu sắc và các đề xuất hành động thiết thực."
    )
    
    return "\n".join(prompt_parts)

def synthesize_prediction_with_llm(aggregated_state_input: dict, retrieved_kb_chunks: list, use_streaming: bool = False):
    """
    Sử dụng một FM (ví dụ Claude 3.5 Sonnet) để tổng hợp hiện trạng hệ thống
    và thông tin từ KB để đưa ra dự đoán sự cố.
    Có thể chọn sử dụng streaming hoặc non-streaming.
    """
    if not LLM_PREDICTION_MODEL_ARN:
        error_msg = "ERROR Bedrock LLM Client: LLM_PREDICTION_MODEL_ARN environment variable not set for final prediction synthesis."
        print(error_msg)
        return error_msg # Trả về thông báo lỗi để dễ debug hơn là raise

    try:
        client = get_bedrock_runtime_client()
    except Exception as client_init_e:
        return f"Lỗi khởi tạo Bedrock client: {client_init_e}"

    final_prompt = _construct_llm_prompt(aggregated_state_input, retrieved_kb_chunks)
    
    # Log một phần prompt để debug, tránh làm đầy CloudWatch Logs
    prompt_log_preview_length = 1500
    prompt_log_preview = final_prompt[:prompt_log_preview_length] + \
                         (f"... (and {len(final_prompt)-prompt_log_preview_length} more chars)" \
                          if len(final_prompt) > prompt_log_preview_length else "")
    print(f"DEBUG Bedrock LLM Client: Final prompt to LLM '{LLM_PREDICTION_MODEL_ARN}' (length: {len(final_prompt)} chars):\n{prompt_log_preview}")

    request_body = {
        "anthropic_version": ANTHROPIC_VERSION,
        "max_tokens": LLM_MAX_TOKENS, 
        "temperature": LLM_TEMPERATURE,
        "top_p": LLM_TOP_P,
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": final_prompt}]
            }
        ]
        # Có thể thêm "stop_sequences": ["\n\nHuman:"] cho một số model Claude cũ hơn
    }

    if use_streaming:
        print(f"Bedrock LLM Client: Invoking LLM (streaming): {LLM_PREDICTION_MODEL_ARN} with max_tokens: {LLM_MAX_TOKENS}")
        full_response_text = ""
        final_stop_reason = "unknown_streaming_stop"
        try:
            response_stream = client.invoke_model_with_response_stream(
                body=json.dumps(request_body),
                modelId=LLM_PREDICTION_MODEL_ARN,
                accept='application/json',
                contentType='application/json'
            )

            for event in response_stream.get('body'): # type: ignore
                chunk = json.loads(event['chunk']['bytes'])
                
                if chunk['type'] == 'content_block_delta':
                    if chunk['delta']['type'] == 'text_delta':
                        full_response_text += chunk['delta']['text']
                elif chunk['type'] == 'message_delta': # Một số phiên bản API/model có thể dùng cấu trúc này
                    if 'delta' in chunk and 'text' in chunk['delta']:
                         full_response_text += chunk['delta']['text']
                    if 'delta' in chunk and 'stop_reason' in chunk['delta'] and chunk['delta']['stop_reason']:
                        final_stop_reason = chunk['delta']['stop_reason']
                elif chunk['type'] == 'message_stop':
                    invocation_metrics = chunk.get('amazon-bedrock-invocationMetrics', {})
                    final_stop_reason = invocation_metrics.get('stopReason', final_stop_reason)
                    break 

            print(f"DEBUG Bedrock LLM Client (Streaming): Final Stop Reason: {final_stop_reason}")
            if final_stop_reason == "max_tokens":
                print("WARNING Bedrock LLM Client: LLM output (streaming) may be TRUNCATED because 'max_tokens' limit was reached.")
                full_response_text += "\n\n[CẢNH BÁO: Kết quả có thể đã bị cắt ngắn do đạt giới hạn token tối đa cho output.]"
            
            print(f"DEBUG Bedrock LLM Client (Streaming): Full synthesized text length: {len(full_response_text)}")
            return full_response_text

        except Exception as e_stream:
            error_msg = f"ERROR Bedrock LLM Client: Exception during LLM streaming invocation ({LLM_PREDICTION_MODEL_ARN}): {e_stream}"
            print(error_msg)
            traceback.print_exc()
            return f"Lỗi khi tổng hợp dự đoán từ LLM (streaming): {str(e_stream)}"
    else: # Non-streaming
        print(f"Bedrock LLM Client: Invoking LLM (non-streaming): {LLM_PREDICTION_MODEL_ARN} with max_tokens: {LLM_MAX_TOKENS}")
        try:
            response = client.invoke_model(
                body=json.dumps(request_body),
                modelId=LLM_PREDICTION_MODEL_ARN,
                accept='application/json',
                contentType='application/json'
            )
            response_body = json.loads(response.get('body').read())
            
            prediction_text = "Lỗi: Không có nội dung trả về hợp lệ từ LLM." 
            stop_reason = response_body.get("stop_reason") 
            # Đối với Claude Messages API, stop_reason thường nằm ở cấp cao nhất của response_body
            # hoặc trong amazon-bedrock-invocationMetrics cho một số cấu hình/phiên bản
            if not stop_reason and 'amazon-bedrock-invocationMetrics' in response_body:
                 stop_reason = response_body['amazon-bedrock-invocationMetrics'].get('stopReason')
            
            print(f"DEBUG Bedrock LLM Client (Non-Streaming): LLM Stop Reason: {stop_reason if stop_reason else 'Not explicitly found'}")

            if response_body.get("content") and isinstance(response_body["content"], list) and len(response_body["content"]) > 0:
                if response_body["content"][0].get("type") == "text":
                    prediction_text = response_body["content"][0].get("text", "Nội dung text rỗng từ LLM.")
            
            if stop_reason == "max_tokens":
                print("WARNING Bedrock LLM Client: LLM output (non-streaming) may be TRUNCATED because 'max_tokens' limit was reached.")
                prediction_text += "\n\n[CẢNH BÁO: Kết quả có thể đã bị cắt ngắn do đạt giới hạn token tối đa cho output.]"
            
            # Log một phần response body để debug (có thể chứa nhiều thông tin hơn chỉ là text)
            # print(f"DEBUG Bedrock LLM Client (Non-Streaming): LLM Raw Response Body (first 500 chars): {str(response_body)[:500]}...")
            print(f"DEBUG Bedrock LLM Client (Non-Streaming): Extracted Prediction Text (first 500 chars): {prediction_text[:500]}...")
            return prediction_text

        except Exception as e_invoke:
            error_msg = f"ERROR Bedrock LLM Client: Exception during non-streaming LLM invocation ({LLM_PREDICTION_MODEL_ARN}): {e_invoke}"
            print(error_msg)
            traceback.print_exc()
            return f"Lỗi khi tổng hợp dự đoán từ LLM (non-streaming): {str(e_invoke)}"

# Trong app.py của AggregationCleanLogLambda, bạn sẽ gọi:
# final_llm_prediction_text = synthesize_prediction_with_llm(
#     aggregated_state_for_llm, 
#     retrieved_chunks_raw_agg,
#     use_streaming=False  # Hoặc True nếu muốn thử streaming
# )
