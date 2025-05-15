import boto3
import os
import json
import traceback

BEDROCK_RUNTIME_CLIENT = None
LLM_PREDICTION_MODEL_ARN = os.environ.get('LLM_PREDICTION_MODEL_ARN')
ANTHROPIC_VERSION = os.environ.get("ANTHROPIC_VERSION", "bedrock-2023-05-31")
LLM_MAX_TOKENS = int(os.environ.get("LLM_MAX_TOKENS", 8000))
LLM_TEMPERATURE = float(os.environ.get("LLM_TEMPERATURE", 0.1))
LLM_TOP_P = float(os.environ.get("LLM_TOP_P", 0.9))
AWS_REGION_LLM = os.environ.get('AWS_REGION', 'us-east-1')

def get_bedrock_runtime_client():
    global BEDROCK_RUNTIME_CLIENT
    if BEDROCK_RUNTIME_CLIENT is None:
        print(f"Bedrock LLM Client: Initializing Bedrock Runtime client for region: {AWS_REGION_LLM}")
        try:
            BEDROCK_RUNTIME_CLIENT = boto3.client(
                service_name='bedrock-runtime',
                region_name=AWS_REGION_LLM
            )
        except Exception as e_client:
            print(f"ERROR Bedrock LLM Client: Failed to initialize Bedrock Runtime client: {e_client}")
            traceback.print_exc()
            raise
    return BEDROCK_RUNTIME_CLIENT

def _construct_llm_prompt(aggregated_state_from_clean_logs: dict, retrieved_kb_chunks: list) -> str:
    prompt_parts = [
        "Bạn là một chuyên gia AI phân tích và dự đoán sự cố hệ thống IT. Nhiệm vụ của bạn là dựa vào các thông tin tổng hợp từ các file log đã được làm sạch của nhiều dịch vụ và thông tin từ Knowledge Base (nếu có) để đưa ra dự đoán về các sự cố tiềm ẩn, nguyên nhân và hành động gợi ý.",
        f"\n\nThông tin Hiện trạng Hệ thống Tổng hợp (từ Batch ID: {aggregated_state_from_clean_logs.get('batch_id', 'N/A')}):",
        aggregated_state_from_clean_logs.get('overall_summary', "Không có tóm tắt tổng quan từ batch.")
    ]

    service_phenomena = aggregated_state_from_clean_logs.get("service_phenomena", [])
    if service_phenomena:
        prompt_parts.append("\nChi tiết hiện tượng theo từng dịch vụ (tối đa 3 dịch vụ đầu tiên được liệt kê):")
        for i, service_data in enumerate(service_phenomena[:3]):
            prompt_parts.append(f"\n- Dịch vụ: {service_data.get('service_name', 'Không rõ tên')}")
            if "source_file" in service_data:
                prompt_parts.append(f"  (Nguồn dữ liệu đã clean: {service_data.get('source_file')})")
            
            errors_summary = service_data.get("errors_summary", [])
            if errors_summary:
                prompt_parts.append("  - Tóm tắt các lỗi/hiện tượng bất thường chính:")
                for err_idx, err_sum in enumerate(errors_summary[:3]):
                    prompt_parts.append(f"    - {err_sum}")
            
            metrics_summary = service_data.get("metrics_summary", {})
            if metrics_summary:
                prompt_parts.append("  - Tóm tắt các metrics quan trọng:")
                for m_key, m_val in list(metrics_summary.items())[:3]:
                    prompt_parts.append(f"    - {m_key}: {m_val}")
            
            other_info = service_data.get("other_key_info", [])
            if other_info:
                prompt_parts.append("  - Thông tin quan trọng khác từ service này:")
                for info_idx, info_item in enumerate(other_info[:2]):
                    prompt_parts.append(f"    - {info_item}")
    else:
        prompt_parts.append("\nKhông có dữ liệu hiện tượng chi tiết từ các dịch vụ được cung cấp trong batch này.")

    if retrieved_kb_chunks:
        prompt_parts.append("\n\nThông tin từ Knowledge Base về các sự cố tương tự trong quá khứ (tối đa 3 sự cố liên quan nhất):")
        for i, chunk in enumerate(retrieved_kb_chunks[:3]): 
            content_text = chunk.get('content', {}).get('text', 'Không có nội dung chi tiết từ KB.')
            s3_uri_full = chunk.get('location', {}).get('s3Location', {}).get('uri', 'Không rõ nguồn')
            s3_filename = os.path.basename(s3_uri_full) if s3_uri_full != 'N/A' else 'N/A'
            score_value = chunk.get('score')
            score_str = f"{score_value:.4f}" if isinstance(score_value, float) else str(score_value)
            prompt_parts.append(f"\nSự cố quá khứ {i+1} (Nguồn tham khảo: {s3_filename}, Mức độ liên quan: {score_str}):")
            prompt_parts.append(content_text) 
    else:
        prompt_parts.append("\n\nKhông tìm thấy sự cố nào tương tự trong Knowledge Base hoặc không thể truy vấn được.")

    prompt_parts.append(
        "\n\nYêu cầu Phân tích và Dự đoán:"
        "\n1. Hiện tượng chính: Tóm tắt các hiện tượng bất thường chính đang xảy ra trên toàn hệ thống dựa trên dữ liệu tổng hợp đã cung cấp."
        "\n2. Liên kết với quá khứ: Các hiện tượng này có điểm tương đồng nào nổi bật với các sự cố đã ghi nhận trong Knowledge Base (nếu có)?"
        "\n3. Dự đoán Sự cố Tiềm ẩn: Dựa trên TẤT CẢ thông tin trên, những sự cố cụ thể nào có khả năng cao sẽ xảy ra trong tương lai gần? Hãy liệt kê rõ ràng và giải thích."
        "\n4. Nguyên nhân có thể: Đưa ra giả thuyết về (các) nguyên nhân gốc rễ của các sự cố tiềm ẩn này."
        "\n5. Hành động Đề xuất: Người vận hành nên thực hiện ngay những hành động kiểm tra hoặc ngăn chặn cụ thể nào?"
        "\n\nHãy trình bày câu trả lời một cách chi tiết, rõ ràng, có cấu trúc theo từng mục trên, bằng tiếng Việt."
    )
    return "\n".join(prompt_parts)

def synthesize_prediction_with_llm(aggregated_state_from_clean_logs: dict, retrieved_kb_chunks: list):
    if not LLM_PREDICTION_MODEL_ARN:
        error_msg = "ERROR Bedrock LLM Client: LLM_PREDICTION_MODEL_ARN env var not set."
        print(error_msg)
        return error_msg
    try:
        client = get_bedrock_runtime_client()
    except Exception as client_init_e:
        return f"Lỗi khởi tạo Bedrock client: {client_init_e}"

    final_prompt = _construct_llm_prompt(aggregated_state_from_clean_logs, retrieved_kb_chunks)
    prompt_log_preview = final_prompt[:1500] + (f"... ({len(final_prompt)-1500} more chars)" if len(final_prompt) > 1500 else "")
    print(f"DEBUG Bedrock LLM Client: Final prompt to LLM '{LLM_PREDICTION_MODEL_ARN}':\n{prompt_log_preview}")

    request_body = {
        "anthropic_version": ANTHROPIC_VERSION,
        "max_tokens": LLM_MAX_TOKENS,
        "temperature": LLM_TEMPERATURE,
        "top_p": LLM_TOP_P,
        "messages": [{"role": "user", "content": [{"type": "text", "text": final_prompt}]}]
    }
    try:
        print(f"Bedrock LLM Client: Invoking LLM (non-streaming): {LLM_PREDICTION_MODEL_ARN} with max_tokens: {request_body['max_tokens']}")
        response = client.invoke_model(
            body=json.dumps(request_body), modelId=LLM_PREDICTION_MODEL_ARN,
            accept='application/json', contentType='application/json'
        )
        response_body = json.loads(response.get('body').read())
        prediction_text = "Lỗi: Không có nội dung từ LLM."
        stop_reason = response_body.get("stop_reason", "N/A")
        if 'amazon-bedrock-invocationMetrics' in response_body:
            stop_reason = response_body['amazon-bedrock-invocationMetrics'].get('stopReason', stop_reason)
        print(f"DEBUG Bedrock LLM Client: LLM Stop Reason: {stop_reason}")

        if response_body.get("content") and isinstance(response_body["content"], list) and response_body["content"]:
            if response_body["content"][0].get("type") == "text":
                prediction_text = response_body["content"][0].get("text", "Nội dung text rỗng từ LLM.")
        if stop_reason == "max_tokens":
            print("WARNING Bedrock LLM Client: LLM output (non-streaming) TRUNCATED due to 'max_tokens'.")
            prediction_text += "\n\n[CẢNH BÁO: Kết quả có thể bị cắt ngắn do đạt giới hạn token.]"
        return prediction_text
    except Exception as e:
        error_msg = f"ERROR Bedrock LLM Client: Exception during non-streaming LLM invocation ({LLM_PREDICTION_MODEL_ARN}): {e}"
        print(error_msg)
        traceback.print_exc()
        return f"Lỗi khi tổng hợp dự đoán từ LLM (non-streaming): {str(e)}"

def synthesize_prediction_with_llm_streaming(aggregated_state_from_clean_logs: dict, retrieved_kb_chunks: list):
    if not LLM_PREDICTION_MODEL_ARN:
        error_msg = "ERROR Bedrock LLM Client: LLM_PREDICTION_MODEL_ARN env var not set."
        print(error_msg)
        return error_msg
    try:
        client = get_bedrock_runtime_client()
    except Exception as client_init_e:
        return f"Lỗi khởi tạo Bedrock client: {client_init_e}"
    
    final_prompt = _construct_llm_prompt(aggregated_state_from_clean_logs, retrieved_kb_chunks)
    prompt_log_preview = final_prompt[:1500] + (f"... ({len(final_prompt)-1500} more chars)" if len(final_prompt) > 1500 else "")
    print(f"DEBUG Bedrock LLM Client: Streaming prompt to LLM '{LLM_PREDICTION_MODEL_ARN}':\n{prompt_log_preview}")
    request_body = {
        "anthropic_version": ANTHROPIC_VERSION,
        "max_tokens": LLM_MAX_TOKENS,
        "temperature": LLM_TEMPERATURE,
        "top_p": LLM_TOP_P,
        "messages": [{"role": "user", "content": [{"type": "text", "text": final_prompt}]}]
    }
    print(f"Bedrock LLM Client: Invoking LLM (streaming): {LLM_PREDICTION_MODEL_ARN} with max_tokens: {request_body['max_tokens']}")
    full_response_text = ""
    final_stop_reason = "unknown"
    try:
        response_stream = client.invoke_model_with_response_stream(
            body=json.dumps(request_body), modelId=LLM_PREDICTION_MODEL_ARN,
            accept='application/json', contentType='application/json'
        )
        for event in response_stream.get('body'): # type: ignore
            chunk = json.loads(event['chunk']['bytes'])
            if chunk['type'] == 'content_block_delta' and chunk['delta']['type'] == 'text_delta':
                full_response_text += chunk['delta']['text']
            elif chunk['type'] == 'message_stop':
                invocation_metrics = chunk.get('amazon-bedrock-invocationMetrics', {})
                final_stop_reason = invocation_metrics.get('stopReason', final_stop_reason)
                break
        print(f"DEBUG Bedrock LLM Client (Streaming): Final Stop Reason: {final_stop_reason}")
        if final_stop_reason == "max_tokens":
            print("WARNING Bedrock LLM Client: LLM output (streaming) TRUNCATED due to 'max_tokens'.")
            full_response_text += "\n\n[CẢNH BÁO: Kết quả có thể bị cắt ngắn do đạt giới hạn token.]"
        return full_response_text
    except Exception as e:
        error_msg = f"ERROR Bedrock LLM Client: Exception during LLM streaming ({LLM_PREDICTION_MODEL_ARN}): {e}"
        print(error_msg)
        traceback.print_exc()
        return f"Lỗi khi tổng hợp dự đoán từ LLM (streaming): {str(e)}"