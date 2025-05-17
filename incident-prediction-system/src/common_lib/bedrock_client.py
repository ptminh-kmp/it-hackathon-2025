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
LLM_MAX_TOKENS = int(os.environ.get("LLM_MAX_TOKENS", 8000)) # Max tokens cho MỖI LƯỢT gọi
LLM_TEMPERATURE = float(os.environ.get("LLM_TEMPERATURE", 0.1))
LLM_TOP_P = float(os.environ.get("LLM_TOP_P", 0.9))
AWS_REGION_LLM = os.environ.get('AWS_REGION', 'us-east-1')

# Ngưỡng ký tự tối thiểu của tổng output trước khi thử "tiếp tục" nếu stop_reason là end_turn
LLM_MIN_LENGTH_FOR_CONTINUATION_THRESHOLD = int(os.environ.get("LLM_MIN_LENGTH_CONTINUE_THRES", "2000"))
# Số lần thử "tiếp tục" tối đa nếu model dừng với end_turn và output ngắn
LLM_MAX_CONTINUATION_ATTEMPTS = int(os.environ.get("LLM_MAX_CONTINUATION_ATTEMPTS", "2"))


def get_bedrock_runtime_client():
    """Khởi tạo và trả về Bedrock Runtime client."""
    global BEDROCK_RUNTIME_CLIENT
    if BEDROCK_RUNTIME_CLIENT is None:
        print(f"Bedrock LLM Client: Initializing Bedrock Runtime client for region: {AWS_REGION_LLM}")
        try:
            BEDROCK_RUNTIME_CLIENT = boto3.client(
                service_name='bedrock-runtime',
                region_name=AWS_REGION_LLM
            )
        except Exception as e_client:
            print(f"ERROR Bedrock LLM Client: Failed to initialize: {e_client}")
            traceback.print_exc()
            raise
    return BEDROCK_RUNTIME_CLIENT

def _construct_llm_prompt(aggregated_state_input: dict, retrieved_kb_chunks: list) -> str:
    """Xây dựng prompt chi tiết cho LLM (giữ nguyên như phiên bản trước)."""
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
                prompt_parts.append("  - Thông tin quan trọng khác từ service này:")
                for info_idx, info_item in enumerate(other_info[:2]): 
                    prompt_parts.append(f"    - {info_item}")
        if len(service_phenomena_list) > 3:
            prompt_parts.append(f"\n... và còn {len(service_phenomena_list) - 3} nguồn log/dịch vụ khác đã được phân tích (không hiển thị chi tiết ở đây để giữ prompt ngắn gọn).")
    else:
        prompt_parts.append("\n(Không có dữ liệu hiện tượng chi tiết từ các dịch vụ/nguồn log được cung cấp trong batch này để phân tích.)")

    if retrieved_kb_chunks:
        prompt_parts.append("\n\n--- THÔNG TIN THAM KHẢO TỪ KNOWLEDGE BASE (SỰ CỐ TƯƠNG TỰ TRONG QUÁ KHỨ) ---")
        for i, chunk in enumerate(retrieved_kb_chunks[:3]): 
            content_text = chunk.get('content', {}).get('text', 'Không có nội dung chi tiết từ KB.')
            s3_uri_full = chunk.get('location', {}).get('s3Location', {}).get('uri', 'Không rõ nguồn')
            s3_filename = os.path.basename(s3_uri_full) if s3_uri_full != 'N/A' else 'N/A'
            score_value = chunk.get('score')
            score_str = f"{score_value:.4f}" if isinstance(score_value, float) else str(score_value)
            prompt_parts.append(f"\nSự cố quá khứ tham khảo {i+1} (Nguồn: {s3_filename}, Mức độ liên quan với truy vấn hiện tại: {score_str}):")
            prompt_parts.append(content_text[:1000] + ("..." if len(content_text) > 1000 else ""))
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


def _invoke_llm_anthropic_messages_api(
    client,
    model_arn: str,
    messages: list, # Lịch sử hội thoại đầy đủ
    max_tokens_for_this_turn: int, # Max tokens cho lượt sinh này
    temperature: float,
    top_p: float,
    anthropic_version: str,
    use_streaming: bool
):
    """
    Hàm nội bộ để gọi Anthropic Messages API (streaming hoặc non-streaming).
    Trả về (generated_text_part, stop_reason).
    """
    request_body = {
        "anthropic_version": anthropic_version,
        "max_tokens": max_tokens_for_this_turn,
        "temperature": temperature,
        "top_p": top_p,
        "messages": messages # Toàn bộ lịch sử messages
    }
    
    generated_text_part = ""
    actual_stop_reason = "unknown"

    if use_streaming:
        print(f"Bedrock LLM Client: Invoking LLM (streaming): {model_arn} with max_tokens: {max_tokens_for_this_turn} for this turn.")
        try:
            response_stream = client.invoke_model_with_response_stream(
                body=json.dumps(request_body), modelId=model_arn,
                accept='application/json', contentType='application/json'
            )
            for event in response_stream.get('body'): # type: ignore
                chunk = json.loads(event['chunk']['bytes'])
                if chunk['type'] == 'content_block_delta' and chunk['delta']['type'] == 'text_delta':
                    generated_text_part += chunk['delta']['text']
                elif chunk['type'] == 'message_delta': # Một số SDK/model có thể dùng cấu trúc này
                    delta = chunk.get('delta', {})
                    if delta.get('type') == 'text_delta':
                         generated_text_part += delta.get('text', '')
                    if 'stop_reason' in delta and delta['stop_reason']: # Lấy stop_reason sớm nếu có trong delta
                        actual_stop_reason = delta['stop_reason']
                elif chunk['type'] == 'message_stop':
                    invocation_metrics = chunk.get('amazon-bedrock-invocationMetrics', {})
                    actual_stop_reason = invocation_metrics.get('stopReason', actual_stop_reason) # Ưu tiên từ metrics
                    break 
            print(f"DEBUG Bedrock LLM Client (Streaming): Turn finished. Stop Reason: '{actual_stop_reason}'")
        except Exception as e_stream:
            print(f"ERROR Bedrock LLM Client: Streaming Exception: {e_stream}")
            traceback.print_exc()
            return f"Lỗi khi gọi LLM (streaming): {str(e_stream)}", "error"
    else: # Non-streaming
        print(f"Bedrock LLM Client: Invoking LLM (non-streaming): {model_arn} with max_tokens: {max_tokens_for_this_turn} for this turn.")
        try:
            response = client.invoke_model(
                body=json.dumps(request_body), modelId=model_arn,
                accept='application/json', contentType='application/json'
            )
            response_body = json.loads(response.get('body').read())
            
            actual_stop_reason = response_body.get("stop_reason")
            if not actual_stop_reason and 'amazon-bedrock-invocationMetrics' in response_body:
                 actual_stop_reason = response_body['amazon-bedrock-invocationMetrics'].get('stopReason')
            
            if response_body.get("content") and isinstance(response_body["content"], list) and response_body["content"]:
                if response_body["content"][0].get("type") == "text":
                    generated_text_part = response_body["content"][0].get("text", "")
            print(f"DEBUG Bedrock LLM Client (Non-Streaming): Turn finished. Stop Reason: '{actual_stop_reason}'")
        except Exception as e_invoke:
            print(f"ERROR Bedrock LLM Client: Non-streaming Exception: {e_invoke}")
            traceback.print_exc()
            return f"Lỗi khi gọi LLM (non-streaming): {str(e_invoke)}", "error"
            
    if actual_stop_reason == "max_tokens":
        print(f"WARNING Bedrock LLM Client: Output for this turn may be TRUNCATED because 'max_tokens' ({max_tokens_for_this_turn}) limit was reached.")
        # Không tự thêm cảnh báo vào text ở đây, hàm cha sẽ quyết định
        
    return generated_text_part, actual_stop_reason


def synthesize_prediction_with_llm(
    aggregated_state_input: dict, 
    retrieved_kb_chunks: list, 
    use_streaming: bool = False # Mặc định là False, AggregationLambda sẽ quyết định
    ):
    """
    Sử dụng một FM để tổng hợp hiện trạng và thông tin KB, đưa ra dự đoán.
    Có khả năng thử gọi tiếp nếu model dừng với 'end_turn' và output chưa đủ dài.
    """
    if not LLM_PREDICTION_MODEL_ARN:
        error_msg = "ERROR Bedrock LLM Client: LLM_PREDICTION_MODEL_ARN env var not set."
        print(error_msg)
        return error_msg

    try:
        client = get_bedrock_runtime_client()
    except Exception as client_init_e:
        return f"Lỗi khởi tạo Bedrock client: {client_init_e}"

    initial_prompt_text = _construct_llm_prompt(aggregated_state_input, retrieved_kb_chunks)
    
    current_messages_history = [
        {"role": "user", "content": [{"type": "text", "text": initial_prompt_text}]}
    ]

    full_synthesized_text = ""
    continuation_attempts_done = 0
    
    # Tính toán max_tokens còn lại cho mỗi lượt. Giả sử prompt đầu tiên chiếm 1 phần.
    # Đây là cách đơn giản, thực tế cần ước lượng token của prompt.
    # Hiện tại, LLM_MAX_TOKENS sẽ là max cho MỖI lượt gọi.
    max_tokens_per_turn = LLM_MAX_TOKENS 

    while continuation_attempts_done <= LLM_MAX_CONTINUATION_ATTEMPTS:
        print(f"Bedrock LLM Client: Invocation turn {continuation_attempts_done + 1} for BatchID '{aggregated_state_input.get('batch_id', 'N/A')}'")
        
        # Log một phần của message cuối cùng (prompt của user cho lượt này)
        if current_messages_history and current_messages_history[-1]["role"] == "user":
            last_user_prompt_text = current_messages_history[-1]["content"][0]["text"]
            prompt_preview = last_user_prompt_text[:500] + (f"... (prompt length: {len(last_user_prompt_text)})" if len(last_user_prompt_text) > 500 else "")
            print(f"DEBUG Bedrock LLM Client: Prompting with (last user message preview for this turn):\n{prompt_preview}")

        generated_text_this_turn, stop_reason_this_turn = _invoke_llm_anthropic_messages_api(
            client,
            LLM_PREDICTION_MODEL_ARN,
            current_messages_history,
            max_tokens_per_turn, # Giới hạn token cho lượt sinh này
            LLM_TEMPERATURE,
            LLM_TOP_P,
            ANTHROPIC_VERSION,
            use_streaming
        )

        print(f"DEBUG Bedrock LLM Client: Turn {continuation_attempts_done + 1} - Stop Reason: '{stop_reason_this_turn}', Generated Part Length: {len(generated_text_this_turn)}")

        if stop_reason_this_turn == "error" or generated_text_this_turn.startswith("Lỗi khi gọi LLM"):
            full_synthesized_text += f"\n[Lỗi trong lượt gọi thứ {continuation_attempts_done + 1}: {generated_text_this_turn}]"
            break 

        full_synthesized_text += generated_text_this_turn # Nối phần mới sinh ra

        if stop_reason_this_turn == "max_tokens":
            print("INFO Bedrock LLM Client: Stopped due to max_tokens for this turn. Will not attempt further continuation as output is already at specified limit for a turn.")
            full_synthesized_text += "\n\n[CẢNH BÁO: Kết quả có thể đã bị cắt ngắn do đạt giới hạn token tối đa cho một lượt output.]"
            break 
        elif stop_reason_this_turn == "end_turn":
            if continuation_attempts_done < LLM_MAX_CONTINUATION_ATTEMPTS and \
               len(full_synthesized_text) < LLM_MIN_LENGTH_FOR_CONTINUATION_THRESHOLD:
                print(f"INFO Bedrock LLM Client: Output with 'end_turn' is short (length {len(full_synthesized_text)} < {LLM_MIN_LENGTH_FOR_CONTINUATION_THRESHOLD}). Attempting continuation ({continuation_attempts_done + 1}/{LLM_MAX_CONTINUATION_ATTEMPTS}).")
                
                # Thêm câu trả lời của assistant vào lịch sử
                current_messages_history.append({
                    "role": "assistant",
                    "content": [{"type": "text", "text": generated_text_this_turn}] # Chỉ phần text mới sinh ra trong lượt này
                })
                # Thêm prompt "tiếp tục" của user
                current_messages_history.append({
                    "role": "user",
                    "content": [{"type": "text", "text": "Rất tốt, hãy tiếp tục phân tích và cung cấp thêm chi tiết cho các mục còn lại hoặc các mục chưa được đầy đủ. Hãy đảm bảo bạn hoàn thành tất cả các yêu cầu trong prompt gốc và cung cấp một câu trả lời toàn diện."}]
                })
                if not use_streaming: # Thêm khoảng ngắt nếu không streaming để dễ đọc
                    full_synthesized_text += "\n\n--- (Tiếp tục phân tích) ---\n" 
                continuation_attempts_done += 1
                # Vòng lặp sẽ tiếp tục
            else:
                print(f"INFO Bedrock LLM Client: Model stopped with 'end_turn'. Output length {len(full_synthesized_text)}. Max continuation attempts or length threshold met. Finalizing.")
                break
        else: # Các stop_reason khác (ví dụ: "stop_sequence" nếu có)
            print(f"INFO Bedrock LLM Client: Model stopped with reason: '{stop_reason_this_turn}'. Finalizing.")
            break
            
    if continuation_attempts_done >= LLM_MAX_CONTINUATION_ATTEMPTS and stop_reason_this_turn == "end_turn":
        print(f"WARNING Bedrock LLM Client: Max continuation attempts ({LLM_MAX_CONTINUATION_ATTEMPTS}) reached, and model still stopped with 'end_turn'. Output might still be incomplete if shorter than desired length.")

    print(f"DEBUG Bedrock LLM Client: Final synthesized text length after all attempts: {len(full_synthesized_text)}")
    return full_synthesized_text
