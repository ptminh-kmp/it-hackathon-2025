# src/common_lib/ec2_api_client.py
import requests # Thư viện này sẽ được cung cấp bởi Lambda Layer
import json
import os
import traceback

EC2_PUBLIC_IP = os.environ.get('EC2_PUBLIC_IP')
EC2_API_PORT = os.environ.get('EC2_API_PORT', '8001') # Mặc định port 8001
EC2_API_PATH = os.environ.get('EC2_API_PATH', 'predict').lstrip('/') # Mặc định path là /predict, loại bỏ / ở đầu nếu có
EC2_API_FULL_ENDPOINT = f"http://{EC2_PUBLIC_IP}:{EC2_API_PORT}/{EC2_API_PATH}"


def send_prediction_to_ec2(payload: dict):
    if not EC2_PUBLIC_IP: # Kiểm tra xem IP có được set không
        print("ERROR EC2 Client: EC2_PUBLIC_IP environment variable not set. Cannot send prediction.")
        return False
        
    headers = {'Content-Type': 'application/json; charset=utf-8'} # Thêm charset=utf-8
    
    try:
        loggable_payload_summary = {
            "source_log_file_or_batch_id": payload.get("source_log_file", payload.get("batch_id")),
            "service_name": payload.get("service_name", "Aggregated Batch"),
            "prediction_length": len(payload.get("final_llm_prediction", "")),
            "num_kb_chunks_retrieved": len(payload.get("retrieved_knowledge_chunks_summary", []))
        }
        print(f"EC2 Client: Sending prediction to EC2 API Endpoint: {EC2_API_FULL_ENDPOINT}")
        print(f"EC2 Client: Payload summary being sent: {json.dumps(loggable_payload_summary, ensure_ascii=False)}")

        # Đảm bảo payload được encode thành UTF-8 trước khi gửi
        json_payload_str = json.dumps(payload, ensure_ascii=False)
        json_payload_bytes = json_payload_str.encode('utf-8')

        response = requests.post(
            EC2_API_FULL_ENDPOINT, 
            data=json_payload_bytes, 
            headers=headers, 
            timeout=25 # Tăng timeout lên 25 giây
        )
        
        print(f"DEBUG EC2 Client: Response Status Code from EC2: {response.status_code}")
        response_text_preview = response.text[:500] if response.text else "" # Lấy 500 ký tự đầu của response
        print(f"DEBUG EC2 Client: Response Text from EC2 (first 500 chars): {response_text_preview}...")

        response.raise_for_status() # Gây exception nếu HTTP status code là lỗi (4xx hoặc 5xx)
        
        print(f"EC2 Client: Successfully sent prediction to EC2. Status Code: {response.status_code}")
        return True
        
    except requests.exceptions.Timeout:
        print(f"ERROR EC2 Client: Timeout when sending prediction to EC2 API: {EC2_API_FULL_ENDPOINT}")
        traceback.print_exc()
        return False
    except requests.exceptions.ConnectionError as conn_err:
        print(f"ERROR EC2 Client: Connection error when sending prediction to EC2 API: {EC2_API_FULL_ENDPOINT}. Error: {conn_err}")
        traceback.print_exc()
        return False
    except requests.exceptions.HTTPError as http_err:
        print(f"ERROR EC2 Client: HTTP error occurred when sending prediction to EC2 API: {http_err.response.status_code} {http_err.response.reason}")
        error_response_text = http_err.response.text[:500] if hasattr(http_err.response, 'text') and http_err.response.text else "No response body for error."
        print(f"Error response content from EC2: {error_response_text}...")
        traceback.print_exc()
        return False
    except requests.exceptions.RequestException as req_err:
        print(f"ERROR EC2 Client: General RequestException sending prediction to EC2 API: {req_err}")
        traceback.print_exc()
        return False
    except Exception as e:
        print(f"ERROR EC2 Client: An unexpected error occurred in send_prediction_to_ec2: {e}")
        traceback.print_exc()
        return False