# src/common_lib/service_specific_parsers.py
from .log_parser_utils import get_aware_utc_datetime
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import json # Nếu file clean là JSON

# --- Bắt đầu Processor cho ESB Service (Clean Data) ---
def process_esb_clean_logs(clean_log_content: str, esb_config: dict):
    service_name = esb_config.get("service_name", "ESB Service")
    print(f"CleanParser: Processing CLEANED ESB data for {service_name}")
    # extracted_phenomena là dict bạn sẽ build từ clean_log_content
    # Ví dụ: {"service_name": service_name, "errors_summary": ["Mô tả lỗi 1...", ...], 
    #         "metrics_summary": {"avg_response_time": 1500, "error_rate_percent": 5},
    #         "other_key_info": ["Thông tin A"]}
    # Logic cụ thể phụ thuộc vào định dạng file clean_log_content của bạn.
    
    extracted_phenomena = {
        "service_name": service_name,
        "errors_summary": [], 
        "metrics_summary": {},
        "other_key_info": [] 
    }
    try:
        # GIẢ SỬ file clean là một JSON string, chứa một dictionary hoặc list of dictionaries
        data = json.loads(clean_log_content)
        
        # --- LOGIC XỬ LÝ CỤ THỂ CỦA BẠN CHO ESB CLEAN DATA ---
        # Ví dụ:
        if isinstance(data, dict): # Nếu file clean là một JSON object tổng hợp
            if "detected_errors_list" in data and isinstance(data["detected_errors_list"], list):
                for err_item in data["detected_errors_list"][:5]: # Lấy 5 lỗi đầu
                    extracted_phenomena["errors_summary"].append(str(err_item)) 
            if "calculated_metrics_dict" in data and isinstance(data["calculated_metrics_dict"], dict):
                extracted_phenomena["metrics_summary"] = data["calculated_metrics_dict"]
            if "key_events_list" in data and isinstance(data["key_events_list"], list):
                 for event_item in data["key_events_list"][:3]:
                    extracted_phenomena["other_key_info"].append(str(event_item))
        
        elif isinstance(data, list): # Nếu file clean là một list các "sự kiện" đã trích xuất
            error_count = 0
            for item in data:
                if isinstance(item, dict) and item.get("event_type") == "ERROR": # Ví dụ
                    error_count +=1
                    if len(extracted_phenomena["errors_summary"]) < 5:
                        extracted_phenomena["errors_summary"].append(item.get("description", "Lỗi không rõ từ clean data"))
            if error_count > 0:
                 extracted_phenomena["metrics_summary"]["total_errors_from_clean_data"] = error_count
        # --- KẾT THÚC LOGIC CỤ THỂ ---
        else:
            print(f"Warning ESB CleanParser: Unexpected data type in clean log for {service_name}: {type(data)}")
            extracted_phenomena["errors_summary"].append("Dữ liệu clean không đúng định dạng JSON object/list.")

    except json.JSONDecodeError:
        print(f"ERROR ESB CleanParser: Clean data for {service_name} is not valid JSON. Content (start): {clean_log_content[:200]}")
        extracted_phenomena["errors_summary"].append("Lỗi đọc dữ liệu clean (không phải JSON).")
    except Exception as e:
        print(f"ERROR ESB CleanParser: Error processing clean data for {service_name}: {e}")
        extracted_phenomena["errors_summary"].append(f"Lỗi xử lý dữ liệu clean: {str(e)}")

    print(f"CleanParser: Finished processing CLEANED ESB data for {service_name}. Extracted: {json.dumps(extracted_phenomena, ensure_ascii=False)}")
    return extracted_phenomena

def process_apigw_clean_logs(clean_log_content: str, apigw_config: dict):
    service_name = apigw_config.get("service_name", "APIGW Service")
    print(f"CleanParser: Processing CLEANED APIGW data for {service_name} (Placeholder - Needs Implementation)")
    # TODO: Implement logic dựa trên cấu trúc file clean data của APIGW.
    # Trả về một dict tương tự như process_esb_clean_logs
    return {
        "service_name": service_name, 
        "errors_summary": ["APIGW clean data processing not fully implemented."],
        "metrics_summary": {},
        "other_key_info": []
    }

def process_roc_clean_logs(clean_log_content: str, roc_config: dict):
    service_name = roc_config.get("service_name", "ROC Service")
    print(f"CleanParser: Processing CLEANED ROC data for {service_name} (Placeholder - Needs Implementation)")
    # TODO: Implement logic tương tự
    return {
        "service_name": service_name, 
        "errors_summary": ["ROC clean data processing not fully implemented."],
        "metrics_summary": {},
        "other_key_info": []
    }

# Thêm các hàm process_SERVICE_clean_logs khác nếu cần