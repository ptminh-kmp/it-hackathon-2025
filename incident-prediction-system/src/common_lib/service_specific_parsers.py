# src/common_lib/service_specific_parsers.py
import csv
from io import StringIO
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter
import traceback 
import json

from .log_parser_utils import get_aware_utc_datetime

def _evaluate_condition(item_value_str: str | None, operator: str, condition_value_from_config: any, condition_values_list_from_config: list | None = None) -> bool:
    """
    Hàm nội bộ để đánh giá một điều kiện.
    condition_values_list_from_config: Dùng cho operator 'contains_any_keyword'.
    """
    if item_value_str is None and operator not in ["is_null", "is_not_null"]: # is_null/is_not_null có thể hoạt động với None
        return False

    try:
        # Xử lý các operator không cần so sánh giá trị trước
        if operator == "is_null":
            return item_value_str is None
        if operator == "is_not_null":
            return item_value_str is not None
        
        # Đối với các operator còn lại, item_value_str không nên là None
        if item_value_str is None: return False


        if operator == "contains_any_keyword":
            if not isinstance(item_value_str, str) or not isinstance(condition_values_list_from_config, list):
                return False
            val1_lower = item_value_str.lower()
            for keyword in condition_values_list_from_config:
                if str(keyword).lower() in val1_lower:
                    return True
            return False
        
        # Các so sánh số học hoặc chuỗi
        if isinstance(condition_value_from_config, (int, float)):
            val1 = float(item_value_str)
            val2 = float(condition_value_from_config)
        else: 
            val1 = str(item_value_str)
            val2 = str(condition_value_from_config)

        if operator == "<" and val1 < val2: return True
        if operator == "<=" and val1 <= val2: return True
        if operator == ">" and val1 > val2: return True
        if operator == ">=" and val1 >= val2: return True
        if operator == "==" and val1 == val2: return True
        if operator == "!=" and val1 != val2: return True
        # 'contains' cho một keyword đơn lẻ (khác với contains_any_keyword)
        if operator == "contains" and isinstance(val1, str) and isinstance(val2, str) and val2.lower() in val1.lower(): return True
        
    except (ValueError, TypeError) as e:
        # print(f"DEBUG _evaluate_condition: Error comparing '{item_value_str}' and '{condition_value_from_config}' with op '{operator}'. Err: {e}")
        return False
    return False


def _derive_metrics_from_parsed_rows(
    parsed_rows_with_standard_fields: list[dict], 
    metric_configs: list, 
    service_name_for_log: str,
    global_error_condition: dict | None = None # Thêm điều kiện lỗi chung của service
):
    """
    Hàm nội bộ để tính toán các "metric".
    global_error_condition: Dùng cho metric loại "count_if_condition_met" nếu nó không tự định nghĩa condition.
    """
    derived_metrics = {}
    if not isinstance(parsed_rows_with_standard_fields, list):
        print(f"MetricDeriver: Input for {service_name_for_log} not a list.")
        return derived_metrics

    for m_cfg in metric_configs:
        metric_name = m_cfg.get("name")
        metric_type = m_cfg.get("type")
        current_metric_value = None

        # Lấy điều kiện từ metric_cfg, nếu không có thì dùng global_error_condition (cho count_if_condition_met)
        condition_field_standard = m_cfg.get("condition_field")
        condition_op = m_cfg.get("condition_operator")
        condition_val_config = m_cfg.get("condition_value")
        condition_values_list_config = m_cfg.get("condition_values_list") # Cho contains_any_keyword

        # Nếu metric là đếm lỗi chung và không có condition riêng, dùng global_error_condition
        if metric_type == "count_if_condition_met" and not condition_field_standard and global_error_condition:
            condition_field_standard = global_error_condition.get("field_to_check") # Đây là tên trường chuẩn
            condition_op = global_error_condition.get("operator")
            condition_val_config = global_error_condition.get("value")
            condition_values_list_config = global_error_condition.get("keywords") # Nếu global error là keyword based

        if metric_type == "total_row_count":
            current_metric_value = len(parsed_rows_with_standard_fields)

        elif metric_type == "count_if_condition_met":
            if not all([condition_field_standard, condition_op]) or (condition_val_config is None and condition_values_list_config is None and condition_op not in ["is_null", "is_not_null"]):
                print(f"MetricDeriver: Misconfig for metric '{metric_name}' (count_if_condition_met) in {service_name_for_log}.")
                continue
            
            count = 0
            for item_dict in parsed_rows_with_standard_fields:
                item_value_str = item_dict.get(condition_field_standard)
                if _evaluate_condition(item_value_str, condition_op, condition_val_config, condition_values_list_config):
                    count += 1
            current_metric_value = count

        elif metric_type == "average_of_field":
            target_field_standard = m_cfg.get("target_field")
            if not target_field_standard: continue
            values_for_avg = []
            for item_dict in parsed_rows_with_standard_fields:
                val_str = item_dict.get(target_field_standard)
                if val_str is not None:
                    try: values_for_avg.append(float(val_str))
                    except (ValueError, TypeError): pass
            if values_for_avg: current_metric_value = round(sum(values_for_avg) / len(values_for_avg), 2)
            else: current_metric_value = 0.0
            
        elif metric_type == "count_by_field_value":
            count_field_standard = m_cfg.get("count_field")
            if not count_field_standard: continue
            value_counts = Counter()
            for item_dict in parsed_rows_with_standard_fields:
                value_to_count = str(item_dict.get(count_field_standard, "N/A_FIELD_VALUE"))
                value_counts[value_to_count] += 1
            current_metric_value = dict(value_counts)

        # (Giữ lại count_by_field_value_if_condition_met nếu cần)

        if current_metric_value is not None:
            derived_metrics[metric_name] = current_metric_value
            
    return derived_metrics


def process_esb_logs(csv_log_content: str, esb_config: dict, processing_time_utc: datetime | None = None):
    # ... (Hàm process_esb_logs đã được cập nhật ở câu trả lời trước để xử lý CSV và
    #      giả định tất cả dòng là lỗi, sử dụng _derive_metrics_from_parsed_rows
    #      với type "total_row_count" và "count_by_field_value".
    #      Hãy đảm bảo nó sử dụng _derive_metrics_from_parsed_rows phiên bản mới nhất ở trên
    #      và ánh xạ đúng tên cột CSV sang tên trường chuẩn trước khi gọi _derive_metrics_from_parsed_rows)

    # Dưới đây là phiên bản cập nhật cho ESB (nếu file CSV chỉ chứa lỗi)
    service_name = esb_config.get("service_name", "ESB Service (Error Logs Only)")
    print(f"ESB CSV Parser (Assumed Error Logs Only): Processing for service: '{service_name}'")
    
    extracted_phenomena = {
        "service_name": service_name, "errors_summary": [], 
        "metrics_summary": {}, "other_key_info": []  
    }

    csv_opts = esb_config.get("csv_processing_options", {})
    col_mapping = esb_config.get("csv_column_to_field_mapping", {})
    message_std_field = "log_message" # Tên trường chuẩn cho message
    operation_std_field = "operation_name" # Tên trường chuẩn cho operation
    
    critical_keywords = esb_config.get("critical_keywords_in_message_field", [])
    cfg_metric_definitions = esb_config.get("metrics_to_derive_from_csv", [])
    cfg_max_err_examples = esb_config.get("summary_options", {}).get("max_error_event_examples", 10)
    cfg_max_crit_info = esb_config.get("summary_options", {}).get("max_critical_info_from_keywords", 5)

    parsed_rows_with_standard_fields = []
    try:
        corrected_log_content = csv_log_content.replace('“', '"').replace('”', '"')
        csvfile = StringIO(corrected_log_content)
        reader = csv.DictReader(csvfile, delimiter=csv_opts.get("delimiter", ","), quotechar=csv_opts.get("quotechar", "\""))

        for csv_row_dict in reader:
            standard_field_row = {std_name: csv_row_dict.get(csv_col) for std_name, csv_col in col_mapping.items()}
            parsed_rows_with_standard_fields.append(standard_field_row)

            # Vì tất cả dòng đều là lỗi, lấy ví dụ từ đây
            if len(extracted_phenomena["errors_summary"]) < cfg_max_err_examples:
                err_msg = str(standard_field_row.get(message_std_field, "N/A"))
                op_name = str(standard_field_row.get(operation_std_field, "N/A"))
                summary = f"ESB Error - Op: '{op_name}', Msg: {err_msg[:150]}" + ("..." if len(err_msg) > 150 else "")
                extracted_phenomena["errors_summary"].append(summary)

            # Kiểm tra critical keywords
            msg_content_std = str(standard_field_row.get(message_std_field, "")).lower()
            for keyword in critical_keywords:
                if keyword.lower() in msg_content_std:
                    if len(extracted_phenomena["other_key_info"]) < cfg_max_crit_info:
                        ts_std_field = "timestamp" # Tên trường chuẩn cho timestamp
                        ts_orig_str = standard_field_row.get(ts_std_field, "")
                        info_summary = f"Keyword '{keyword}' at {ts_orig_str} in msg: {msg_content_std[:120]}" + ("..." if len(msg_content_std) > 120 else "")
                        extracted_phenomena["other_key_info"].append(info_summary)
                    break 
        
        if not parsed_rows_with_standard_fields:
            print(f"Warning ESB Parser: CSV for {service_name} is empty or no data rows.")
        else:
            print(f"DEBUG ESB Parser: Parsed {len(parsed_rows_with_standard_fields)} rows for {service_name}.")

    except Exception as e_csv:
        print(f"ERROR ESB Parser: Could not parse CSV for {service_name}: {e_csv}")
        traceback.print_exc()
        extracted_phenomena["errors_summary"].append(f"Lỗi đọc CSV: {str(e_csv)}")
        return extracted_phenomena
    
    if parsed_rows_with_standard_fields:
        # Ánh xạ tên trường trong metric_configs sang tên trường chuẩn
        updated_metric_definitions = []
        for m_cfg_orig in cfg_metric_definitions:
            m_cfg_new = m_cfg_orig.copy()
            for std_f_map, csv_f_in_map in col_mapping.items():
                if m_cfg_new.get("condition_field") == csv_f_in_map: m_cfg_new["condition_field"] = std_f_map
                if m_cfg_new.get("target_field") == csv_f_in_map: m_cfg_new["target_field"] = std_f_map
                if m_cfg_new.get("count_field") == csv_f_in_map: m_cfg_new["count_field"] = std_f_map
            updated_metric_definitions.append(m_cfg_new)
            
        extracted_phenomena["metrics_summary"] = _derive_metrics_from_parsed_rows(
            parsed_rows_with_standard_fields, 
            updated_metric_definitions, 
            service_name
        )
    else:
        extracted_phenomena["metrics_summary"] = {}

    print(f"ESB Parser: Finished for '{service_name}'. Extracted: {json.dumps(extracted_phenomena, ensure_ascii=False)}")
    return extracted_phenomena


def process_apigw_logs(csv_log_content: str, apigw_config: dict, processing_time_utc: datetime | None = None):
    service_name = apigw_config.get("service_name", "APIGW Service")
    print(f"APIGW CSV Parser: Processing CSV data for service: '{service_name}'")
    
    extracted_phenomena = {
        "service_name": service_name, "errors_summary": [], 
        "metrics_summary": {}, "other_key_info": []  
    }

    csv_opts = apigw_config.get("csv_processing_options", {})
    col_mapping = apigw_config.get("csv_column_to_field_mapping", {})
    
    # Lấy cấu hình điều kiện lỗi và các trường liên quan (tên trường chuẩn sau khi map)
    error_condition_cfg = apigw_config.get("error_condition", {})
    err_cond_field_std = None # Tên trường chuẩn để check lỗi
    err_cond_col_name = error_condition_cfg.get("field_to_check") # Tên cột CSV gốc
    for std_n, csv_n in col_mapping.items():
        if csv_n == err_cond_col_name:
            err_cond_field_std = std_n
            break
    err_cond_op = error_condition_cfg.get("operator")
    err_cond_val = error_condition_cfg.get("value") # Dùng cho operator so sánh giá trị
    err_cond_keywords = error_condition_cfg.get("keywords") # Dùng cho operator contains_any_keyword

    message_std_field = "log_content_detail" # Tên trường chuẩn cho message/content của APIGW
    critical_keywords = apigw_config.get("critical_keywords_in_message_field", []) # Hoặc _in_content_field

    cfg_metric_definitions = apigw_config.get("metrics_to_derive_from_csv", [])
    cfg_max_err_examples = apigw_config.get("summary_options", {}).get("max_error_event_examples", 5)
    cfg_max_crit_info = apigw_config.get("summary_options", {}).get("max_critical_info_from_keywords", 3)

    parsed_rows_with_standard_fields = []
    try:
        corrected_log_content = csv_log_content.replace('“', '"').replace('”', '"')
        csvfile = StringIO(corrected_log_content)
        reader = csv.DictReader(csvfile, delimiter=csv_opts.get("delimiter", ","), quotechar=csv_opts.get("quotechar", "\""))

        for csv_row_dict in reader:
            standard_field_row = {std_name: csv_row_dict.get(csv_col) for std_name, csv_col in col_mapping.items()}
            parsed_rows_with_standard_fields.append(standard_field_row)

            is_error = False
            if err_cond_field_std and err_cond_op: # Nếu có cấu hình điều kiện lỗi
                value_to_check_str = standard_field_row.get(err_cond_field_std)
                if _evaluate_condition(value_to_check_str, err_cond_op, err_cond_val, err_cond_keywords):
                    is_error = True
            
            if is_error:
                if len(extracted_phenomena["errors_summary"]) < cfg_max_err_examples:
                    err_msg = str(standard_field_row.get(message_std_field, "N/A"))
                    handler_func = str(standard_field_row.get("handler_function", "N/A"))
                    summary = f"APIGW Error - Handler: '{handler_func}', Detail: {err_msg[:150]}" + ("..." if len(err_msg) > 150 else "")
                    extracted_phenomena["errors_summary"].append(summary)
            
            # Kiểm tra critical keywords (nếu có cấu hình riêng, hoặc có thể dựa vào is_error ở trên)
            msg_content_std = str(standard_field_row.get(message_std_field, "")).lower()
            for keyword in critical_keywords: # critical_keywords_in_message_field từ config
                if keyword.lower() in msg_content_std:
                    if len(extracted_phenomena["other_key_info"]) < cfg_max_crit_info:
                        # ... (thêm vào other_key_info)
                        pass # Tạm thời pass
                    break
        
        if not parsed_rows_with_standard_fields:
            print(f"Warning APIGW Parser: CSV for {service_name} is empty.")
        else:
            print(f"DEBUG APIGW Parser: Parsed {len(parsed_rows_with_standard_fields)} rows for {service_name}.")

    except Exception as e_csv: # ... (xử lý lỗi đọc CSV như ESB)
        print(f"ERROR APIGW Parser: Could not parse CSV for {service_name}: {e_csv}")
        traceback.print_exc()
        extracted_phenomena["errors_summary"].append(f"Lỗi đọc CSV APIGW: {str(e_csv)}")
        return extracted_phenomena

    if parsed_rows_with_standard_fields:
        updated_metric_definitions_apigw = []
        for m_cfg_orig in cfg_metric_definitions:
            m_cfg_new = m_cfg_orig.copy()
            # Ánh xạ tên trường trong metric_configs sang tên trường chuẩn
            for std_f_map, csv_f_in_map in col_mapping.items():
                if m_cfg_new.get("condition_field") == csv_f_in_map: m_cfg_new["condition_field"] = std_f_map
                if m_cfg_new.get("target_field") == csv_f_in_map: m_cfg_new["target_field"] = std_f_map
                if m_cfg_new.get("count_field") == csv_f_in_map: m_cfg_new["count_field"] = std_f_map
            updated_metric_definitions_apigw.append(m_cfg_new)

        extracted_phenomena["metrics_summary"] = _derive_metrics_from_parsed_rows(
            parsed_rows_with_standard_fields, 
            updated_metric_definitions_apigw, 
            service_name,
            # Truyền global_error_condition đã được map sang tên trường chuẩn nếu cần
            # global_error_condition={
            #    "field_to_check": err_cond_field_std, # Tên trường chuẩn
            #    "operator": err_cond_op,
            #    "value": err_cond_val,
            #    "keywords": err_cond_keywords
            # } if err_cond_field_std else None
        )
    else:
        extracted_phenomena["metrics_summary"] = {}

    print(f"APIGW CSV Parser: Finished for '{service_name}'. Extracted: {json.dumps(extracted_phenomena, ensure_ascii=False)}")
    return extracted_phenomena


def process_roc_clean_logs(clean_log_content: str, roc_config: dict):
    # Tương tự, implement logic đọc CSV cho ROC nếu file clean của ROC là CSV
    service_name = roc_config.get("service_name", "ROC Service")
    print(f"CleanParser: Processing CLEANED ROC data for {service_name} (Needs CSV Implementation if applicable)")
    extracted_phenomena = {
        "service_name": service_name, 
        "errors_summary": ["ROC clean data processing logic (CSV) needs to be implemented."],
        "metrics_summary": {},
        "other_key_info": []
    }
    # TODO: Implement
    return extracted_phenomena
