# src/common_lib/service_specific_parsers.py
import csv
from io import StringIO
from datetime import datetime, timezone, timedelta # Đảm bảo timezone, timedelta được import
from collections import defaultdict, Counter # Thêm Counter
import traceback 
import json
import re # Thêm re cho keyword search

# Import hàm tiện ích từ log_parser_utils.py
# Đảm bảo file log_parser_utils.py có hàm get_aware_utc_datetime như đã cung cấp.
from .log_parser_utils import get_aware_utc_datetime

def _evaluate_condition(item_value: any, operator: str, condition_value_from_config: any, condition_values_list_from_config: list | None = None) -> bool:
    """
    Hàm nội bộ để đánh giá một điều kiện.
    item_value: Giá trị thực tế từ dữ liệu (đã được ép kiểu nếu có thể trong logic gọi).
    condition_values_list_from_config: Dùng cho operator 'in' hoặc 'contains_any_keyword'.
    """
    if item_value is None and operator not in ["is_null", "is_not_null"]:
        return False # Không thể thực hiện hầu hết các so sánh nếu item_value là None
    try:
        if operator == "is_null": return item_value is None
        if operator == "is_not_null": return item_value is not None
        
        # Đối với các operator còn lại, item_value không nên là None (trừ khi đã được xử lý ở trên)
        if item_value is None: return False # Nếu đến đây mà item_value vẫn None thì điều kiện (khác is_null/is_not_null) là False

        if operator == "in":
            if not isinstance(condition_values_list_from_config, list): return False
            # Chuyển item_value và các giá trị trong list sang cùng kiểu (string) để so sánh an toàn
            return str(item_value) in [str(v) for v in condition_values_list_from_config]

        if operator == "contains_any_keyword":
            if not isinstance(item_value, str) or not isinstance(condition_values_list_from_config, list): return False
            val1_lower = item_value.lower()
            for keyword in condition_values_list_from_config:
                if str(keyword).lower() in val1_lower: return True
            return False
        
        # Xử lý so sánh số học và chuỗi cho các operator khác
        val1_to_compare = item_value
        val2_to_compare = condition_value_from_config

        # Cố gắng ép kiểu thông minh hơn cho so sánh số học
        if isinstance(condition_value_from_config, (int, float)):
            try: 
                val1_to_compare = float(item_value) # Luôn thử float để bao gồm cả int
                val2_to_compare = float(condition_value_from_config)
            except (ValueError, TypeError): # Nếu item_value không phải số, so sánh như chuỗi
                val1_to_compare = str(item_value)
                val2_to_compare = str(condition_value_from_config)
        elif isinstance(item_value, (int,float)) and isinstance(condition_value_from_config, str) :
             try: # Thử ép config_value thành kiểu của item_value
                val2_to_compare = type(item_value)(condition_value_from_config)
             except (ValueError, TypeError): # Nếu không được, so sánh chuỗi
                val1_to_compare = str(item_value)
                val2_to_compare = str(condition_value_from_config)
        # Nếu kiểu đã giống nhau hoặc cả hai không phải số (và không phải trường hợp trên), so sánh trực tiếp hoặc như chuỗi
        elif not (isinstance(item_value, (int,float)) and isinstance(condition_value_from_config, (int,float))):
             # Nếu không phải cả hai đều là số, đảm bảo so sánh chuỗi
            if not isinstance(val1_to_compare, type(val2_to_compare)):
                val1_to_compare = str(item_value)
                val2_to_compare = str(condition_value_from_config)


        if operator == "<" and val1_to_compare < val2_to_compare: return True
        if operator == "<=" and val1_to_compare <= val2_to_compare: return True
        if operator == ">" and val1_to_compare > val2_to_compare: return True
        if operator == ">=" and val1_to_compare >= val2_to_compare: return True
        if operator == "==" and val1_to_compare == val2_to_compare: return True
        if operator == "!=" and val1_to_compare != val2_to_compare: return True
        if operator == "contains" and isinstance(val1_to_compare, str) and isinstance(val2_to_compare, str) and val2_to_compare.lower() in val1_to_compare.lower(): return True
        
    except (ValueError, TypeError) as e:
        # print(f"DEBUG _evaluate_condition: Error comparing '{item_value}' and '{condition_value_from_config}' with op '{operator}'. Err: {e}")
        return False
    return False


def _derive_metrics_from_parsed_data(
    list_of_parsed_data_dicts: list[dict], 
    metric_configs: list, 
    service_name_for_log: str
):
    """
    Hàm nội bộ để tính toán các "metric" từ list các dict (mỗi dict là một dòng log đã parse 
    với tên trường chuẩn, hoặc một "keyword match event").
    """
    derived_metrics = {}
    if not isinstance(list_of_parsed_data_dicts, list):
        print(f"MetricDeriver: Input data for {service_name_for_log} is not a list. Skipping metric derivation.")
        return derived_metrics

    for m_cfg in metric_configs:
        metric_name = m_cfg.get("name")
        metric_type = m_cfg.get("type")
        current_metric_value = None # Khởi tạo ở đây

        condition_field_standard = m_cfg.get("condition_field") 
        condition_op = m_cfg.get("condition_operator")
        condition_val_config = m_cfg.get("condition_value")
        condition_values_list_config = m_cfg.get("condition_values_list")
        
        target_field_standard = m_cfg.get("target_field") 
        count_field_standard = m_cfg.get("count_field") 

        if metric_type == "total_row_count": 
             current_metric_value = len(list_of_parsed_data_dicts)
        
        elif metric_type == "count_if_condition_met" or metric_type == "count_if_multiple_conditions_met":
            conditions_to_check = m_cfg.get("conditions") if metric_type == "count_if_multiple_conditions_met" else \
                                  [{"condition_field": condition_field_standard, 
                                    "condition_operator": condition_op, 
                                    "condition_value": condition_val_config,
                                    "condition_values_list": condition_values_list_config
                                   }] if condition_field_standard and condition_op else [] # Cần cả field và op

            if not conditions_to_check: # Nếu không có conditions nào được định nghĩa đúng
                print(f"MetricDeriver: No valid conditions defined for metric '{metric_name}' (type: {metric_type}) in {service_name_for_log}.")
                continue
            
            count = 0
            for item_dict in list_of_parsed_data_dicts:
                if not isinstance(item_dict, dict): continue 
                all_conditions_met_for_item = True
                for cond_idx, cond in enumerate(conditions_to_check):
                    cond_field = cond.get("condition_field")
                    cond_op = cond.get("condition_operator")
                    cond_val = cond.get("condition_value")
                    cond_list_val = cond.get("condition_values_list")

                    if not (cond_field and cond_op) or (cond_val is None and cond_list_val is None and cond_op not in ["is_null", "is_not_null"]):
                        print(f"MetricDeriver: Misconfig for sub-condition {cond_idx} in metric '{metric_name}' for {service_name_for_log} (missing field/op or value for non-null ops).")
                        all_conditions_met_for_item = False; break
                    
                    item_value_from_dict = item_dict.get(cond_field) 
                    if not _evaluate_condition(item_value_from_dict, cond_op, cond_val, cond_list_val):
                        all_conditions_met_for_item = False; break
                
                if all_conditions_met_for_item:
                    count += 1
            current_metric_value = count

        elif metric_type == "average_of_field":
            if not target_field_standard: 
                print(f"MetricDeriver: Misconfig for metric '{metric_name}' (average_of_field) - missing target_field in {service_name_for_log}.")
                continue
            values_for_avg = []
            for item_dict in list_of_parsed_data_dicts:
                if not isinstance(item_dict, dict): continue
                val_from_item = item_dict.get(target_field_standard)
                if val_from_item is not None:
                    try: values_for_avg.append(float(val_from_item))
                    except (ValueError, TypeError): pass
            if values_for_avg: current_metric_value = round(sum(values_for_avg) / len(values_for_avg), 2)
            else: current_metric_value = 0.0 # Hoặc None nếu muốn thể hiện không có dữ liệu
            
        elif metric_type == "count_by_field_value":
            if not count_field_standard:
                print(f"MetricDeriver: Misconfig for metric '{metric_name}' (count_by_field_value) - missing count_field in {service_name_for_log}.")
                continue
            value_counts = Counter()
            for item_dict in list_of_parsed_data_dicts:
                if not isinstance(item_dict, dict): continue
                value_to_count = str(item_dict.get(count_field_standard, "N/A_FIELD_VALUE"))
                value_counts[value_to_count] += 1
            current_metric_value = dict(value_counts)
        
        elif metric_type == "sum_of_keyword_counts": 
            total_keywords = 0
            for event_dict in list_of_parsed_data_dicts:
                if isinstance(event_dict, dict) and event_dict.get("type") == "keyword_match":
                     total_keywords += 1 
            current_metric_value = total_keywords
            
        elif metric_type == "group_keyword_counts_by_category": 
            category_counts = Counter()
            for event_dict in list_of_parsed_data_dicts:
                if isinstance(event_dict, dict) and event_dict.get("type") == "keyword_match":
                    category = event_dict.get("category", "unknown_category")
                    category_counts[category] += 1
            current_metric_value = dict(category_counts)
        
        else:
            print(f"Warning MetricDeriver: Unsupported metric type '{metric_type}' for metric '{metric_name}' in {service_name_for_log}.")
            continue

        if current_metric_value is not None:
            derived_metrics[metric_name] = current_metric_value
            
    return derived_metrics


# --- Processor cho ESB Service (File CSV chỉ chứa lỗi từ Jupyter) ---
def process_esb_clean_logs(csv_log_content: str, esb_config: dict, processing_time_utc: datetime | None = None):
    service_name = esb_config.get("service_name", "ESB Service (Error Logs Only)")
    print(f"ESB CSV Parser (Assumed Cleaned Error Logs Only): Processing for service: '{service_name}'")
    
    extracted_phenomena = {
        "service_name": service_name, "errors_summary": [], 
        "metrics_summary": {}, "other_key_info": []  
    }

    csv_opts = esb_config.get("csv_processing_options", {})
    col_mapping = esb_config.get("csv_column_to_field_mapping", {})
    
    # Lấy tên trường chuẩn cho message và operation từ mapping
    # Nếu không có trong mapping, sẽ không thể trích xuất đúng
    std_message_field = None
    std_operation_field = None
    std_timestamp_field = None
    for std_f, csv_f in col_mapping.items():
        if std_f == esb_config.get("clean_log_fields_to_extract", {}).get("message_field", "log_message"): # Tên chuẩn
            std_message_field = std_f
        if std_f == esb_config.get("clean_log_fields_to_extract", {}).get("operation_field", "operation_name"):
            std_operation_field = std_f
        if std_f == esb_config.get("timestamp_processing", {}).get("source_field", "timestamp"):
             std_timestamp_field = std_f


    critical_keywords = esb_config.get("critical_keywords_in_message_field", []) # Đổi tên cho khớp config
    cfg_metric_definitions = esb_config.get("metrics_to_derive_from_csv", [])
    cfg_max_err_examples = esb_config.get("summary_options", {}).get("max_error_event_examples", 10)
    cfg_max_crit_info = esb_config.get("summary_options", {}).get("max_critical_info_from_keywords", 5)

    parsed_rows_with_standard_fields = [] # List để chứa các dict với tên trường đã chuẩn hóa
    
    try:
        # Tiền xử lý thay thế smart quotes (nếu có)
        corrected_log_content = csv_log_content.replace('“', '"').replace('”', '"')
        csvfile = StringIO(corrected_log_content)
        
        # csv.DictReader sử dụng dòng đầu tiên làm header
        reader = csv.DictReader(csvfile, delimiter=csv_opts.get("delimiter", ","), quotechar=csv_opts.get("quotechar", "\""))

        for line_number, csv_row_dict in enumerate(reader):
            standard_field_row = {}
            # Ánh xạ từ tên cột CSV gốc sang tên trường chuẩn
            for std_name_in_map, csv_col_name_in_map in col_mapping.items():
                standard_field_row[std_name_in_map] = csv_row_dict.get(csv_col_name_in_map)
            
            # (Tùy chọn) Parse timestamp nếu cần cho logic sau này (ví dụ: hiển thị)
            # Hiện tại, metrics của ESB (total_row_count, count_by_field_value) không cần timestamp chi tiết
            if std_timestamp_field:
                ts_str_val = standard_field_row.get(std_timestamp_field)
                ts_format = esb_config.get("timestamp_processing",{}).get("input_format")
                if ts_str_val and ts_format:
                    standard_field_row["_datetime_obj_utc"] = get_aware_utc_datetime(ts_str_val, ts_format)

            parsed_rows_with_standard_fields.append(standard_field_row)

            # Vì file CSV này được giả định chỉ chứa lỗi, nên mọi dòng đều là "error event"
            if len(extracted_phenomena["errors_summary"]) < cfg_max_err_examples:
                err_msg = str(standard_field_row.get(std_message_field, "N/A_MSG"))
                op_name = str(standard_field_row.get(std_operation_field, "N/A_OP"))
                summary = f"ESB Error - Op: '{op_name}', Msg: {err_msg[:150]}" + ("..." if len(err_msg) > 150 else "")
                extracted_phenomena["errors_summary"].append(summary)

            # Kiểm tra critical keywords trong message (đã được map sang tên trường chuẩn)
            if std_message_field:
                msg_content_std = str(standard_field_row.get(std_message_field, "")).lower()
                for keyword in critical_keywords:
                    if keyword.lower() in msg_content_std:
                        if len(extracted_phenomena["other_key_info"]) < cfg_max_crit_info:
                            ts_orig_str = standard_field_row.get(std_timestamp_field, "") # Lấy timestamp đã map
                            info_summary = f"Keyword '{keyword}' at {ts_orig_str} in msg: {msg_content_std[:120]}" + ("..." if len(msg_content_std) > 120 else "")
                            extracted_phenomena["other_key_info"].append(info_summary)
                        break # Chỉ cần 1 keyword/dòng
        
        if not parsed_rows_with_standard_fields:
            print(f"Warning ESB Parser: CSV for {service_name} is empty or no data rows after header.")
        else:
            print(f"DEBUG ESB Parser: Parsed {len(parsed_rows_with_standard_fields)} (assumed error) log data rows from CSV for {service_name}.")

    except Exception as e_csv:
        print(f"ERROR ESB Parser: Could not parse CSV content for {service_name}: {e_csv}")
        traceback.print_exc()
        extracted_phenomena["errors_summary"].append(f"Lỗi đọc hoặc xử lý CSV của ESB: {str(e_csv)}")
        return extracted_phenomena # Trả về sớm nếu lỗi nghiêm trọng khi đọc CSV
    
    # Tính toán các metrics tổng hợp từ TOÀN BỘ parsed_rows_with_standard_fields
    if parsed_rows_with_standard_fields:
        # Trong file config, các trường của metric (condition_field, count_field, target_field)
        # NÊN được định nghĩa bằng TÊN TRƯỜNG CHUẨN (keys của col_mapping)
        # Nếu chúng được định nghĩa bằng tên cột CSV gốc, bạn cần logic map ở đây như trước.
        # Giả sử metric_definitions đã dùng tên trường chuẩn.
        extracted_phenomena["metrics_summary"] = _derive_metrics_from_parsed_data(
            parsed_rows_with_standard_fields, 
            cfg_metric_definitions, 
            service_name
        )
    else:
        extracted_phenomena["metrics_summary"] = {}

    print(f"ESB CSV Parser (Error Logs Only): Finished processing for '{service_name}'. Extracted: {json.dumps(extracted_phenomena, ensure_ascii=False)}")
    return extracted_phenomena


# --- Processor cho APIGW Service (File CSV, cần xác định lỗi từ config) ---
def process_apigw_clean_logs(csv_log_content: str, apigw_config: dict, processing_time_utc: datetime | None = None):
    service_name = apigw_config.get("service_name", "APIGW Service")
    print(f"APIGW CSV Parser: Processing CSV data for service: '{service_name}'")
    
    extracted_phenomena = {
        "service_name": service_name, "errors_summary": [], 
        "metrics_summary": {}, "other_key_info": []  
    }

    csv_opts = apigw_config.get("csv_processing_options", {})
    col_mapping = apigw_config.get("csv_column_to_field_mapping", {})
    
    error_condition_cfg = apigw_config.get("error_condition", {})
    # Lấy tên trường chuẩn sau khi map từ tên cột CSV gốc trong error_condition_cfg
    err_cond_col_name_csv = error_condition_cfg.get("field_to_check")
    err_cond_field_std = None 
    for std_n, csv_n in col_mapping.items(): # Tìm tên trường chuẩn tương ứng
        if csv_n == err_cond_col_name_csv:
            err_cond_field_std = std_n
            break
    err_cond_op = error_condition_cfg.get("operator")
    err_cond_val_config = error_condition_cfg.get("value") # Dùng cho operator so sánh giá trị
    err_cond_keywords_list = error_condition_cfg.get("keywords") # Dùng cho operator contains_any_keyword

    # Lấy tên trường chuẩn cho message/content và handler
    std_message_field = next((std_f for std_f, csv_f in col_mapping.items() if csv_f == apigw_config.get("clean_log_fields_to_extract", {}).get("content_field")), "log_content_detail")
    std_handler_field = next((std_f for std_f, csv_f in col_mapping.items() if csv_f == apigw_config.get("clean_log_fields_to_extract", {}).get("handler_function_field")), "handler_function")
    std_timestamp_field = next((std_f for std_f, csv_f in col_mapping.items() if csv_f == apigw_config.get("timestamp_processing", {}).get("source_field_in_csv")), "timestamp_log")


    critical_keywords_cfg = apigw_config.get("critical_keywords_in_message_field", []) 
    cfg_metric_definitions = apigw_config.get("metrics_to_derive_from_csv", [])
    cfg_max_err_examples = apigw_config.get("summary_options", {}).get("max_error_event_examples", 5)
    cfg_max_crit_info = apigw_config.get("summary_options", {}).get("max_critical_info_from_keywords", 3)

    parsed_rows_with_standard_fields = []
    error_rows_for_metrics = [] # Chỉ chứa các dòng được xác định là lỗi (standard_field_row)
    try:
        corrected_log_content = csv_log_content.replace('“', '"').replace('”', '"')
        csvfile = StringIO(corrected_log_content)
        reader = csv.DictReader(csvfile, delimiter=csv_opts.get("delimiter", ","), quotechar=csv_opts.get("quotechar", "\""))

        for csv_row_dict in reader:
            standard_field_row = {std_name: csv_row_dict.get(csv_col) for std_name, csv_col in col_mapping.items()}
            
            # Parse timestamp nếu có
            ts_str_val_apigw = standard_field_row.get(std_timestamp_field)
            ts_format_apigw = apigw_config.get("timestamp_processing",{}).get("input_format")
            if ts_str_val_apigw and ts_format_apigw:
                standard_field_row["_datetime_obj_utc"] = get_aware_utc_datetime(ts_str_val_apigw, ts_format_apigw)

            parsed_rows_with_standard_fields.append(standard_field_row)

            is_error = False
            if err_cond_field_std and err_cond_op: # Nếu có cấu hình điều kiện lỗi
                value_to_check_from_std_field = standard_field_row.get(err_cond_field_std)
                if _evaluate_condition(value_to_check_from_std_field, err_cond_op, err_cond_val_config, err_cond_keywords_list):
                    is_error = True
            
            if is_error:
                error_rows_for_metrics.append(standard_field_row) # Thêm vào list lỗi để tính metric lỗi
                if len(extracted_phenomena["errors_summary"]) < cfg_max_err_examples:
                    err_msg = str(standard_field_row.get(std_message_field, "N/A"))
                    handler_func = str(standard_field_row.get(std_handler_field, "N/A"))
                    summary = f"APIGW Error - Handler: '{handler_func}', Detail: {err_msg[:150]}" + ("..." if len(err_msg) > 150 else "")
                    extracted_phenomena["errors_summary"].append(summary)
            
            # Kiểm tra critical keywords (nếu có cấu hình riêng)
            if std_message_field : # Chỉ check nếu có trường message chuẩn
                msg_content_std = str(standard_field_row.get(std_message_field, "")).lower()
                for keyword in critical_keywords_cfg:
                    if keyword.lower() in msg_content_std:
                        if len(extracted_phenomena["other_key_info"]) < cfg_max_crit_info:
                            info_summary = f"APIGW Keyword '{keyword}': {msg_content_std[:100]}" + ("..." if len(msg_content_std) > 100 else "")
                            extracted_phenomena["other_key_info"].append(info_summary)
                        break 
        
        if not parsed_rows_with_standard_fields:
            print(f"Warning APIGW Parser: CSV for {service_name} is empty.")
        else:
            print(f"DEBUG APIGW Parser: Parsed {len(parsed_rows_with_standard_fields)} total rows for {service_name}. Found {len(error_rows_for_metrics)} error rows based on condition.")

    except Exception as e_csv:
        print(f"ERROR APIGW Parser: Could not parse CSV for {service_name}: {e_csv}")
        traceback.print_exc()
        extracted_phenomena["errors_summary"].append(f"Lỗi đọc CSV APIGW: {str(e_csv)}")
        return extracted_phenomena

    if parsed_rows_with_standard_fields:
        # Tạo global_error_condition đã được map sang tên trường chuẩn (nếu error_condition được định nghĩa)
        global_err_cond_for_metrics_apigw = None
        if err_cond_field_std and err_cond_op:
            global_err_cond_for_metrics_apigw = {
                "condition_field": err_cond_field_std, # Tên trường chuẩn
                "condition_operator": err_cond_op,
                "condition_value": err_cond_val_config, 
                "condition_values_list": err_cond_keywords_list 
            }
        
        # Metric config nên dùng tên trường chuẩn
        extracted_phenomena["metrics_summary"] = _derive_metrics_from_parsed_data(
            parsed_rows_with_standard_fields, # Truyền tất cả các dòng
            cfg_metric_definitions, # Metric definitions đã dùng tên trường chuẩn
            service_name
            # global_error_condition có thể không cần nếu metric "count_if_condition_met"
            # cho "total_error_lines" đã tự định nghĩa điều kiện giống error_condition của service.
            # Nếu metric "apigw_total_error_lines_in_file" không có "condition_field" riêng,
            # thì global_error_condition này sẽ được dùng (nếu _derive_metrics hỗ trợ).
            # Hiện tại _derive_metrics_from_parsed_data yêu cầu metric "count_if_condition_met"
            # phải tự định nghĩa condition của nó.
        )
    else:
        extracted_phenomena["metrics_summary"] = {}

    print(f"APIGW CSV Parser: Finished for '{service_name}'. Extracted: {json.dumps(extracted_phenomena, ensure_ascii=False)}")
    return extracted_phenomena

# --- Processor cho ROC Service (File CSV, cột _source.log chứa string không cấu trúc) ---
def process_roc_clean_logs(csv_log_content: str, roc_config: dict, processing_time_utc: datetime | None = None):
    service_name = roc_config.get("service_name", "ROC Service")
    print(f"ROC CSV Parser: Processing CSV data for service: '{service_name}'")
    
    extracted_phenomena = {
        "service_name": service_name,
        "errors_summary": [], 
        "metrics_summary": {}, 
        "other_key_info": []  
    }

    csv_opts = roc_config.get("csv_processing_options", {})
    log_col_name_csv = roc_config.get("unstructured_log_column_name", "_source.log")
    # metadata_cols_to_keep_csv = roc_config.get("csv_metadata_columns_to_keep", [])
    
    # ts_processing_cfg = roc_config.get("timestamp_processing", {})
    # ts_source_col_csv = ts_processing_cfg.get("source_field_in_csv") 
    # ts_input_format = ts_processing_cfg.get("input_format")

    roc_analysis_cfg = roc_config.get("roc_log_string_analysis", {})
    keywords_to_extract_cfg = roc_analysis_cfg.get("keywords_to_extract", [])
    max_context_chars = roc_analysis_cfg.get("max_context_chars_around_keyword", 100)

    cfg_metric_definitions = roc_config.get("metrics_to_derive_from_roc_keywords", [])
    cfg_max_keyword_examples = roc_config.get("summary_options", {}).get("max_keyword_match_examples", 7)

    all_keyword_match_events = [] # List các dict, mỗi dict là một keyword match event

    try:
        corrected_log_content = csv_log_content.replace('“', '"').replace('”', '"')
        csvfile = StringIO(corrected_log_content)
        reader = csv.DictReader(csvfile, delimiter=csv_opts.get("delimiter", ","), quotechar=csv_opts.get("quotechar", "\""))
        
        processed_rows_count = 0
        for csv_row_dict in reader:
            processed_rows_count += 1
            log_string_unstructured = csv_row_dict.get(log_col_name_csv)
            
            if not log_string_unstructured or not log_string_unstructured.strip():
                continue

            log_string_lower = log_string_unstructured.lower()

            for kw_config in keywords_to_extract_cfg:
                keyword_to_find = kw_config.get("keyword", "").lower() # Keyword từ config
                if not keyword_to_find: continue

                # Tìm tất cả các lần xuất hiện
                current_pos = 0
                while current_pos < len(log_string_lower):
                    found_pos = log_string_lower.find(keyword_to_find, current_pos)
                    if found_pos == -1: # Không tìm thấy nữa
                        break
                    
                    start = found_pos
                    end = found_pos + len(keyword_to_find)
                    
                    context_start = max(0, start - max_context_chars)
                    context_end = min(len(log_string_unstructured), end + max_context_chars)
                    context_snippet = log_string_unstructured[context_start:context_end].replace('\n', ' ').replace('\r', '')
                    
                    keyword_match_event = {
                        "type": "keyword_match", 
                        "keyword_matched": kw_config.get("keyword"), 
                        "severity": kw_config.get("severity", "unknown"),
                        "category": kw_config.get("category", "general"),
                        "context_snippet": context_snippet,
                        # "original_csv_metadata": {meta_col: csv_row_dict.get(meta_col) for meta_col in metadata_cols_to_keep_csv}, 
                        # "original_csv_timestamp_utc": get_aware_utc_datetime(csv_row_dict.get(ts_source_col_csv), ts_input_format).isoformat() if ts_source_col_csv and csv_row_dict.get(ts_source_col_csv) and ts_input_format else None
                    }
                    all_keyword_match_events.append(keyword_match_event)

                    if len(extracted_phenomena["errors_summary"]) < cfg_max_keyword_examples:
                        summary_text = f"ROC Keyword '{kw_config.get('keyword')}' (Sev: {kw_config.get('severity')}) found. Context: ...{context_snippet[max(0, len(context_snippet)//2 - 40) : len(context_snippet)//2 + 40]}..."
                        extracted_phenomena["errors_summary"].append(summary_text)
                    
                    current_pos = end # Tiếp tục tìm từ sau vị trí vừa tìm thấy
        
        if processed_rows_count == 0:
            print(f"Warning ROC Parser: CSV for {service_name} is empty.")
        else:
            print(f"DEBUG ROC Parser: Processed {processed_rows_count} CSV rows. Found {len(all_keyword_match_events)} total keyword matches for {service_name}.")

    except Exception as e_csv:
        print(f"ERROR ROC Parser: Could not parse CSV for {service_name}: {e_csv}")
        traceback.print_exc()
        extracted_phenomena["errors_summary"].append(f"Lỗi đọc CSV ROC: {str(e_csv)}")
        return extracted_phenomena
    
    if all_keyword_match_events:
        # cfg_metric_definitions của ROC sẽ tham chiếu đến các key trong keyword_match_event
        # (ví dụ: "severity", "category")
        extracted_phenomena["metrics_summary"] = _derive_metrics_from_parsed_data(
            all_keyword_match_events, # Truyền list các dict keyword match
            cfg_metric_definitions, 
            service_name
        )
    else:
        extracted_phenomena["metrics_summary"] = {}

    print(f"ROC Parser: Finished for '{service_name}'. Extracted: {json.dumps(extracted_phenomena, ensure_ascii=False)}")
    return extracted_phenomena

# --- Processor cho Dynatrace Alert (JSON) ---
def process_dynatrace_alert_json(json_alert_content: str, dynatrace_config: dict):
    # ... (Giữ nguyên như phiên bản trước)
    service_name = dynatrace_config.get("service_name", "Dynatrace Alert")
    print(f"Dynatrace Alert Parser: Processing data for '{service_name}'")
    extracted_phenomena = {"service_name": service_name, "errors_summary": [], "metrics_summary": {}, "other_key_info": []}
    
    cfg_json_processing = dynatrace_config.get("json_alert_processing", {})
    cfg_ts_processing = dynatrace_config.get("timestamp_processing", {})
    cfg_ts_fields_to_parse = cfg_ts_processing.get("fields_to_parse", []) # list các JSON key (đường dẫn) chứa timestamp
    cfg_ts_is_unix_ms = cfg_ts_processing.get("input_format_is_unix_ms", False)
    cfg_ts_input_format = cfg_ts_processing.get("input_format") # Ví dụ: "%Y-%m-%dT%H:%M:%S.%fZ"

    cfg_critical_keywords = dynatrace_config.get("critical_keywords_in_problem_details", [])
    cfg_problem_details_field_path = cfg_json_processing.get("problem_details_text_field", "") 
    
    cfg_metric_definitions = dynatrace_config.get("metrics_to_derive_from_alert_json", [])
    cfg_summary_fields_paths = dynatrace_config.get("summary_options", {}).get("max_alert_summary_fields", []) # list các JSON key path
    cfg_max_crit_info = dynatrace_config.get("summary_options", {}).get("max_critical_info_from_keywords", 3)

    try:
        alert_data_dict_orig = json.loads(json_alert_content)
        if not isinstance(alert_data_dict_orig, dict):
            extracted_phenomena["errors_summary"].append(f"Dynatrace alert data not a JSON object: {type(alert_data_dict_orig)}")
            return extracted_phenomena
        
        # processed_alert_item_for_metrics sẽ chứa các giá trị đã được trích xuất và chuẩn hóa tên trường
        # dựa trên mapping từ cfg_json_processing (key là tên trường chuẩn, value là đường dẫn trong JSON alert)
        processed_alert_item_for_metrics = {}
        alert_summary_parts_for_display = []

        for std_field_name, dt_field_path in cfg_json_processing.items():
            path_keys = dt_field_path.split('.')
            current_val = alert_data_dict_orig
            try:
                for key_part in path_keys:
                    if isinstance(current_val, dict): current_val = current_val.get(key_part)
                    elif isinstance(current_val, list) and key_part.isdigit() and int(key_part) < len(current_val): # Xử lý index cho list
                        current_val = current_val[int(key_part)]
                    else: current_val = None; break
            except: current_val = None

            if current_val is not None:
                processed_val = current_val
                # Kiểm tra xem trường này có phải là timestamp cần parse không
                # So sánh dt_field_path (key gốc từ JSON) với danh sách cfg_ts_fields_to_parse
                if dt_field_path in cfg_ts_fields_to_parse:
                    if cfg_ts_is_unix_ms:
                        try: processed_val = datetime.fromtimestamp(int(current_val) / 1000, timezone.utc).isoformat() + "Z"
                        except (TypeError, ValueError): processed_val = str(current_val) 
                    elif cfg_ts_input_format:
                         try: processed_val = get_aware_utc_datetime(str(current_val), cfg_ts_input_format).isoformat() + "Z"
                         except: processed_val = str(current_val)
                
                processed_alert_item_for_metrics[std_field_name] = processed_val 
                if dt_field_path in cfg_summary_fields_paths: # So sánh với key gốc từ JSON
                    alert_summary_parts_for_display.append(f"{std_field_name}: {str(processed_val)[:100]}")
        
        if alert_summary_parts_for_display:
            extracted_phenomena["errors_summary"].append("; ".join(alert_summary_parts_for_display))

        if cfg_problem_details_field_path and cfg_critical_keywords:
            path_keys = cfg_problem_details_field_path.split('.')
            problem_text = alert_data_dict_orig
            try:
                for key_part in path_keys:
                    if isinstance(problem_text, dict): problem_text = problem_text.get(key_part)
                    else: problem_text = None; break
            except: problem_text = None

            if isinstance(problem_text, str):
                problem_text_lower = problem_text.lower()
                for keyword in cfg_critical_keywords:
                    if keyword.lower() in problem_text_lower:
                        if len(extracted_phenomena["other_key_info"]) < cfg_max_crit_info:
                            extracted_phenomena["other_key_info"].append(f"Dynatrace Keyword '{keyword}' in details: {problem_text[:150]}...")
                        break 
        
        if processed_alert_item_for_metrics: 
            extracted_phenomena["metrics_summary"] = _derive_metrics_from_parsed_data(
                [processed_alert_item_for_metrics], 
                cfg_metric_definitions, service_name
            )
    except json.JSONDecodeError:
        extracted_phenomena["errors_summary"].append("Lỗi đọc Dynatrace JSON.")
    except Exception as e:
        extracted_phenomena["errors_summary"].append(f"Lỗi xử lý Dynatrace: {str(e)}")
        traceback.print_exc()

    print(f"CleanParser: Finished processing Dynatrace Alert for '{service_name}'. Extracted: {json.dumps(extracted_phenomena, ensure_ascii=False)}")
    return extracted_phenomena
