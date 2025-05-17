# src/common_lib/service_specific_parsers.py
import csv
from io import StringIO
from datetime import datetime, timezone, timedelta
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
        return False
    try:
        if operator == "is_null": return item_value is None
        if operator == "is_not_null": return item_value is not None
        
        # Đối với các operator còn lại, item_value không nên là None (trừ khi đã được xử lý ở trên)
        if item_value is None: return False

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
        elif not isinstance(item_value, type(condition_value_from_config)) and \
             not (isinstance(item_value, (int,float)) and isinstance(condition_value_from_config, (int,float))):
            # Nếu kiểu khác nhau và không phải cả hai đều là số, so sánh chuỗi
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


def _derive_metrics_from_parsed_data( # Đổi tên từ _derive_metrics_from_parsed_rows
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
        current_metric_value = None

        # Các trường điều kiện và target field ở đây sẽ là TÊN TRƯỜNG CHUẨN sau khi map (nếu áp dụng)
        # Hoặc tên trường trực tiếp từ dict (ví dụ: từ keyword_match_info)
        condition_field_standard = m_cfg.get("condition_field") 
        condition_op = m_cfg.get("condition_operator")
        condition_val_config = m_cfg.get("condition_value")
        condition_values_list_config = m_cfg.get("condition_values_list") # Cho 'in' hoặc 'contains_any_keyword'
        
        target_field_standard = m_cfg.get("target_field") # Cho average, sum,...
        count_field_standard = m_cfg.get("count_field") # Cho count_by_field_value...

        if metric_type == "total_row_count": 
             current_metric_value = len(list_of_parsed_data_dicts)
        
        elif metric_type == "count_if_condition_met" or metric_type == "count_if_multiple_conditions_met":
            # Xử lý multiple conditions
            conditions_to_check = m_cfg.get("conditions") if metric_type == "count_if_multiple_conditions_met" else \
                                  [{"condition_field": condition_field_standard, 
                                    "condition_operator": condition_op, 
                                    "condition_value": condition_val_config,
                                    "condition_values_list": condition_values_list_config
                                   }] if condition_field_standard else [] # Nếu là single, tạo list 1 condition

            if not conditions_to_check:
                print(f"MetricDeriver: No conditions defined for metric '{metric_name}' (count_if_condition_met/multiple) in {service_name_for_log}.")
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

                    if not all([cond_field, cond_op]) or (cond_val is None and cond_list_val is None and cond_op not in ["is_null", "is_not_null"]):
                        print(f"MetricDeriver: Misconfig for sub-condition {cond_idx} in metric '{metric_name}' for {service_name_for_log}.")
                        all_conditions_met_for_item = False; break
                    
                    item_value_from_dict = item_dict.get(cond_field) 
                    if not _evaluate_condition(item_value_from_dict, cond_op, cond_val, cond_list_val):
                        all_conditions_met_for_item = False; break
                
                if all_conditions_met_for_item:
                    count += 1
            current_metric_value = count

        elif metric_type == "average_of_field":
            if not target_field_standard: continue
            values_for_avg = []
            for item_dict in list_of_parsed_data_dicts:
                if not isinstance(item_dict, dict): continue
                val_from_item = item_dict.get(target_field_standard)
                if val_from_item is not None:
                    try: values_for_avg.append(float(val_from_item))
                    except (ValueError, TypeError): pass
            if values_for_avg: current_metric_value = round(sum(values_for_avg) / len(values_for_avg), 2)
            else: current_metric_value = 0.0
            
        elif metric_type == "count_by_field_value":
            if not count_field_standard: continue
            value_counts = Counter()
            for item_dict in list_of_parsed_data_dicts:
                if not isinstance(item_dict, dict): continue
                value_to_count = str(item_dict.get(count_field_standard, "N/A_FIELD_VALUE"))
                value_counts[value_to_count] += 1
            current_metric_value = dict(value_counts)
        
        elif metric_type == "sum_of_keyword_counts": # Dùng cho ROC, data_list là list các keyword_match_info
            total_keywords = 0
            for event_dict in list_of_parsed_data_dicts:
                if isinstance(event_dict, dict) and event_dict.get("type") == "keyword_match":
                     total_keywords += 1 
            current_metric_value = total_keywords
            
        elif metric_type == "group_keyword_counts_by_category": # Dùng cho ROC
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

# --- Processor cho ESB Service (File CSV chỉ chứa lỗi) ---
def process_esb_logs(csv_log_content: str, esb_config: dict, processing_time_utc: datetime | None = None):
    service_name = esb_config.get("service_name", "ESB Service (Error Logs Only)")
    print(f"ESB CSV Parser (Assumed Error Logs Only): Processing for service: '{service_name}'")
    
    extracted_phenomena = {
        "service_name": service_name, "errors_summary": [], 
        "metrics_summary": {}, "other_key_info": []  
    }

    csv_opts = esb_config.get("csv_processing_options", {})
    col_mapping = esb_config.get("csv_column_to_field_mapping", {})
    
    # Lấy tên trường chuẩn cho message và operation từ mapping
    # Nếu không có trong mapping, sẽ không thể trích xuất đúng
    std_message_field = next((std_f for std_f, csv_f in col_mapping.items() if csv_f == esb_config.get("clean_log_fields_to_extract", {}).get("message_field")), "log_message")
    std_operation_field = next((std_f for std_f, csv_f in col_mapping.items() if csv_f == esb_config.get("clean_log_fields_to_extract", {}).get("operation_field")), "operation_name")
    std_timestamp_field = next((std_f for std_f, csv_f in col_mapping.items() if csv_f == esb_config.get("timestamp_processing", {}).get("source_field")), "timestamp")


    critical_keywords = esb_config.get("critical_keywords_in_message_field", [])
    cfg_metric_definitions = esb_config.get("metrics_to_derive_from_csv", [])
    cfg_max_err_examples = esb_config.get("summary_options", {}).get("max_error_event_examples", 10)
    cfg_max_crit_info = esb_config.get("summary_options", {}).get("max_critical_info_from_keywords", 5)

    parsed_rows_with_standard_fields = []
    try:
        corrected_log_content = csv_log_content.replace('“', '"').replace('”', '"')
        csvfile = StringIO(corrected_log_content)
        reader = csv.DictReader(csvfile, delimiter=csv_opts.get("delimiter", ","), quotechar=csv_opts.get("quotechar", "\""))

        for line_number, csv_row_dict in enumerate(reader):
            standard_field_row = {}
            # Ánh xạ từ tên cột CSV gốc sang tên trường chuẩn
            for std_name, csv_col_name_in_map in col_mapping.items():
                standard_field_row[std_name] = csv_row_dict.get(csv_col_name_in_map)
            
            # Parse timestamp (nếu có) sang datetime object và lưu dưới một key chuẩn (ví dụ _datetime_obj)
            # Điều này hữu ích nếu metric cần so sánh thời gian sau này, mặc dù hiện tại ESB ko dùng
            ts_str_val = standard_field_row.get(std_timestamp_field)
            ts_format = esb_config.get("timestamp_processing",{}).get("input_format")
            if ts_str_val and ts_format:
                standard_field_row["_datetime_obj_utc"] = get_aware_utc_datetime(ts_str_val, ts_format)

            parsed_rows_with_standard_fields.append(standard_field_row)

            # Vì tất cả dòng đều là lỗi, lấy ví dụ từ đây
            if len(extracted_phenomena["errors_summary"]) < cfg_max_err_examples:
                err_msg = str(standard_field_row.get(std_message_field, "N/A"))
                op_name = str(standard_field_row.get(std_operation_field, "N/A"))
                summary = f"ESB Error - Op: '{op_name}', Msg: {err_msg[:150]}" + ("..." if len(err_msg) > 150 else "")
                extracted_phenomena["errors_summary"].append(summary)

            # Kiểm tra critical keywords
            msg_content_std = str(standard_field_row.get(std_message_field, "")).lower()
            for keyword in critical_keywords:
                if keyword.lower() in msg_content_std:
                    if len(extracted_phenomena["other_key_info"]) < cfg_max_crit_info:
                        ts_orig_str = standard_field_row.get(std_timestamp_field, "")
                        info_summary = f"Keyword '{keyword}' at {ts_orig_str} in msg: {msg_content_std[:120]}" + ("..." if len(msg_content_std) > 120 else "")
                        extracted_phenomena["other_key_info"].append(info_summary)
                    break 
        
        if not parsed_rows_with_standard_fields:
            print(f"Warning ESB Parser: CSV for {service_name} is empty or no data rows.")
        else:
            print(f"DEBUG ESB Parser: Parsed {len(parsed_rows_with_standard_fields)} error log data rows for {service_name}.")

    except Exception as e_csv:
        print(f"ERROR ESB Parser: Could not parse CSV for {service_name}: {e_csv}")
        traceback.print_exc()
        extracted_phenomena["errors_summary"].append(f"Lỗi đọc CSV: {str(e_csv)}")
        return extracted_phenomena
    
    if parsed_rows_with_standard_fields:
        # Trong config, metrics_to_derive_from_csv sẽ dùng tên trường chuẩn
        extracted_phenomena["metrics_summary"] = _derive_metrics_from_parsed_data(
            parsed_rows_with_standard_fields, 
            cfg_metric_definitions, # Giờ đây metric_definitions đã dùng tên trường chuẩn
            service_name
        )
    else:
        extracted_phenomena["metrics_summary"] = {}

    print(f"ESB CSV Parser (Error Logs Only): Finished for '{service_name}'. Extracted: {json.dumps(extracted_phenomena, ensure_ascii=False)}")
    return extracted_phenomena


# --- Processor cho APIGW Service (File CSV, cần xác định lỗi) ---
def process_apigw_logs(csv_log_content: str, apigw_config: dict, processing_time_utc: datetime | None = None):
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
    for std_n, csv_n in col_mapping.items():
        if csv_n == err_cond_col_name_csv:
            err_cond_field_std = std_n
            break
    err_cond_op = error_condition_cfg.get("operator")
    err_cond_val_config = error_condition_cfg.get("value")
    err_cond_keywords_list = error_condition_cfg.get("keywords")

    std_message_field = "log_content_detail" # Tên trường chuẩn cho message/content của APIGW
    std_handler_field = "handler_function" # Tên trường chuẩn cho handler/message của APIGW

    critical_keywords_cfg = apigw_config.get("critical_keywords_in_message_field", []) # Có thể là _in_content_field
    cfg_metric_definitions = apigw_config.get("metrics_to_derive_from_csv", [])
    cfg_max_err_examples = apigw_config.get("summary_options", {}).get("max_error_event_examples", 5)
    cfg_max_crit_info = apigw_config.get("summary_options", {}).get("max_critical_info_from_keywords", 3)

    parsed_rows_with_standard_fields = []
    error_rows_for_metrics = [] # Chỉ chứa các dòng được xác định là lỗi
    try:
        corrected_log_content = csv_log_content.replace('“', '"').replace('”', '"')
        csvfile = StringIO(corrected_log_content)
        reader = csv.DictReader(csvfile, delimiter=csv_opts.get("delimiter", ","), quotechar=csv_opts.get("quotechar", "\""))

        for csv_row_dict in reader:
            standard_field_row = {std_name: csv_row_dict.get(csv_col) for std_name, csv_col in col_mapping.items()}
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
            print(f"DEBUG APIGW Parser: Parsed {len(parsed_rows_with_standard_fields)} total rows for {service_name}. Found {len(error_rows_for_metrics)} error rows.")

    except Exception as e_csv:
        print(f"ERROR APIGW Parser: Could not parse CSV for {service_name}: {e_csv}")
        traceback.print_exc()
        extracted_phenomena["errors_summary"].append(f"Lỗi đọc CSV APIGW: {str(e_csv)}")
        return extracted_phenomena

    if parsed_rows_with_standard_fields:
        updated_metric_definitions_apigw = []
        for m_cfg_orig in cfg_metric_definitions:
            m_cfg_new = m_cfg_orig.copy()
            for std_f_map, csv_f_in_map in col_mapping.items():
                if m_cfg_new.get("condition_field") == csv_f_in_map: m_cfg_new["condition_field"] = std_f_map
                if m_cfg_new.get("target_field") == csv_f_in_map: m_cfg_new["target_field"] = std_f_map
                if m_cfg_new.get("count_field") == csv_f_in_map: m_cfg_new["count_field"] = std_f_map
            updated_metric_definitions_apigw.append(m_cfg_new)
        
        # Quyết định list nào sẽ dùng để tính metric
        # Nếu metric là đếm lỗi, dùng error_rows_for_metrics
        # Nếu metric là tính chung (ví dụ avg_elapsed_time), dùng parsed_rows_with_standard_fields
        # Cần sửa _derive_metrics_from_parsed_data để nó có thể nhận 2 list hoặc logic trong metric_cfg phức tạp hơn
        # Hiện tại, đơn giản là truyền parsed_rows_with_standard_fields và để metric_cfg tự lọc
        
        # Tạo global_error_condition đã được map sang tên trường chuẩn
        global_err_cond_for_metrics_apigw = None
        if err_cond_field_std and err_cond_op: # Chỉ tạo nếu có định nghĩa error_condition
            global_err_cond_for_metrics_apigw = {
                "field_to_check": err_cond_field_std, # Tên trường chuẩn
                "operator": err_cond_op,
                "value": err_cond_val_config, # Giá trị từ config
                "keywords": err_cond_keywords_list # List keywords từ config
            }
            # print(f"DEBUG APIGW: Global error condition for metrics: {global_err_cond_for_metrics_apigw}")


        extracted_phenomena["metrics_summary"] = _derive_metrics_from_parsed_data(
            parsed_rows_with_standard_fields, # Luôn truyền tất cả các dòng đã parse
            updated_metric_definitions_apigw, 
            service_name
            # global_error_condition không cần truyền nữa nếu metric "count_if_condition_met" tự định nghĩa điều kiện
            # hoặc nếu metric "apigw_total_error_lines_in_file" đã có điều kiện là error_condition của service
        )
    else:
        extracted_phenomena["metrics_summary"] = {}

    print(f"APIGW CSV Parser: Finished for '{service_name}'. Extracted: {json.dumps(extracted_phenomena, ensure_ascii=False)}")
    return extracted_phenomena


# --- Processor cho ROC Service (File CSV, cột _source.log chứa string không cấu trúc) ---
def process_roc_logs(csv_log_content: str, roc_config: dict, processing_time_utc: datetime | None = None):
    service_name = roc_config.get("service_name", "ROC Service")
    print(f"ROC CSV Parser: Processing CSV data for service: '{service_name}'")
    
    extracted_phenomena = {
        "service_name": service_name,
        "errors_summary": [], # Sẽ chứa các ví dụ về keyword match
        "metrics_summary": {}, 
        "other_key_info": [] 
    }

    csv_opts = roc_config.get("csv_processing_options", {})
    log_col_name_csv = roc_config.get("unstructured_log_column_name", "_source.log")
    # metadata_cols_to_keep_csv = roc_config.get("csv_metadata_columns_to_keep", []) # Tên cột CSV gốc
    
    # ts_processing_cfg = roc_config.get("timestamp_processing", {})
    # ts_source_col_csv = ts_processing_cfg.get("source_field_in_csv") 
    # ts_input_format = ts_processing_cfg.get("input_format")

    roc_analysis_cfg = roc_config.get("roc_log_string_analysis", {})
    keywords_to_extract_cfg = roc_analysis_cfg.get("keywords_to_extract", [])
    max_context_chars = roc_analysis_cfg.get("max_context_chars_around_keyword", 100)

    cfg_metric_definitions = roc_config.get("metrics_to_derive_from_roc_keywords", [])
    cfg_max_keyword_examples = roc_config.get("summary_options", {}).get("max_keyword_match_examples", 7)

    all_keyword_matches_info = [] # List các dict, mỗi dict là một keyword match event

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

            # row_metadata = {meta_col: csv_row_dict.get(meta_col) for meta_col in metadata_cols_to_keep_csv if meta_col in csv_row_dict}
            # row_timestamp_aware_utc = None
            # if ts_source_col_csv and ts_input_format:
            #     row_timestamp_str = csv_row_dict.get(ts_source_col_csv)
            #     if row_timestamp_str: row_timestamp_aware_utc = get_aware_utc_datetime(row_timestamp_str, ts_input_format)

            log_string_lower = log_string_unstructured.lower()

            for kw_config in keywords_to_extract_cfg:
                keyword_to_find = kw_config.get("keyword", "").lower()
                if not keyword_to_find: continue

                for match in re.finditer(re.escape(keyword_to_find), log_string_lower):
                    start, end = match.span()
                    context_start = max(0, start - max_context_chars)
                    context_end = min(len(log_string_unstructured), end + max_context_chars)
                    context_snippet = log_string_unstructured[context_start:context_end]
                    
                    keyword_match_info = {
                        "type": "keyword_match", # Để _derive_metrics_from_parsed_data biết loại data
                        "keyword_matched": kw_config.get("keyword"), 
                        "severity": kw_config.get("severity", "unknown"),
                        "category": kw_config.get("category", "general"),
                        "context_snippet": context_snippet.replace('\n', ' ').replace('\r', ''), # Xóa newlines
                        # "original_csv_metadata": row_metadata, 
                        # "original_csv_timestamp_utc": row_timestamp_aware_utc.isoformat() if row_timestamp_aware_utc else None
                    }
                    all_keyword_matches_info.append(keyword_match_info)

                    if len(extracted_phenomena["errors_summary"]) < cfg_max_keyword_examples:
                        summary_text = f"ROC Keyword '{kw_config.get('keyword')}' (Sev: {kw_config.get('severity')}) found. Context: ...{context_snippet[max(0, len(context_snippet)//2 - 40) : len(context_snippet)//2 + 40]}..."
                        extracted_phenomena["errors_summary"].append(summary_text.replace('\n', ' ').replace('\r', ''))
        
        if processed_rows_count == 0:
            print(f"Warning ROC Parser: CSV for {service_name} is empty.")
        else:
            print(f"DEBUG ROC Parser: Processed {processed_rows_count} CSV rows. Found {len(all_keyword_matches_info)} total keyword matches for {service_name}.")

    except Exception as e_csv:
        print(f"ERROR ROC Parser: Could not parse CSV for {service_name}: {e_csv}")
        traceback.print_exc()
        extracted_phenomena["errors_summary"].append(f"Lỗi đọc CSV ROC: {str(e_csv)}")
        return extracted_phenomena
    
    if all_keyword_matches_info:
        # cfg_metric_definitions của ROC sẽ tham chiếu đến các key trong keyword_match_info
        # (ví dụ: "severity", "category")
        extracted_phenomena["metrics_summary"] = _derive_metrics_from_parsed_data(
            all_keyword_matches_info, 
            cfg_metric_definitions, 
            service_name
        )
    else:
        extracted_phenomena["metrics_summary"] = {}

    print(f"ROC Parser: Finished for '{service_name}'. Extracted: {json.dumps(extracted_phenomena, ensure_ascii=False)}")
    return extracted_phenomena

# --- Processor cho Dynatrace Alert (JSON) ---
def process_dynatrace_alert_json(json_alert_content: str, dynatrace_config: dict):
    # ... (Nội dung hàm này giữ nguyên như phiên bản trước, đã xử lý JSON và gọi _derive_metrics_from_parsed_data) ...
    service_name = dynatrace_config.get("service_name", "Dynatrace Alert")
    print(f"Dynatrace Alert Parser: Processing data for '{service_name}'")
    extracted_phenomena = {"service_name": service_name, "errors_summary": [], "metrics_summary": {}, "other_key_info": []}
    # (Copy toàn bộ logic của process_dynatrace_alert_json từ câu trả lời trước vào đây)
    cfg_json_processing = dynatrace_config.get("json_alert_processing", {})
    cfg_ts_processing = dynatrace_config.get("timestamp_processing", {})
    cfg_ts_fields_to_parse = cfg_ts_processing.get("fields_to_parse", [])
    cfg_ts_is_unix_ms = cfg_ts_processing.get("input_format_is_unix_ms", False)
    cfg_ts_input_format = cfg_ts_processing.get("input_format")
    cfg_critical_keywords = dynatrace_config.get("critical_keywords_in_problem_details", [])
    cfg_problem_details_field_path = cfg_json_processing.get("problem_details_text_field", "")
    cfg_metric_definitions = dynatrace_config.get("metrics_to_derive_from_alert_json", [])
    cfg_summary_fields = dynatrace_config.get("summary_options", {}).get("max_alert_summary_fields", [])
    cfg_max_crit_info = dynatrace_config.get("summary_options", {}).get("max_critical_info_from_keywords", 3)

    try:
        alert_data_dict_orig = json.loads(json_alert_content)
        if not isinstance(alert_data_dict_orig, dict): # ... (xử lý lỗi type)
            extracted_phenomena["errors_summary"].append(f"Dynatrace alert data not a JSON object: {type(alert_data_dict_orig)}")
            return extracted_phenomena
        
        # Tạo một dict mới để chứa các giá trị đã xử lý/chuẩn hóa cho metric và summary
        processed_alert_item_for_metrics = {}
        alert_summary_parts_for_display = []

        for std_field_name, dt_field_path in cfg_json_processing.items():
            path_keys = dt_field_path.split('.')
            current_val = alert_data_dict_orig
            try:
                for key_part in path_keys:
                    if isinstance(current_val, dict): current_val = current_val.get(key_part)
                    else: current_val = None; break
            except: current_val = None

            if current_val is not None:
                processed_val = current_val
                if dt_field_path in cfg_ts_fields_to_parse:
                    if cfg_ts_is_unix_ms:
                        try: processed_val = datetime.fromtimestamp(current_val / 1000, timezone.utc).isoformat() + "Z"
                        except: processed_val = str(current_val) 
                    elif cfg_ts_input_format:
                         processed_val = get_aware_utc_datetime(str(current_val), cfg_ts_input_format).isoformat() + "Z"
                
                processed_alert_item_for_metrics[std_field_name] = processed_val # Lưu giá trị đã xử lý với tên trường chuẩn
                if dt_field_path in cfg_summary_fields: # So sánh với key gốc từ config
                    alert_summary_parts_for_display.append(f"{std_field_name}: {str(processed_val)[:100]}")
        
        if alert_summary_parts_for_display:
            extracted_phenomena["errors_summary"].append("; ".join(alert_summary_parts_for_display))

        if cfg_problem_details_field_path and cfg_critical_keywords: # ... (logic tìm keyword như trước)
            path_keys = cfg_problem_details_field_path.split('.')
            problem_text = alert_data_dict_orig; # ... (logic lấy problem_text như trước)
            if isinstance(problem_text, str):
                # ... (logic tìm keyword và thêm vào other_key_info)
                pass # Giữ lại logic này

        if processed_alert_item_for_metrics: # Chỉ tính metric nếu có dữ liệu đã xử lý
            extracted_phenomena["metrics_summary"] = _derive_metrics_from_parsed_data(
                [processed_alert_item_for_metrics], # Truyền list chứa 1 item
                cfg_metric_definitions, service_name
            )
    # ... (phần còn lại của try-except)
    except json.JSONDecodeError: # ...
        extracted_phenomena["errors_summary"].append("Lỗi đọc Dynatrace JSON.")
    except Exception as e: # ...
        extracted_phenomena["errors_summary"].append(f"Lỗi xử lý Dynatrace: {str(e)}")
        traceback.print_exc()

    return extracted_phenomena
