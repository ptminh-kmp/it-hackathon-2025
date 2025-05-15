import re
from datetime import datetime, timezone, timedelta

def get_aware_utc_datetime(timestamp_str: str | None, timestamp_format_from_config: str = "%Y-%m-%dT%H:%M:%S.%fZ") -> datetime:
    if timestamp_str:
        try:
            if 'Z' in timestamp_str:
                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            elif '+' in timestamp_str[10:] or '-' in timestamp_str[10:]: # Sau YYYY-MM-DD
                return datetime.fromisoformat(timestamp_str)
        except ValueError: # Nếu fromisoformat thất bại với các trường hợp trên
            pass # Tiếp tục thử strptime bên dưới

        # Thử strptime với format từ config
        try:
            dt_naive = datetime.strptime(timestamp_str, timestamp_format_from_config)
            # print(f"DEBUG LogParserUtils: Timestamp '{timestamp_str}' parsed with strptime (format: '{timestamp_format_from_config}') was naive, assumed UTC.")
            return dt_naive.replace(tzinfo=timezone.utc)
        except ValueError as e_strptime:
            print(f"Warning LogParserUtils: Could not parse timestamp '{timestamp_str}' with ISO or specific format '{timestamp_format_from_config}'. Error: {e_strptime}. Using current UTC as fallback.")
            return datetime.now(timezone.utc)
    else:
        # print("Warning LogParserUtils: Log line missing timestamp string. Using current UTC as fallback.")
        return datetime.now(timezone.utc)