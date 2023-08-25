from functools import reduce
import re
import logging


def deep_get(dictionary:dict, keys:str, default=None):
    return reduce(lambda d, key: d.get(key, default) if isinstance(d, dict) else default, keys.split("."), dictionary)


def check_data(kind, data_list:list, valid_check:dict):
    result = True
    checked_data = []
    error_data = []
    for data in data_list:
        for key, value in data.items():
            if key not in valid_check:
                continue
            is_valid = True
            for valid_key, valid_value in valid_check.items():
                if valid_key == 'type':
                    if valid_value == 'string':
                        is_valid = is_valid & isinstance(value, str)
                    elif valid_value == 'integer':
                        is_valid = is_valid & isinstance(value, int)
                    elif valid_value == 'float':
                        is_valid = is_valid & isinstance(value, float)     
                elif valid_key == 'nullable':
                    # null 가능하고 값이 null인 경우
                    if value and isinstance(value, None):
                        continue
                elif valid_key == 'format':
                    if re.match(value, ):
                        is_valid = is_valid
                    else:
                        is_valid = False
                if not is_valid:
                    break;
            result = result & is_valid
            if not result:
                break;
        if result:
            checked_data.append(data)
        else:
            error_data.append(data)
    logging.info(f"[{kind}] 데이터 포맷 체크 결과: {len(error_data)}/{len(checked_data)}")
    return {
        "checked_data": checked_data,
        "error_data": error_data
    }