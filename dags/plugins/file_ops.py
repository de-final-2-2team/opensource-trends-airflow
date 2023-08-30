import logging
import json
import os 
from datetime import datetime

def load_as_json(file_path, content, filename=None, subpath=None):
    save_path = f"{file_path}/data"
    if subpath:
        save_path += f"/{subpath}"

    os.makedirs(save_path, exist_ok=True)

    current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    if filename: 
        _file_name = f"{filename}_{current_datetime}_.json"
    else:
        _file_name = f"{current_datetime}_.json"

    with open(f"{save_path}/{_file_name}", 'w', encoding='utf-8') as f:
        json.dump(content, f)
    if isinstance(content, list):
        logging.info(f"[{filename}] 데이터 수집 완료 | 데이터 수: {len(content)}")
    else:
        logging.info(f"[{filename}] 데이터 수집 완료")