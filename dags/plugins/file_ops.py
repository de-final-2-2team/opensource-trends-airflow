import logging
import json
import os 
from datetime import datetime

# def load_as_json(file_path, kind, content, repo=None):
#     save_path = f"{file_path}/data/{kind}"
#     os.makedirs(save_path, exist_ok=True)

#     current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
#     if repo : 
#         file_name = f"{repo}_{current_datetime}_.json"
#     else: 
#         file_name = f"{current_datetime}_.json"

#     with open(f"{save_path}/{file_name}", 'w') as f:
#         json.dump(content, f)
#     if isinstance(content, list):
#         logging.info(f"[{kind}] 데이터 수집 완료 | 데이터 수: {len(content)}")
#     else:
#         logging.info(f"[{kind}] 데이터 수집 완료")

def load_as_json(file_path, subpath, filename, content):
    save_path = f"{file_path}/data/{subpath}"
    os.makedirs(save_path, exist_ok=True)

    current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    file_name = f"{filename}_{current_datetime}_.json"

    with open(f"{save_path}/{file_name}", 'w') as f:
        json.dump(content, f)
    if isinstance(content, list):
        logging.info(f"[{filename}] 데이터 수집 완료 | 데이터 수: {len(content)}")
    else:
        logging.info(f"[{filename}] 데이터 수집 완료")