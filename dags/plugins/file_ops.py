import logging
import json
import os 

def load_as_json(file_path, kind, content):
    save_path = f"{file_path}/../data"
    os.makedirs(save_path, exist_ok=True)

    with open(f"{save_path}/{kind}.json", 'w') as f:
        json.dump(content, f)
    if isinstance(content, list):
        logging.info(f"[{kind}] 데이터 수집 완료 | 데이터 수: {len(content)}")
    else:
        logging.info(f"[{kind}] 데이터 수집 완료")
