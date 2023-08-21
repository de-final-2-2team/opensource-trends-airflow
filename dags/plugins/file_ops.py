import logging
import json


def load_as_json(file_path, kind, content):
    with open(f"{file_path}/result/{kind}.json", 'w') as f:
        json.dump(content, f)
    if isinstance(content, list):
        logging.info(f"[{kind}] 데이터 수집 완료 | 데이터 수: {len(content)}")
    else:
        logging.info(f"[{kind}] 데이터 수집 완료")
