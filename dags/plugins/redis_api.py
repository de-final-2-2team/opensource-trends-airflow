import requests
import logging

def save_repo_to_redis(_id, full_nm):
    url = "http://flask-api:5000/repo/save_repo"
    try: 
        repo_data = {
            "ID": _id,
            "FULL_NM": full_nm
        }
        response = requests.post(url, json=repo_data, timeout=10)
        response_data = response.json()

        if response.status_code == 201:
            return response_data
        else:
            raise Exception(f"[{response.status_code}] API 요청실패! {response_data}")
    except requests.exceptions.RequestException as req_exception:
        raise req_exception
    except ValueError as value_error:
        raise value_error
    except Exception as e: 
        raise e
