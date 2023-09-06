# Flask-API
>  Redis 저장소의 데이터를 CRUD할 수 있는 REST-API 개발함 
> - Airflow DAG에서 해당 API를 호출하면 redis의 데이터를 조회 및 저장, 삭제 등 가능함
> - Github API token 정보와 Github Repository 목록 저장 목적에 맞게 커스텀함

<br>

## Airflow DAG에서 api 호출하기
### URL 형식
`http://flask-api:5000/{api}/{param}`

### 예시

```python
def get_git_all_tokens():
    response = requests.get('http://flask-api:5000/tokens/get_all_tokens')
    selected_token = response.json()
    print(selected_token)
```

## API 목록
<img width="913" alt="image" src="https://github.com/de-final-2-2team/opensource-trends-airflow/assets/63229014/6512dbb3-3117-4f50-ab38-5a6b5a0764d9">

### 기타
> Django가 아닌 Flask를 선택한 이유

- Django에 비해 Flask는 매우 단순하고 가볍다.    
➡️ 단순한 REST API 서버 만들기에는 Flask가 더 효율적이다.
