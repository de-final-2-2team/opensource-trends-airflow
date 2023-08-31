# opensource-trends-airflow

## 환경 구축
```
docker-compose up --build
```
## 📂폴더 구조 (주요 파일 위주)
```
opensource-trends-airflow
│
├─ dags -- # Airflow DAG 폴더
│  │  
│  ├─ dynamic_dags -- # Dynamic DAGS 생성 관련 폴더
│  │  ├─ config  -- # .yml 모아놓은 폴더
│  │  ├─ generator.py  -- # Dynamic DAG 생성하는 DAG  
│  │  └─ github_api.jinja2 # Github API로 수집하는 Dynamic DAG
│  └─ plugins -- # 공통 기능 모아놓은 플러그인 폴더 
│     ├─ awsfunc.py
│     ├─ common.py
│     ├─ file_ops.py
│     ├─ github_api.py
│     ├─ redis_api.py
│     └─ slack.py
│ 
├─ docker-compose.yaml -- # Airflow, Flask 등 환경 구축
├─ Dockerfile -- # Airflow 추가 설정
├─ dockerfile.chrome -- # Airflow에서 크롤링하기 위한 컨테이너
│
├─ flask-api
│  ├─ app.py 
│  ├─ Dockerfile -- # Flask 서버 구동하기 위한  컨테이너 
│  ├─ README.md
│  ├─ repo -- # repo 관련 API 정의
│  ├─ requirements.txt
│  └─ tokens -- # token 관련 API 정의
│ 
├─ prometheus
│  └─ prometheus.yml -- # prometheus 설정 파일
├─ statsd_mapping.conf -- # statsd 메트릭 설정 파일
├─ requirements.txt -- # 전체 환경에 필요한 라이브러리 목록
└─ README.md

```