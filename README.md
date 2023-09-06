# 오픈소스 트렌드 분석
오픈 소스에 대한 릴리즈, 이슈, 관련 질문, 관심도 등을 파악할 수 있도록 시각화 하여 대시 보드를 제공합니다.

## #️⃣ 주제 선정 이유 
정보통신산업진흥원의 '2021 오픈소스SW(OSS) 실태조사 보고서’ 따르면, 국내 기업의 오픈 소스 활용 비율은 61.5%로 많은 것을 확인할 수 있습니다.  
Microsoft가 Github를, IBM이 Red Hat을 인수하는 등 오픈 소스가 더욱 중요해지고 있습니다. 


오픈 소스를 도입하려면, 새로운 오픈 소스가 무엇이 있는지 기존에 도입한 오픈 소스의 이슈 혹은 버그 수정 여부 등을 지속적으로 확인해야 합니다.  
 따라서, 이번 프로젝트에는 오픈 소스 트랜드 및 상세 정보를 파악할 수 있는 대시보드를 제공하는 것을 주제로 선정하였습니다.

> **기대효과**
> 1. repository top 10  
>    - star 수를 기준으로 현재 가장 관심 있어하는 repository를 확인할 수 있습니다.
> 2. repository 상세 내용
>    - release, commit, issue 등 repository 상세 내용을 확인할 수 있습니다.
> 3. google trend 관련 키워드
>    - repository와 연관된 키워드를 확인할 수 있습니다.
> 4. 관련 stack overflow 최신 질문 목록
>    -  repository와 관련된 stack overflow 질문 목록을 확인할 수 있습니다.

<br> 

## 👪 팀원 및 역할 분담
| 이름 | 구분 | 역할 | 
|:---:|:---:|:---:|   
| **김상희** <br> **[@ksh1357](https://github.com/ksh1357)** | **Airflow**  | web crawling을 통한 데이터 수집 DAG 개발 <br>  • Github 데이터 수집 DAG 개발 <Br> • 데이터 정합성 확인 DAG 개발   | 
| | **Preset** | • 대시보드 및 차트 생성 |  | 
|**남윤아** <Br> **[@namuna309](https://github.com/namuna309)** |**Airflow**| • Plugin 개발(Slack, AWS S3 read and write 등)  <Br> • StackOverflow 데이터 수집 DAG 개발 <br> • EMR Data Process 자동화 DAG 개발 
| | **Spark** |• Raw Data 정제 및 S3 적재 모듈 생성 | |
| | **AWS** | • AWS EC2, EMR 관리 <br> • EC2 관리를 위한 CloudWatch 경보,  EventBridge 규칙, Lambda 함수 개발 |
|**김혜민** <br> **[@HyeM207](https://github.com/HyeM207)** |**Airflow**| • Docker-Compose 환경 구축 <br> • Github 데이터 수집 DAG 수정 및    GoogleTrend  데이터 수집 DAG 개발 <br> • Plugin 개발 (Slack, Pytrend 호출 등) <br> • Airflow Monitoring 환경 구축 (Grafana) |
| | **Snowflake** | 데이터 마트 구축 (S3ToSnowflake) | 
| | **Flask** | Redis 연결 Flask REST-API개발 | 

## #️⃣ 기술 스택 
| 기술 스택  | Develop 환경 | Production 환경 | 
| -- | -- | -- |
| Apache Spark (Python) | 로컬 (Docker) | Amazon EMR (m5.xlarge) | 
| Apache Airflow & monitoring (Python) |로컬 (Docker Compose) | Amazon EC2 (t3.2xlarge)  | 
| Data Warehouse | Snowflake | Snowflake |
| Data Lake | Amazon S3 | Amazon S3 | 
| AWS Monitoring | - | Amazon CloudWatch | 
| Dashboard |  Preset | Preset | 
| SCM (Software Configuration Management) | Github | Github|
| 기타  | - |  Amazon SecretManager, Amazon Lambda | 

## #️⃣ 아키텍처
![image](https://github.com/de-final-2-2team/.github/assets/63229014/2a613298-f92b-4cd3-9d43-c64aba75307d)


## #️⃣ 시각화 (Preset)
1. 오픈 소스 트랜드
![image](https://github.com/de-final-2-2team/.github/assets/63229014/f4a51276-af30-432a-bf22-59f428a07a25)


2. Repository 상세
![image](https://github.com/de-final-2-2team/.github/assets/63229014/e5b9759a-6c2c-42fb-84ef-1f1e1c23b830)



## ⭐ 결과물 
- [2팀-2조 최종 프로젝트 PPT.pdf](https://github.com/de-final-2-2team/.github/files/12509880/2.-2.pdf)

- [2팀 2조 프로젝트 보고서.pdf](https://github.com/de-final-2-2team/opensource-trends-airflow/files/12534550/2.2.pdf)


## 💻 환경 구축
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
