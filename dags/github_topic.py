from airflow import DAG
from airflow.decorators import task
from datetime import datetime


@task
def load(kind, content):
    from plugins.file_ops import load_as_json
    
    import logging
    import os

    logging.info(f"[{{ dag_id }}:{kind}] 데이터 저장 시작")
    dag_root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_as_json(file_path=dag_root_path, filename=kind, content=content, subpath=kind)


@task
def get_topics(url, wait_time = 60):
    # selenium으로부터 webdriver 모듈을 불러옵니다.
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.service import Service
    # Explicit Wait을 위해 추가
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException

    import logging

    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    options.add_argument('window-size=1920x1080')
    options.add_argument("disable-gpu")
    options.add_argument(f'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    # WebDriverException: page crash
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    topics = []
    with webdriver.Remote('remote_chromedriver:4444/wd/hub', options=options) as driver:
        driver.get(url)
        element = WebDriverWait(driver, wait_time).until(EC.presence_of_element_located((By.CLASS_NAME, 'py-4')))

        # 버튼 누르기
        while driver.find_element(By.CSS_SELECTOR, ".ajax-pagination-btn").is_displayed:
            try:
                WebDriverWait(driver, 1).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, ".ajax-pagination-btn"))
                ).click()
            except TimeoutException:
                break


        # 링크 가져오기
        sections = driver.find_elements(By.CSS_SELECTOR, '.py-4 > .flex-column')
        try:
            for section in sections:
                try:
                    topic_url = section.get_attribute('href')
                    topic_name = topic_url[topic_url.rfind("/") + 1:]
                    topics.append({"TOPIC_NM": topic_name,"TOPIC_URL": topic_url})
                except Exception as e:
                    logging.error("invalid topic url\n" + repr(e))
        except NoSuchElementException as e:
            logging.error("No Such Element '.py-4 > a'\n" + repr(e))
    return topics


with DAG(
    dag_id="github_topic",
    start_date=datetime(2023, 8, 30),
    schedule='52 23 * * *',
    catchup=False
) as dag:
    load(kind="topic", content=get_topics(url="https://github.com/topics", wait_time=60))
