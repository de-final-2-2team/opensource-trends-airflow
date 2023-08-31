from pytrends.request import TrendReq
import pandas as pd
from airflow.exceptions import (
    AirflowException,
    AirflowFailException
)
import logging
import time
import requests


class PyTrendsWrapper:
    def __init__(self):
        session = requests.Session()
        session.get('https://trends.google.com/trends/explore')
        cookies_map = session.cookies.get_dict()
        nid_cookie = cookies_map['NID']
        
        self.pytrends = TrendReq(hl='en-US', tz=540, timeout=(20,30), retries=3, backoff_factor=0.1, requests_args={'headers': {'Cookie' : f'NID={nid_cookie}'}})

    def build_payload(self, keyword, timeframe, geo):
        if geo == "WORLD":
            geo = ''
        self.pytrends.build_payload([keyword], cat=313, timeframe=timeframe, geo=geo)

    def get_request(self, kind, keyword, timeframe, geo):
        try:
            self.build_payload(keyword=keyword, timeframe=timeframe, geo=geo)
            time.sleep(2)
            if kind == "it_time":
                result = self.pytrends.interest_over_time().reset_index()
            elif kind == "it_region":
                dataframes = []
                resolution_list = ['CITY', 'COUNTRY']
                for resolution in resolution_list: 
                    df = self.pytrends.interest_by_region(resolution=resolution, inc_low_vol=True, inc_geo_code=True).reset_index()
                    dataframes.append(df)
                result = pd.concat(dataframes, ignore_index=True)
            elif kind == "rel_topics":
                result = self.pytrends.related_topics()
            elif kind == "rel_queries":
                result = self.pytrends.related_queries()
            else:
                raise AirflowException(f"[{kind}] pytrend 데이터 수집 실패 : 맞지 않는 kind 입니다.\\n")
            
            logging.info(f"[get_request] {result}")
            return result.to_dict()
        except Exception as e:
            raise AirflowFailException(f"[{kind}] 데이터 수집 실패\\n" + repr(e))