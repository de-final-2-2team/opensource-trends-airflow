from pytrends.request import TrendReq
import pandas as pd
from airflow.exceptions import (
    AirflowException,
    AirflowFailException
)
import logging


def bulid_payload(keyword, timeframe, geo):
    pytrends = TrendReq(hl='en-US', tz=540, timeout=(10,25), retries=2, backoff_factor=0.1, requests_args={'verify':False})
    if geo == "WORLD":
        geo = ''
    pytrends.build_payload([keyword], cat=313, timeframe=timeframe, geo=geo)
    return pytrends


def get_request(kind, keyword, timeframe, geo):
    try:
        _pytrends = bulid_payload(keyword=keyword, timeframe=timeframe, geo=geo)

        result = []
        if kind == "it_time":
            result = _pytrends.interest_over_time().reset_index()
        elif kind == "it_region":
            # :return  DATAFRAME
            dataframes = []
            resolution_list = ['CITY', 'COUNTRY']
            for resolution in resolution_list: 
                df = _pytrends.interest_by_region(resolution=resolution, inc_low_vol=True, inc_geo_code=True).reset_index()
                dataframes.append(df)
            result = pd.concat(dataframes, ignore_index=True)
        elif kind == "rel_topics":
            # :return  Dictionary
            result = _pytrends.related_topics()
        elif kind == "rel_queries":
            result = _pytrends.related_queries()
        else:
            raise AirflowException(f"[{kind}] pytrend 데이터 수집 실패 : 맞지 않는 kind 입니다.\\n")
        
        logging.info(f"[get_request] keyword: {keyword} result : {result}")
        return result
    except Exception as e:
        raise AirflowFailException(f"[{kind}] 데이터 수집 실패\\n" + repr(e))