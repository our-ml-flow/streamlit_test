from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy import text
from datetime import datetime, timedelta

import streamlit as st
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px


# DB 연결
def init_connection():
    
    block = SqlAlchemyConnector.load("gcp-mlops-sql-postgres")
    engine = block.get_engine()
    connection = engine.connect()

    return engine


def recent_trade(engine):
    # 조회할 쿼리
    query = """ 
                select * from daily_whale_buy_list2;
            """
    
    # 데이터 df
    whale_df = pd.read_sql(query, engine)
    



if __name__ == '__main__':

    st.header('일자별 거래 현황', divider='rainbow')

    d = st.date_input("원하는 날짜를 선택하세요", datetime.today()- timedelta(1))
    st.write('You choose :', d)
