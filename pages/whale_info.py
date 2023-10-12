from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy import text

import streamlit as st
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import json

def init_connection():

    # DB 연결
    block = SqlAlchemyConnector.load("gcp-mlops-sql-postgres")
    engine = block.get_engine()
    connection = engine.connect()

    return engine



def whale_info():
    # Page 제목
    st.title("고래 정보 확인")

    whale_wallet_address = st.text_input(label="고래 지갑 주소를 입력하세요", placeholder="default value")

    # 고래 지갑 주소 입력되었을 경우 DB에서 데이터 조회해 컬렉션 현황 확인
    if st.button("입력"):
        con = st.container()
        con.caption("Result")

        con.markdown("## 입력한 고래 지갑 주소")
        con.write(f"{str(whale_wallet_address)}")

        # 조회할 쿼리
        query = """ 
                    SELECT owner_address, 
                            collection_name, 
                            contract->>'address' AS address, 
                            contract->>'tokenType' AS token_type, 
                            contract->>'contractDeployer' AS contract_deployer,
                            opensea_slug, 
                            opensea_floor_price
                    FROM alchemy_collection_for_buyer        
                    WHERE owner_address = %(whale_wallet_address)s            
                    ;
                """
        
        # DB 연결
        engine = init_connection()


        params = {"whale_wallet_address": whale_wallet_address}

        whale_info_df = pd.read_sql(query, engine, params=params)

        whale_nft_num_df = whale_info_df.groupby('collection_name')[['collection_name', 'address']].size().reset_index(name='count')
        whale_nft_num_df = whale_nft_num_df.sort_values(by='count', ascending=False)
        whale_nft_num_top_n_df = whale_nft_num_df.head(10)
        
        #st.write("컬렉션 별 보유 NFT 수 Top 10")

        # 데이터프레임을 기반으로 파이 차트 그리기
        fig = px.pie(whale_nft_num_top_n_df, values='count', names='collection_name', title='컬렉션 별 보유 NFT 수 Top 10')
        #whale_nft_num_top_n_df = whale_nft_num_top_n_df.transpose()

        # fig = px.bar(data_frame=whale_nft_num_top_n_df, x='collection_name', y='count')
        # 그래프 출력
        st.plotly_chart(fig)
        # st.bar_chart(fig)
        #fig.show()
        #st.table(whale_info_df)
        # st.table(whale_nft_num_df)


if __name__ == '__main__':
    whale_info()