import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys

repo_dir = os.path.abspath(__file__).split('/streamlit')[0]
print(repo_dir)
sys.path.append(f'{repo_dir}')

from utils import get_sql_engine


# whale_wallet_address로 데이터 가져오는 함수
def load_wallet_address_info_data_from_db(whale_wallet_address):
    # DB 연결
    engine = get_sql_engine()

    # 조회할 쿼리
    query = """ 
                SELECT owner_address
                        , collection_name
                        , contract->>'address' AS address
                        , contract->>'tokenType' AS token_type
                        , contract->>'contractDeployer' AS contract_deployer
                        , opensea_slug
                        , opensea_floor_price
                FROM 
                    alchemy_collection_for_buyer        
                WHERE owner_address = %(whale_wallet_address)s            
                ;
            """

    params = {"whale_wallet_address": whale_wallet_address}

    whale_info_df = pd.read_sql(query, engine, params=params)

    return whale_info_df


# whale_info_df 전처리해 Top10 추출하는 함수
def preprocess_whale_info_df(whale_info_df):
    whale_nft_num_df = whale_info_df.groupby('collection_name')[['collection_name', 'address']].size().reset_index(name='count')
    
    whale_nft_num_df = whale_nft_num_df.sort_values(by='count', ascending=False)
    
    whale_nft_num_top_n_df = whale_nft_num_df.head(10)

    return whale_nft_num_top_n_df


# 보유 컬렉션 비율을 pie chart로 보여주는 함수
def make_pie_chart_for_collection_amount(whale_nft_num_top_n_df):
    # 데이터프레임을 기반으로 파이 차트 그리기
        fig = px.pie(whale_nft_num_top_n_df, values='count', names='collection_name', title='컬렉션 별 보유 NFT 수 Top 10')

        # 그래프 출력
        st.plotly_chart(fig)


# main 함수
def main_whale_info():
    whale_wallet_address = st.text_input(label="고래 지갑 주소를 입력하세요", placeholder="default value")

    # 고래 지갑 주소 입력되었을 경우 DB에서 데이터 조회해 컬렉션 현황 확인
    if st.button("입력"):
        con = st.container()

        con.markdown("## 입력한 고래 지갑 주소")
        con.write(f"{str(whale_wallet_address)}")

        # whale_wallet_address로 데이터 호출
        whale_info_df = load_wallet_address_info_data_from_db(whale_wallet_address)

        # whale_info_df를 전처리하여 '컬렉션 별 보유 NFT 수 Top 10' 추출
        whale_nft_num_top_n_df = preprocess_whale_info_df(whale_info_df)

        make_pie_chart_for_collection_amount(whale_nft_num_top_n_df)


if __name__ == '__main__':
    # Page 제목
    st.title("고래 정보 확인")

    main_whale_info()