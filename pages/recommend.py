from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy import text
from datetime import timedelta

import streamlit as st
import plotly.express as px
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import requests
import random
import time
import re

@st.cache_resource
def init_connection():
    
    block = SqlAlchemyConnector.load("gcp-mlops-sql-postgres")
    engine = block.get_engine()
    connection = engine.connect()

    return engine

#@st.cache_resource
def make_df():
    engine = init_connection()

    recommend_query = """
        select 
            address,
            "collectionName",
            "imageUrl"
            from dune_nft_metadata;
                        """
    
    image_df = pd.read_sql(recommend_query, engine)

    image_df = image_df.dropna(subset=['imageUrl'], axis=0)
    image_df = image_df.drop_duplicates(['collectionName'])
    list_image_df = image_df[['collectionName', 'address', 'imageUrl']]
    list_image_choice = list_image_df.to_dict('records')

    return list_image_choice


def recommend():
    # Page 제목
    st.title("NFT 기반 추천")


def wallet_exist():

    st.write("본인 소유의 지갑이 있다면 지갑 주소를 적어주세요")
    user_wallet_address = st.text_input(label="User Wallet Address", value="default value")
    
    # 지갑 주소가 입력되면 가진 컬렉션 조회해 추천 모델에 전달
    if st.button("입력"):
        con = st.container()
        con.caption("지갑 주소")
        con.write(f"Your Wallet Address : {str(user_wallet_address)}")

        ## 변경 쿼리
        search_wallet_query = """ 
                    SELECT owner_address, 
                            collection_name, 
                            contract->>'address' AS address
                    FROM alchemy_collection_for_buyer        
                    WHERE owner_address = %(user_wallet_address)s            
                    ;
                """    
        # DB 연결
        engine = init_connection()


        params = {"user_wallet_address": user_wallet_address}

        wallet_info_df = pd.read_sql(search_wallet_query, engine, params=params)

        wallet_info_num_df = wallet_info_df.groupby('collection_name')[['collection_name', 'address']].size().reset_index(name='count')
        wallet_info_num_df = wallet_info_num_df.sort_values(by='count', ascending=False)
        wallet_info_num_top_n_df = wallet_info_num_df.head(5)

        wallet_exist_model_df = wallet_info_num_top_n_df[:3]
        
        wallet_exist_model_input = list(wallet_exist_model_df['collection_name'])
        st.write(wallet_exist_model_input)

        # 데이터프레임을 기반으로 파이 차트 그리기
        # fig = px.pie(wallet_info_num_top_n_df, values='count', names='collection_name', title='컬렉션 별 보유 NFT 수 Top 5')

        # # 그래프 출력
        # st.plotly_chart(fig)
        request_model_result(wallet_exist_model_input)

        #return wallet_exist_model_input




@st.cache_data(experimental_allow_widgets=True, ttl=120)
def wallet_no_exist():
    print(f'reload :{time.time}')
    
    list_image_choice = make_df()

    st.write("본인 소유의 지갑이 없다면 원하는 컬렉션을 선택해주세요")

    con = st.container()
    con.caption("원하는 컬렉션 3개를 선택하세요")

    selected_items = random.choices(list_image_choice, k=5)


    selected_collections = [item["collectionName"] for item in selected_items]
    #selected_contracts = [item["address"] for item in selected_items]
    selected_images = [item["imageUrl"] for item in selected_items]

    col1, col2, col3, col4, col5 = st.columns(5)

    
    with col1:
        st.image(selected_images[0],
        width=150, use_column_width=True, caption='1) '+selected_collections[0])

    with col2:
        st.image(selected_images[1], 
        width=150, use_column_width=True, caption='2) '+selected_collections[1])
        

    with col3:
        st.image(selected_images[2], 
        width=150, use_column_width=True, caption='3) '+selected_collections[2])


    with col4:
        st.image(selected_images[3],
        width=150, use_column_width=True, caption='4) '+selected_collections[3])


    with col5:
        st.image(selected_images[4], 
        width=150, use_column_width=True, caption='5) '+selected_collections[4])


    return selected_collections


def input_selection(selected_collections):

    # 지갑 주소
    model_input_contract_address = []

    selected_num = st.multiselect(
        '원하는 컬렉션을 선택해주세요',
        [1, 2, 3, 4, 5], max_selections=3)
    print(selected_num)
    if len(selected_num) == 3:
        for num in selected_num:
                model_input_contract_address.append(selected_collections[int(num)-1])
        if st.button("선택 완료"):
            print(model_input_contract_address)
            st.write('You selected: ')

            st.write(", ".join(model_input_contract_address))

            # for i in model_input_contract_address:
            #     st.write(i)
            # lambda(x:st.write(x), model_input_contract_address)

            
            request_model_result(model_input_contract_address)

            #return model_input_contract_address


def request_model_result(model_input):
    # 요청을 보낼 URL 및 헤더 설정
    url = 'http://127.0.0.1:8000/recsys/predict'
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    # POST 요청에 보낼 데이터 설정
    data = {
        "collections": model_input
    }

    # POST 요청 보내기
    response = requests.post(url, headers=headers, json=data)  

    # 응답 확인
    if response.status_code == 200:
        # 성공적인 응답 처리
        print("응답 데이터:", response.json())
        recommendation_result = response.json()['recommendations']

        st.write('Recommendation results: ')
        st.write(", ".join(recommendation_result))
        # for i in recommendation_result:
        #     st.write(i)
        
        return recommendation_result
    
    else:
        # 에러 처리
        print("에러 응답 코드:", response.status_code)
        print("에러 응답 내용:", response.text)
        return response.status_code
    



if __name__ == '__main__':
    recommend()

    tab1, tab2 = st.tabs(["지갑 있는 경우", "지갑 없는 경우"])

    with tab1:
        # wallet_exist_model_input = wallet_exist()
        wallet_exist()
        # print(wallet_exist_model_input)
        #request_model_result(wallet_exist_model_input)

    with tab2:
        selected_collections = wallet_no_exist()

        # model_input_contract_address = input_selection(selected_collections)
        input_selection(selected_collections)

        #request_model_result(model_input_contract_address)


