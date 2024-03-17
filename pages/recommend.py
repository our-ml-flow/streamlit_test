import streamlit as st
import pandas as pd
import requests
import random
import time
import os
import sys

repo_dir = os.path.abspath(__file__).split('/streamlit')[0]
print(repo_dir)
sys.path.append(f'{repo_dir}')

from utils import get_sql_engine


# API로 추천 모델 입력 송신 & 결과 수신
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

        st.markdown('## Recommendation results')

        st.write(", ".join(recommendation_result))
        
        return recommendation_result
    
    else:
        # 에러 처리
        print("에러 응답 코드:", response.status_code)

        print("에러 응답 내용:", response.text)

        return response.status_code


# wallet_exist인 경우, DB에서 입력된 지갑 주소의 데이터를 가져오는 함수
def load_wallet_exist_data(user_wallet_address):
    # 지갑 데이터 가져오는 쿼리
    search_wallet_query = """ 
                            SELECT 
                                    owner_address
                                    , collection_name
                                    , contract->>'address' AS address
                            FROM 
                                alchemy_collection_for_buyer        
                            WHERE 
                                owner_address = %(user_wallet_address)s            
                            ;
                            """   
    
    # DB 연결
    engine = get_sql_engine()

    params = {"user_wallet_address": user_wallet_address}

    wallet_info_df = pd.read_sql(search_wallet_query, engine, params=params)

    return wallet_info_df


# wallet_info_df에서 컬렉션을 가진 개수 기준으로 정렬한 후, Top 3를 리스트로 추출
def extract_top3_from_wallet_info(wallet_info_df):
    wallet_info_num_df = wallet_info_df.groupby('collection_name')[['collection_name', 'address']].size().reset_index(name='count')
    
    wallet_info_num_df = wallet_info_num_df.sort_values(by='count', ascending=False)
    
    wallet_exist_model_df = wallet_info_num_df.head(3)

    wallet_exist_model_input = list(wallet_exist_model_df['collection_name'])

    return wallet_exist_model_input


# DB에서 metadata 데이터 불러와서 dataframe 만드는 함수
def load_dune_metadata_from_db():
    engine = get_sql_engine()

    recommend_query = """
                        SELECT address
                                , "collectionName"
                                , "imageUrl"
                        FROM 
                            dune_nft_metadata
                        ;
                        """
    
    metadata_df = pd.read_sql(recommend_query, engine)

    return metadata_df


# metadata dataframe에서 wallet_no_exist일 경우 제시할 이미지 뽑기 위해 dataframe을 전처리 후, dictionary로 만드는 함수
def make_df_to_dict(metadata_df):
    metadata_df = metadata_df.dropna(subset=['imageUrl'], axis=0)

    metadata_df = metadata_df.drop_duplicates(['collectionName'])

    image_metadata_df = metadata_df[['collectionName', 'address', 'imageUrl']]

    image_dict = image_metadata_df.to_dict('records')

    return image_dict


# wallet_no_exist일 경우, image_dict에서 랜덤으로 5개 뽑아 선택지로 제시하는 함수
def make_image_choice(list_image_dict):
    items_options = random.choices(list_image_dict, k=5)

    collection_options = [item["collectionName"] for item in items_options]

    image_options = [item["imageUrl"] for item in items_options]

    return image_options, collection_options


@st.cache_data(experimental_allow_widgets=True)
def give_collection_options():
    print(f'reload :{time.time}')
    
    metadata_df = load_dune_metadata_from_db()

    list_image_dict = make_df_to_dict(metadata_df)

    image_options, collection_options = make_image_choice(list_image_dict)

    # 5개 선택지 제시
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.image(image_options[0],
        width=150, use_column_width=True, caption='1) '+collection_options[0])

    with col2:
        st.image(image_options[1], 
        width=150, use_column_width=True, caption='2) '+collection_options[1])

    with col3:
        st.image(image_options[2], 
        width=150, use_column_width=True, caption='3) '+collection_options[2])

    with col4:
        st.image(image_options[3],
        width=150, use_column_width=True, caption='4) '+collection_options[3])

    with col5:
        st.image(image_options[4], 
        width=150, use_column_width=True, caption='5) '+collection_options[4])

    return collection_options


# 원하는 컬렉션 3가지 선택 후 API로 결과 받기
def select_collection_options(collection_options):
    # 입력한 컬렉션 이름 리스트에 추가
    wallet_no_exist_model_input = []

    selected_num = st.multiselect(
        '원하는 컬렉션을 선택해주세요',
        [1, 2, 3, 4, 5], max_selections=3)
    
    print(selected_num)

    if len(selected_num) == 3:

        for num in selected_num:
                wallet_no_exist_model_input.append(collection_options[int(num)-1])

        if st.button("선택 완료"):
            print(wallet_no_exist_model_input)

            st.markdown('## Your collections')

            st.write(", ".join(wallet_no_exist_model_input))
            
            # request_model_result(wallet_no_exist_model_input)


# 지갑 주소가 있는 경우의 main 함수
def main_wallet_exist():
    user_wallet_address = st.text_input(label="User Wallet Address", placeholder="default value")
    
    # 지갑 주소가 입력되면 가진 컬렉션 조회해 추천 모델에 전달
    if st.button("입력"):
        wallet_info_df = load_wallet_exist_data(user_wallet_address)

        wallet_exist_model_input = extract_top3_from_wallet_info(wallet_info_df)

        st.markdown('## Your collections')

        st.write(", ".join(wallet_exist_model_input))

        # request_model_result(wallet_exist_model_input)


# 지갑 주소가 없는 경우의 main 함수
def main_wallet_no_exist():
    con = st.container()

    con.caption("원하는 컬렉션이 없다면 reload를 눌러주세요")

    if st.button("Reload"):
        st.cache_data.clear()

        collection_options = give_collection_options()

    else:
        collection_options = give_collection_options()

    select_collection_options(collection_options)


# main 함수
def main_recommend(): 
    tab1, tab2 = st.tabs(["지갑 있는 경우", "지갑 없는 경우"])

    with tab1:
        st.write("본인 소유의 지갑이 있다면 지갑 주소를 적어주세요")

        main_wallet_exist()

    with tab2:
        st.write("본인 소유의 지갑이 없다면 원하는 컬렉션 3개 선택해주세요")

        main_wallet_no_exist()



if __name__ == '__main__':
    # Page 제목
    st.title("NFT 기반 추천")

    main_recommend()