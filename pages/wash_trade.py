import streamlit as st
import pandas as pd
import plotly.express as px
import time
import os
import sys

repo_dir = os.path.abspath(__file__).split('/streamlit')[0]
print(repo_dir)
sys.path.append(f'{repo_dir}')

from utils import get_sql_engine


# DB에서 6월의 blacklist 데이터 불러와서 dataframe 만드는 함수
def load_june_wash_trade_data_from_db(engine):

    june_wash_trade_query = """
                            WITH first_tb AS (
                                SELECT * 
                                FROM 
                                    dune_nft_trades
                                WHERE
                                    block_time 
                                        BETWEEN DATE('2023-06-01') AND DATE('2023-06-30')
                            ), second_tb AS (
                                SELECT *
                                FROM
                                    first_tb
                                WHERE seller IN (
                                        SELECT 
                                            participant 
                                        FROM 
                                            black_list 
                                        WHERE
                                            written_date = DATE('2023-06-01'))
                                    OR buyer IN (
                                        SELECT
                                            participant 
                                        FROM
                                            black_list 
                                        WHERE 
                                            written_date = DATE('2023-06-01'))
                            )

                            SELECT * 
                            FROM second_tb
                            ;
                            """
    
    june_wash_trade_df = pd.read_sql(june_wash_trade_query, engine)

    return june_wash_trade_df


# DB에서 6월의 전체 trade 데이터 불러와서 dataframe 만드는 함수
def load_june_trade_data_from_db(engine):

    june_trade_query = """
                        SELECT *
                        FROM 
                            dune_nft_trades
                        WHERE block_time 
                                BETWEEN DATE('2023-06-01') AND DATE('2023-06-30')
                        ;
                        """
    
    june_trade_df = pd.read_sql(june_trade_query, engine)

    return june_trade_df


# DB에서 7월의 wash trade 데이터 불러와서 dataframe 만드는 함수
def load_july_wash_trade_data_from_db(engine):

    july_wash_trade_query = """
                            WITH first_tb AS (
                                SELECT * 
                                FROM 
                                    dune_nft_trades
                                WHERE block_time 
                                    BETWEEN DATE('2023-07-01') AND DATE('2023-07-31')
                            ), second_tb AS (
                                SELECT *
                                FROM 
                                    first_tb
                                WHERE seller NOT IN (
                                        SELECT 
                                            participant 
                                        FROM 
                                            black_list
                                        WHERE written_date = DATE('2023-06-01')
                                        )
                                    AND buyer NOT IN (
                                        SELECT 
                                            participant 
                                        FROM 
                                            black_list 
                                        WHERE written_date = DATE('2023-06-01'))
                            ), black_tb AS (
                                SELECT *
                                FROM 
                                    second_tb
                                WHERE seller IN (
                                        SELECT 
                                            participant 
                                        FROM black_list 
                                        WHERE written_date = DATE('2023-07-01'))
                                    OR buyer IN (
                                        SELECT 
                                            participant 
                                        FROM black_list 
                                        WHERE written_date = DATE('2023-07-01'))
                            )

                            SELECT * 
                            FROM black_tb;
                            """
    
    july_wash_trade_df = pd.read_sql(july_wash_trade_query, engine)

    return july_wash_trade_df


# DB에서 7월의 전체 trade 데이터 불러와서 dataframe 만드는 함수
def load_july_trade_data_from_db(engine):

    july_trade_query = """
                        WITH first_tb AS (
                            SELECT * 
                            FROM 
                                dune_nft_trades
                            WHERE block_time 
                                BETWEEN '2023-07-01' AND '2023-07-31'
                        ), second_tb AS (
                            SELECT *
                            FROM 
                                first_tb
                            WHERE seller NOT IN (
                                SELECT 
                                    participant 
                                FROM 
                                    black_list 
                                WHERE written_date = '2023-06-01'
                                )
                                AND buyer NOT IN (
                                    SELECT 
                                        participant 
                                    FROM 
                                        black_list 
                                    WHERE written_date = '2023-06-01'
                                )
                        )

                        SELECT * 
                        FROM second_tb;
                        """
    
    july_trade_df = pd.read_sql(july_trade_query, engine)

    return july_trade_df


# DB에서 collection name 불러와서 dataframe 만드는 함수
def load_metadata_from_db(engine):

    metadata_query = """
                        SELECT address
                                , name  
                        FROM 
                            dune_nft_metadata
                        ; 
                    """
    
    metadata_df = pd.read_sql(metadata_query, engine)
    
    return metadata_df


# 불러온 metadata 전처리하는 함수
def preprocess_metadata_df(metadata_df):
    # address 중복 제거
    metadata_df = metadata_df.drop_duplicates(['address'])

    return metadata_df


# 전체 trade 대비 wash trade 비율을 pie chart로 나타내는 함수
def make_pie_chart_for_wash_trade(trade_df, wash_trade_df):

    percent_wash_trade_df = pd.DataFrame({ 'type':['정상 거래', '비정상 거래'],
                                            'amount':[(len(trade_df)-len(wash_trade_df)), len(wash_trade_df)]})

    fig = px.pie(percent_wash_trade_df, values='amount', names='type', title='전체 거래량 대비 비정상 비율', color_discrete_sequence=px.colors.sequential.Bluered_r)
    fig = fig.update_traces(textposition='inside', textinfo='percent+label+value+text')

    st.plotly_chart(fig)


# market별 거래량 dataframe 전처리 함수
def preprocess_trade_amount_data_of_market(trade_df, wash_trade_df):

    trade_market_counts = trade_df['market'].value_counts().reset_index()

    wash_trade_market_counts = wash_trade_df['market'].value_counts().reset_index()

    # block_time을 기준으로 데이터프레임 병합
    trade_count_df = pd.merge(trade_market_counts, wash_trade_market_counts, on='market', how='left')

    # null값을 0으로 대체 
    trade_count_df = trade_count_df.fillna(0)

    return trade_count_df


# market별 wash trade 거래 비율 찾아 그래프 그리는 함수
def make_bar_chart_for_wash_trade_amount(trade_count_df):
    # 비율 컬럼 만들기
    trade_count_df['비정상 거래 비율'] = trade_count_df['count_y'] / trade_count_df['count_x'] * 100
    
    trade_count_df['정상 거래 비율'] = (trade_count_df['count_x'] - trade_count_df['count_y']) / trade_count_df['count_x'] * 100

    trade_count_df = trade_count_df.sort_values(by='비정상 거래 비율', ascending=True)

    # 누적 막대 그래프 그리기
    trade_count_df.plot(kind='barh', figsize=(10,6), stacked=True, alpha=0.7)

    fig = px.bar(trade_count_df, x=['비정상 거래 비율', '정상 거래 비율'], y='market', title="market별 전체 거래량 대비 비정상 거래 비율", orientation='h', barmode='stack', color_discrete_sequence=px.colors.sequential.Bluered_r)
    
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)


# wash trade 데이터에 dune_nft_metadata의 collection name 연결하기
def connect_wash_trade_to_collection_name(wash_trade_df, metadata_df):
    wash_trade_metadata_df = pd.merge(wash_trade_df, metadata_df,'left', left_on='nft_contract_address', right_on='address')

    # wash_trade_metadata_df의 결측치 채우기
    wash_trade_metadata_df['address'] = wash_trade_metadata_df['address'].fillna('None')

    wash_trade_metadata_df['name'] = wash_trade_metadata_df['name'].fillna('None')

    wash_trade_metadata_df = wash_trade_metadata_df[wash_trade_metadata_df['address'] != 'None']

    wash_trade_metadata_df = wash_trade_metadata_df[wash_trade_metadata_df['name'] != 'None']

    return wash_trade_metadata_df


# wash trade 빈도순으로 collection Top 10의 거래량 그래프 그리기
def make_bar_chart_for_wash_trade_collection_amount(wash_trade_metadata_df):
    # 빈도 계산
    collection_freq_counts= wash_trade_metadata_df['name'].value_counts().reset_index()

    top_10_collection_freq = collection_freq_counts.head(10)

    top_10_collection_freq = top_10_collection_freq.sort_values(by='count', ascending=True)

    fig = px.bar(top_10_collection_freq, x="count", y="name", title="거래 빈도 상위 10개 collection", color='count',  color_continuous_scale = 'Bluered', orientation='h')
    
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)


@st.cache_resource
def load_whole_trade_data():
    engine = get_sql_engine()

    if os.path.isfile("june_trade_df.csv") == True:
        june_trade_df = pd.read_csv("june_trade_df.csv",sep=',')

    elif os.path.isfile("june_trade_df.csv") == False:
        june_trade_df = load_june_trade_data_from_db(engine)
        june_trade_df.to_csv("june_trade_df.csv",sep=',')
        june_trade_df = pd.read_csv("june_trade_df.csv",sep=',')

    if os.path.isfile("july_trade_df.csv") == True:
        july_trade_df = pd.read_csv("july_trade_df.csv",sep=',')

    elif os.path.isfile("july_trade_df.csv") == False:
        july_trade_df = load_july_trade_data_from_db(engine)
        july_trade_df.to_csv("july_trade_df.csv",sep=',')
        july_trade_df = pd.read_csv("july_trade_df.csv",sep=',')

    return june_trade_df, july_trade_df


@st.cache_resource
def load_abnormal_trade_data():
    engine = get_sql_engine()

    if os.path.isfile("june_wash_trade_df.csv") == True:
        june_wash_trade_df = pd.read_csv("june_wash_trade_df.csv",sep=',')

    elif os.path.isfile("june_wash_trade_df.csv") == False:
        june_wash_trade_df = load_june_wash_trade_data_from_db(engine)
        june_wash_trade_df.to_csv("june_wash_trade_df.csv",sep=',')
        june_wash_trade_df = pd.read_csv("june_wash_trade_df.csv",sep=',')

    if os.path.isfile("july_wash_trade_df.csv") == True:
        july_wash_trade_df = pd.read_csv("july_wash_trade_df.csv",sep=',')

    elif os.path.isfile("july_wash_trade_df.csv") == False:
        july_wash_trade_df = load_july_wash_trade_data_from_db(engine)
        july_wash_trade_df.to_csv("july_wash_trade_df.csv",sep=',')
        july_wash_trade_df = pd.read_csv("july_wash_trade_df.csv",sep=',')

    return june_wash_trade_df, july_wash_trade_df


@st.cache_resource
def load_metadata_df():
    engine = get_sql_engine()

    if os.path.isfile("metadata_df.csv") == True:
        metadata_df = pd.read_csv("metadata_df.csv",sep=',')
        
    elif os.path.isfile("metadata_df.csv") == False:
        metadata_df = load_metadata_from_db(engine)
        metadata_df = preprocess_metadata_df(metadata_df)
        metadata_df.to_csv("metadata_df.csv",sep=',')
        metadata_df = pd.read_csv("metadata_df.csv",sep=',')

    return metadata_df


# main 함수
def main_wash_trade():
    # 데이터 로드 시간 확인 위해 추가
    start_time = time.time()

    june_trade_df, july_trade_df = load_whole_trade_data()

    june_wash_trade_df, july_wash_trade_df = load_abnormal_trade_data()

    metadata_df = load_metadata_df()

    end_time = time.time()

    print(f"로드 소요 시간 : {end_time-start_time}")

    # 월별로 그래프 보여주기 위해 탭 분리
    tab1, tab2 = st.tabs(["6월", "7월"])

    with tab1:
        # 제목
        st.header('6월 비정상 거래 현황', divider='rainbow')
        make_pie_chart_for_wash_trade(june_trade_df, june_wash_trade_df)

        st.subheader('거래 주의 collection', divider='red')
        june_wash_trade_metadata_df = connect_wash_trade_to_collection_name(june_wash_trade_df, metadata_df)
        make_bar_chart_for_wash_trade_collection_amount(june_wash_trade_metadata_df)


        st.subheader('거래 주의 market', divider='red')
        june_trade_count_df = preprocess_trade_amount_data_of_market(june_trade_df, june_wash_trade_df)
        make_bar_chart_for_wash_trade_amount(june_trade_count_df)

    with tab2:
        st.header('7월 비정상 거래 현황', divider='rainbow')
        st.write('6월에 비정상 거래를 했던 지갑 주소들을 제외한 거래 내역')
        make_pie_chart_for_wash_trade(july_trade_df, july_wash_trade_df)

        st.subheader('거래 주의 collection', divider='red')
        july_wash_trade_metadata_df = connect_wash_trade_to_collection_name(july_wash_trade_df, metadata_df)
        make_bar_chart_for_wash_trade_collection_amount(july_wash_trade_metadata_df)

        st.subheader('거래 주의 market', divider='red')
        july_trade_count_df = preprocess_trade_amount_data_of_market(july_trade_df, july_wash_trade_df)
        make_bar_chart_for_wash_trade_amount(july_trade_count_df)


if __name__ == '__main__':
    # Page 제목
    st.title("월별 비정상 거래 현황")

    st.markdown("""### 비정상 거래
    자산의 가격이나 유동성에 대해 다른 시장 참가자들을 속이기 위해서 단기간에 동일한 자산을 매수 & 매도하거나
    시장을 조작하는 행위 등을 비정상 거래 행위로 정의한다.
    """)

    main_wash_trade()