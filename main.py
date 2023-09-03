from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy import text
import streamlit as st
from datetime import datetime, timedelta

# @st.cache(persist=True)
# def init_connection():
    
#     block = SqlAlchemyConnector.load("gcp-mlops-sql-postgres")
#     engine = block.get_engine()
#     connection = engine.connect()

    # return engine

def main():
    st.set_page_config(
        page_title = "고래 데이터 기반 NFT 추천 시스템",
        page_icon = "🐳"
    )

    st.title("고래 데이터 기반 NFT 추천 시스템")

    st.sidebar.success("☝️ 원하는 메뉴를 선택하세요")    
    

    d = st.date_input("원하는 날짜를 선택하세요", datetime.today()- timedelta(1))
    st.write('Your birthday is:', d)


if __name__ == '__main__':
    main()
