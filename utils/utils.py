from prefect_sqlalchemy import SqlAlchemyConnector

import streamlit as st


# DB 연결
@st.cache_resource
def get_sql_engine():
    block_name = "gcp-mlops-sql-postgres"

    block = SqlAlchemyConnector.load(block_name)

    engine = block.get_engine()

    return engine
