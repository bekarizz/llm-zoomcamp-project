import os, pandas as pd
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://rag:ragpass@localhost:5432/ragdb")

def run_sql(sql: str) -> pd.DataFrame:
    eng = create_engine(DB_URL, pool_pre_ping=True)
    with eng.begin() as con:
        con.execute(text("SET statement_timeout='5000';"))  # 5s
        return pd.read_sql_query(text(sql), con)
