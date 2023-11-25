import pandas as pd
from sqlalchemy import create_engine
import os
from sqlalchemy.dialects.postgresql import insert
from connection import warehouse_connection

def load_data(table_file, table_name, key):
    
    warehouse_connection()
    db_url = os.environ['DB_URL']
    engine = create_engine(db_url)
    
    def insert_on_conflict_nothing(table, engine, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=[key])
        result = engine.execute(stmt)
        return result.rowcount

    pd.read_parquet(table_file).to_sql(table_name, engine, if_exists="replace", index=False)

def load_fact_data(table_file, table_name):
    
    db_url = os.environ['DB_URL']
    engine = create_engine(db_url)

    pd.read_parquet(table_file).to_sql(table_name, engine, if_exists="replace", index=False)
    print(table_name+" loaded!")