import psycopg2
from dotenv import load_dotenv
import os
from pathlib import Path
from create_table_sql import drop_table_queries, create_table_queries

dotenv_path = Path('.env_db')
load_dotenv(dotenv_path)


HOST = os.getenv('DWH_ENDPOINT')
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DWB_DB_USER")
DB_PASSWORD = os.getenv("DWB_DB_PASSWORD")
DB_PORT = os.getenv("DWH_PORT")
S3_BUCKET_NAME=os.getenv('S3_BUCKET_NAME')


def drop_tables(cur, conn):
    """
    This function drop all tables if already exists
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function creates table if not exists
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Using another files with all connection string to connect into DW cluster
    """
    try:
        conn = psycopg2.connect(f"host={HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}")
        cur = conn.cursor()
        drop_tables(cur, conn)
        create_tables(cur, conn)
        conn.close()
    except Exception as e:
        if e == e:
            print('Fail to create or drop tables!')
            print(f'\n\n{e}')

if __name__ == "__main__":
    main()