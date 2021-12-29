#!/mnt/c/Users/tienl/Udacity_Courses/DE_capstone/capstone_venv/bin/python3.8
import psycopg2
from dotenv import load_dotenv
import os
from pathlib import Path
from create_table_sql import drop_table_queries, create_table_queries, copy_table_queries

#Dynamic Environment variables
load_dotenv()
HOST = os.getenv('DWH_ENDPOINT')
DWH_ROLE_ARB=os.getenv('DWH_ROLE_ARB')

#Local and Static Environment Variables
my_path = Path('/mnt/c/Users/tienl/Udacity_Courses/DE_capstone/.env')
load_dotenv(my_path)
localhost=os.getenv('localhost')
local_dbname=os.getenv('local_dbname')
local_password=os.getenv('local_password')
local_username=os.getenv('local_username')
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
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


def load_staging_tables(cur, conn):
    """
    This function load the staging tables into our DW
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Using another files with all connection string to connect into DW cluster
    """
    try:
        
        conn = psycopg2.connect(f"host={HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}")
        # conn = psycopg2.connect(f"host={localhost},database={local_dbname},user={local_username},password={local_password}")
        
        cur = conn.cursor()
        
        drop_tables(cur, conn)
        print("\n\nSucessfully DROP tables!")
        
        create_tables(cur, conn)
        print("\n\nSucessfully CREATE new tables!\n")
        
        load_staging_tables(cur,conn)
        print("\n\nSucessfully COPY new tables!\n")
        conn.close()
        
    except Exception as e:
        if e == e:
            print('Fail to create or drop tables!')
            print(f'\n{e}')

if __name__ == "__main__":
    main()
