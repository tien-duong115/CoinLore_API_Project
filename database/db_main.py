#!/mnt/c/Users/tienl/Udacity_Courses/DE_capstone/capstone_venv/bin/python3.8
import psycopg2
from dotenv import load_dotenv
import os
from pathlib import Path
from create_table_sql import drop_table_queries, create_table_queries, copy_table_queries, insert_table_queries

exe_path = '/mnt/c/Users/tienl/Udacity_Courses/DE_capstone/database'
os.chdir(exe_path)
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
        print(f"\nSucessfully DROPPED \n{query}!\n")
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
        
def insert_tables(cur, conn):
    """
    This function insert data into the tables in the DW
    """
    for query in insert_table_queries:
        print(f'TRYING INSERT AT: \n{query}')
        cur.execute(query)
        conn.commit()


def main():
    """
    Using another files with all connection string to connect into DW cluster
    """
    ### Redshift database connection
    conn = psycopg2.connect(f"host={HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}")
    
    ### Local postgres database connection
    # conn = psycopg2.connect(host=f'{localhost}',database=f'{local_dbname}',user=f'{local_username}',password=f'{local_password}')
    cur = conn.cursor()
    try:
        drop_tables(cur, conn)
    except Exception as e:
        print(f'1: \n{e}')
    try:      
        create_tables(cur, conn)
    except Exception as e:
        print(f'2: \n{e}')
    try:        
        load_staging_tables(cur,conn)
        print("\n\nSucessfully COPY new tables!\n")
    except Exception as e:
        print(f'3: \n{e}')
    try:
        insert_tables(cur,conn)
        print("Sucessfully inserted!")
    except Exception as e:
        print(f'4: \n{e}')

    
    conn.close()
    

if __name__ == "__main__":
    main()
