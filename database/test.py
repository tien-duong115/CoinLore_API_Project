#!/mnt/c/Users/tienl/Udacity_Courses/DE_capstone/capstone_venv/bin/python3.8
from dotenv import load_dotenv
from pathlib import Path
import os



# p_2 = Path('database/.env')
#Dynamic Environment variables
load_dotenv()
HOST = os.getenv('DWH_ENDPOINT')
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DWB_DB_USER")
DB_PASSWORD = os.getenv("DWB_DB_PASSWORD")
DB_PORT = os.getenv("DWH_PORT")
S3_BUCKET_NAME=os.getenv('S3_BUCKET_NAME')
print('\n\n')
print(os.getenv('env_2'))
print('\n\n')
print('\n\n')

#Local and Static Environment Variables
my_path = Path('/mnt/c/Users/tienl/Udacity_Courses/DE_capstone/.env')
load_dotenv(my_path)
localhost=os.getenv('localhost')
local_dbname=os.getenv('local_dbname')
local_password=os.getenv('local_password')
local_username=os.getenv('local_username')
print('\n\n')
print(os.getenv('env_1'))
print(local_dbname)
print(os.getenv('env_2'))
print('\n\n')

