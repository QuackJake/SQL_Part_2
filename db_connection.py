#.venv\Scripts\activate
import pyodbc, os
from dotenv import load_dotenv
from pathlib import Path

def create_connection():
    dotenv_path = Path('./logins.env')
    load_dotenv(dotenv_path=dotenv_path)

    SERVER = os.getenv('SERVER')
    DATABASE = os.getenv('DATABASE')
    USERNAME = os.getenv('DB_USER')
    PASSWORD = os.getenv('USER_PWD')

    connection_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;"
    )

    return pyodbc.connect(connection_string)