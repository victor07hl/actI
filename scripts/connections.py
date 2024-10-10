from sqlalchemy import create_engine
from dotenv import load_dotenv
import pyodbc
import pandas as pd
import os
class connections:
    def __init__(self) -> None:
        pass

    def load_env_vars(self):
        load_dotenv()
        return {'msql_user':os.getenv('msql_user'),
                'msql_pwd':os.getenv('msql_pwd'),
                'server_ip':os.getenv('server_ip'),
                'db':os.getenv('db')}
    def engine(self,db):
        vars = self.load_env_vars()
        str_connect = f'mssql+pyodbc://{vars['msql_user']}:{vars['msql_pwd']}@{vars['server_ip']}/{db}?driver=ODBC Driver 17 for SQL Server'
        cnxn = create_engine(str_connect)
        
        return cnxn
    
    def engine_pyodbc(self):
        driver = '{ODBC Driver 17 for SQL Server}'
        vars = self.load_env_vars()
        str_connect_odbc = f'DRIVER={driver};Server={vars["server_ip"]};Database={vars["db"]};Port=1433;UID={vars["msql_user"]};PWD={vars["msql_pwd"]};Encrypt=no'
        conn_pyodbc = pyodbc.connect(str_connect_odbc)
        conn_pyodbc.autocommit = True

        return conn_pyodbc
    
class sql_management(connections):
    def __init__(self):
        super().__init__()

    def create_database(self,db):
        conn = self.engine_pyodbc()
        cursor = conn.cursor()
        try:
            cursor.execute(f"create database {db}")
            cursor.close()
        except pyodbc.ProgrammingError as e:
            if f"Database '{db}' already exists" in str(e):
                print(f"Database '{db}' already exists")
            else:
                print(e)

    def create_schema(self,database,schema):
        conn = self.engine_pyodbc()
        cursor = conn.cursor()
        try:
            cursor.execute(f"use {database}")
            cursor.execute(f"create schema {schema}")
            cursor.close()
        except pyodbc.ProgrammingError as e:
            if f"'{schema}' in the database" in str(e):
                print(f"Schema '{schema}' already exists in Database '{database}'")
            else:
                print(e)

    def execute_custom_query(self,query):
        conn = self.engine_pyodbc()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            cursor.close()
        except pyodbc.ProgrammingError as e:
            print(e)
        



    
    
if __name__=='__main__':
    sql_m = sql_management()
    sql_m.create_database(db='dwh')
    sql_m.create_schema('dwh','terrazas')
    cnxn = connections().engine(db = 'dwh')
    df = pd.read_sql('''
                    SELECT 
                    TABLE_SCHEMA, 
                    TABLE_NAME
                FROM 
                    INFORMATION_SCHEMA.TABLES
                WHERE 
                    TABLE_TYPE = 'BASE TABLE' 
                    AND TABLE_SCHEMA = 'terrazas';

                    ''',con=cnxn)

    print(df)
    