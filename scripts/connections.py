from sqlalchemy import create_engine
from dotenv import load_dotenv
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
    def engine(self):
        driver = '{ODBC Driver 17 for SQL Server}'
        vars = self.load_env_vars()
        str_connect = f'mssql+pyodbc://{vars['msql_user']}:{vars['msql_pwd']}@{vars['server_ip']}/{vars['db']}?driver=ODBC Driver 17 for SQL Server'
        print(str_connect)
        #cnxn = pyodbc.connect(str_connect)
        cnxn = create_engine(str_connect)
        
        return cnxn
    
if __name__=='__main__':
    cnxn = connections().engine()
    df = pd.read_sql("select name from sys.databases",con=cnxn)
    print(df)
    