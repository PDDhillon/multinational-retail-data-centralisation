'''A class used to connect and upload data to the database'''
import yaml
from yaml.loader import SafeLoader
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd

class DatabaseConnector():
    def read_db_creds(self, filepath):
        with open(filepath) as f:
            data = yaml.load(f, Loader=SafeLoader)
            return data
    
    def init_db_engine(self, cred_url):        
        engine = create_engine(cred_url)
        return engine
    
    def list_db_tables(self, cred_url):
        engine = self.init_db_engine(cred_url)
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, df : pd.DataFrame, table_name, url):
        engine = self.init_db_engine(url)
        df.to_sql(name=table_name,con=engine,index=False,if_exists="replace")

