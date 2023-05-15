import yaml
from yaml.loader import SafeLoader
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd

class DatabaseConnector():
    '''A class used to connect and download/upload data to/from the database.    
            
    Methods
    ----------
    read_db_creds(filepath="")
            Reads and return database credentials from a YAML file. Returns a dictionary.
    init_db_engine(cred_url="")
            Initialises and returns a database engine for a given set of credentials.
    list_db_tables(cred_url="")
            Lists all the tables that live in a database for a given set of credentials.
    upload_to_db(df : pd.DataFrame, table_name="", url="")
            Uploads a pandas dataframe to a SQL table for a given table name.
    '''
    def read_db_creds(self, filepath):
        """ Reads and return database credentials from a YAML file. Returns a dictionary. """  
        with open(filepath) as f:
            data = yaml.load(f, Loader=SafeLoader)
            return data
    
    def init_db_engine(self, cred_url):        
        """ Initialises and returns a database engine for a given set of credentials. """  
        engine = create_engine(cred_url)
        return engine
    
    def list_db_tables(self, cred_url):
        """ Lists all the tables that live in a database for a given set of credentials. """  
        engine = self.init_db_engine(cred_url)
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, df : pd.DataFrame, table_name, url):
        """ Uploads a pandas dataframe to a SQL table for a given table name. """  
        engine = self.init_db_engine(url)
        df.to_sql(name=table_name,con=engine,index=False,if_exists="replace")
    
    if __name__=='__main__':
        print(__doc__)

