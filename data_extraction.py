from database_utils import DatabaseConnector
from sqlalchemy import URL
import pandas as pd
import tabula
import requests
import json
import io

class DataExtractor:     
    '''A utility class that helps in extracting data from different data source.    
            
    Methods
    ----------
    read_rds_table(data_con : DatabaseConnector, table_name="")
            Reads and returns a pandas dataframe for a given table name.
    retrieve_pdf_data(pdf_link="")
            Reads and returns a pandas dataframe from a given PDF link.
    list_number_of_stores(stores_endpoint="", headers={})
            Querys an API endpoint to get the total number of stores.
    retrieve_stores_data(endpoint="", headers={})
            Retrieves all store data by iteratively calling an API endpoint to get the data.
    extract_from_s3(s3_address="")
            Uses a pandas method in order to return a dataframe retrieved from an AWS S3 bucket
    get_request_content(address="")
            Calls and API endpoint and returns a pandas dataframe.
    '''
    def read_rds_table(self, data_con : DatabaseConnector, table_name):
        """ Reads and returns a pandas dataframe for a given table name. """  
        creds = data_con.read_db_creds('db_creds.yaml')
        cred_url = URL.create(
                        "postgresql+psycopg2",
                        username=creds["RDS_USER"],
                        password=creds["RDS_PASSWORD"],
                        host=creds["RDS_HOST"],
                        database=creds["RDS_DATABASE"],
                        port=creds["RDS_PORT"]
                    )
        engine = data_con.init_db_engine(cred_url)
        tables_available = data_con.list_db_tables(cred_url)
        if(table_name in tables_available):
            return pd.read_sql_table(table_name,engine)
        else:
            return None
        
    def retrieve_pdf_data(self, pdf_link):
        """ Reads and returns a pandas dataframe from a given PDF link. """  
        df = pd.concat(tabula.read_pdf(pdf_link, pages="all",multiple_tables=True))
        return df
    
    def list_number_of_stores(self, stores_endpoint, headers):
        """ Querys an API endpoint to get the total number of stores. """  
        # 451 stores required
        response = requests.get(url=stores_endpoint, headers=headers)
        return response
    
    def retrieve_stores_data(self, endpoint, headers):
        """ Retrieves all store data by iteratively calling an API endpoint to get the data. """  
        data = []
        for x in range(451):
            response = requests.get(url=f'{endpoint}/{x}', headers=headers)
            bytes = response.content
            data.append(json.loads(bytes.decode("utf-8")))

        df =pd.DataFrame.from_records(data)
        return df
    
    def extract_from_s3(self, s3_address):
        """ Uses a pandas method in order to return a dataframe retrieved from an AWS S3 bucket """  
        data = pd.read_csv(s3_address)
        return data
    
    def get_request_content_as_json(self, address):
        """ Calls and API endpoint and returns a pandas dataframe. """  
        response = requests.get(address).content
        df = pd.read_json(io.StringIO(response.decode('utf-8')))
        return df
    
    if __name__=='__main__':
        print(__doc__)
    
    
