'''A utility class that helps in extracting data from different data source'''
from database_utils import DatabaseConnector
from sqlalchemy import URL
import pandas as pd
import tabula
import requests
import json

class DataExtractor: 
    def read_rds_table(self, data_con : DatabaseConnector, table_name):
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
        df = pd.concat(tabula.read_pdf(pdf_link, pages="all",multiple_tables=True))
        return df
    
    def list_number_of_stores(self, stores_endpoint, headers):
        # 451 stores required
        response = requests.get(url=stores_endpoint, headers=headers)
        return response
    
    def retrieve_stores_data(self, endpoint, headers):
        data = []
        for x in range(451):
            response = requests.get(url=f'{endpoint}/{x}', headers=headers)
            bytes = response.content
            data.append(json.loads(bytes.decode("utf-8")))

        df =pd.DataFrame.from_records(data)
        return df
        



