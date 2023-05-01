'''A class to help with cleaning data from a respective data source.'''
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
from sqlalchemy import URL

class DataCleaning:
    def clean_user_data(self):
        data_con = DatabaseConnector()
        data_ext = DataExtractor()
        raw_user_data = data_ext.read_rds_table(data_con,"legacy_users")

        #A way to remove the invalid country code rows
        valid_countries = ["GB", "DE", "US"]
        inconsistent_country_codes = set(raw_user_data["country_code"].unique()).difference(valid_countries)
        inconsistent_rows = raw_user_data["country_code"].isin(inconsistent_country_codes)
        raw_user_data = raw_user_data[~inconsistent_rows]

        #convert to date time
        raw_user_data["date_of_birth"] = pd.to_datetime(raw_user_data["date_of_birth"], errors='coerce')
        raw_user_data["join_date"] = pd.to_datetime(raw_user_data["join_date"], errors='coerce')

        #remove duplicated data
        #raw_user_data = raw_user_data.drop_duplicates()
        #raw_user_data.duplicated(["first_name","last_name","address"]).sum()

        return raw_user_data
    
con = DatabaseConnector()
clean = DataCleaning()
data = clean.clean_user_data() 
cred_url = URL.create(
                        "postgresql+psycopg2",
                        username="postgres",
                        password="postgres",
                        host="localhost",
                        database="sales_data",
                        port="5432"
                    )
con.upload_to_db(data, "dim_users", cred_url)