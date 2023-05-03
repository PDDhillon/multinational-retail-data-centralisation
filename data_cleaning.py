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
    
    def clean_card_data(self):
        data_ext = DataExtractor()
        card_df = data_ext.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

        # drop duplicates
        card_df = card_df.drop_duplicates()

        # Map weird values to consistent values
        mapping = {"VISA 16 digit" : "Visa", "VISA 13 digit" : "Visa", "VISA 19 digit" : "Visa", "JCB 16 digit" : "JCB",  "JCB 15 digit" : "JCB"}
        card_df["card_provider"] = card_df["card_provider"].replace(mapping)

        #remove incorrect data
        valid_cards = ["Visa", "JCB", "Diners Club / Carte Blanche", "American Express","Maestro","Discover", "Mastercard"]
        inconsistent_cards = set(card_df["card_provider"].unique()).difference(valid_cards)
        inconsistent_rows = card_df["card_provider"].isin(inconsistent_cards)
        card_df = card_df[~inconsistent_rows]

        card_df["date_payment_confirmed"] = pd.to_datetime(card_df["date_payment_confirmed"], errors="coerce")

        return card_df
    
    def clean_store_data(self):
        ext = DataExtractor()
        #store_df = ext.retrieve_stores_data("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details",{"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
        store_df = pd.read_csv("tempdata.csv")
        print(store_df.info())

        valid_countries = ["GB", "DE", "US"]
        inconsistent_country_codes = set(store_df["country_code"].unique()).difference(valid_countries)
        inconsistent_rows = store_df["country_code"].isin(inconsistent_country_codes)
        store_df = store_df[~inconsistent_rows]

        mapping = {"eeEurope": "Europe", "eeAmerica":"America"}
        store_df["continent"] = store_df["continent"].replace(mapping) 

        store_df["opening_date"] = pd.to_datetime(store_df["opening_date"], errors="coerce")

        return store_df

    
# con = DatabaseConnector()
# clean = DataCleaning()
# data = clean.clean_store_data() 
# cred_url = URL.create(
#                         "postgresql+psycopg2",
#                         username="postgres",
#                         password="postgres",
#                         host="localhost",
#                         database="sales_data",
#                         port="5432"
#                     )
# con.upload_to_db(data, "dim_store_details", cred_url)

