from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
from sqlalchemy import URL
import re
from dataclasses import dataclass

@dataclass(order=True)
class DataCleaning():
    '''A class used to perform all data cleaning jobs.
    
    Attributes
    ----------
    _data_con : DatabaseConnector
            Used to connect to remote and local databases.
    _data_ext : DataExtractor
            Used to extract data that is needed to be cleaned.
            
    Methods
    ----------
    clean_user_data()
            Gets all user data from AWS DB, cleans and returns a pandas dataframe.
    clean_card_data()
            Gets all card data from an AWS S3 Bucket and returns a pandas dataframe.
    clean_store_data()
            Gets store data by iteratively hitting API endpoint for all store data and returns a pandas dataframe.
    convert_product_weights()
            Performs advanced data cleaning in order to convert weight values to a single unit.
    clean_products_data()
            Gets all product data from an AWS S3 Bucket and returns a pandas dataframe.
    clean_orders_data()
            Gets all order data from AWS DB, cleans and returns a pandas dataframe.
    clean_date_events_data()
            Gets all date data from an AWS S3 Bucket and returns a pandas dataframe.
    '''
    _data_con: DatabaseConnector
    _data_ext: DataExtractor 

    @property
    def data_con(self) -> DatabaseConnector:
        return self._data_con
    
    @property
    def data_ext(self) -> DataExtractor:
        return self._data_ext


    def clean_user_data(self):
        """ Returns a pandas dataframe representative of user data from AWS DB"""
        raw_user_data = self.data_ext.read_rds_table(self.data_con,"legacy_users")
        # Map weird values to consistent values
        mapping = {"GGB" : "GB"}
        raw_user_data["country_code"] = raw_user_data["country_code"].replace(mapping)
        #A way to remove the invalid country code rows
        valid_countries = ["GB", "DE", "US"]
        inconsistent_country_codes = set(raw_user_data["country_code"].unique()).difference(valid_countries)
        inconsistent_rows = raw_user_data["country_code"].isin(inconsistent_country_codes)
        raw_user_data = raw_user_data[~inconsistent_rows]
        #convert to date time
        raw_user_data["date_of_birth"] = pd.to_datetime(raw_user_data["date_of_birth"], errors='coerce')
        raw_user_data["join_date"] = pd.to_datetime(raw_user_data["join_date"], errors='coerce')
        return raw_user_data
    
    def clean_card_data(self):
        """ Returns a pandas dataframe representative of card data from AWS S3 Bucket"""
        card_df = self.data_ext.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        # drop duplicates
        card_df = card_df.drop_duplicates()
        # strip non numeric aspects of card number
        card_df["card_number"] = card_df["card_number"].astype("string").str.extract('(\d+)', expand=False)
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
        """ Returns a pandas dataframe representative of store data from API endpoint"""
        store_df = self.data_ext.retrieve_stores_data("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details",{"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
        # remove invalid country codes
        valid_countries = ["GB", "DE", "US"]
        inconsistent_country_codes = set(store_df["country_code"].unique()).difference(valid_countries)
        inconsistent_rows = store_df["country_code"].isin(inconsistent_country_codes)
        store_df = store_df[~inconsistent_rows]
        # map incorrect continent category
        mapping = {"eeEurope": "Europe", "eeAmerica":"America"}
        store_df["continent"] = store_df["continent"].replace(mapping) 
        # strip non numeric aspects of staff numbers
        store_df["staff_numbers"] = store_df["staff_numbers"].astype("string").apply(lambda x: ''.join((re.findall(r"(\d+)",x)))) 
        store_df = store_df.dropna(subset=["opening_date"])        
        return store_df
    
    def convert_product_weights(self, df):
        """ Cleans the weights column of the product dataframe, in order to give correct values. """
        #create masks to filter between kg and non kg values
        kg_mask = (df["weight"].str.endswith("kg"))
        not_kg_mask = (~kg_mask)
        #create filtered subsets
        not_kg_valid = df[not_kg_mask]
        kg_valid = df[kg_mask]        
        #use loc to perform necessary operations 
        df.loc[not_kg_mask, "weight"] = not_kg_valid["weight"].str.replace("x","*").str.replace(" ","").apply(lambda x: eval(re.match(r"(\d+[\*]\d+)|(\d+)",x)[0])/1000)        
        df.loc[kg_mask, "weight"] = kg_valid["weight"].str.replace("kg","")        
        #convert to float
        df["weight"] = df["weight"].astype("float64") 
        return df
    
    def clean_products_data(self):
        """ Returns a pandas dataframe representative of product data from AWS S3 Bucket"""        
        df = pd.read_csv("s3://data-handling-public/products.csv")
        # drop null values
        df = df.dropna()
        #remove incorrect categories
        valid_categories = ["homeware","toys-and-games","food-and-drink","pets","sports-and-leisure","health-and-beauty","diy"]
        inconsistent_categories = set(df["category"].unique()).difference(valid_categories)
        inconsistent_rows = df["category"].isin(inconsistent_categories)
        df = df[~inconsistent_rows]
        df = self.convert_product_weights(df)
        df["date_added"] = pd.to_datetime(df["date_added"], errors="coerce")
        return df
    
    def clean_orders_data(self):
        """ Returns a pandas dataframe representative of order data from AWS DB"""     
        orders_df = self.data_ext.read_rds_table(self.data_con,"orders_table")
        orders_df = orders_df.drop(["first_name", "last_name", "1"], axis=1)
        return orders_df
    
    def clean_date_events_data(self):
        """ Returns a pandas dataframe representative of date event data from AWS S3"""  
        df = self.data_ext.get_request_content_as_json("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
        #remove non numeric values
        df = df[pd.to_numeric(df['day'], errors='coerce').notnull()]
        return df    
    
    if __name__=='__main__':
        print(__doc__)
