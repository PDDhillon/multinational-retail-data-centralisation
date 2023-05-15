# multinational-retail-data-centralisation

# Milestone 2:
The first aspect of any data analysis is data extraction and cleaning. In this milestone, I undertook arguably the most important task of the whole project. As without proper data extraction and cleaning, any future data analysis would be flawed. The data came in many forms and was stored inside of my "sales_data" database. 

All data was retrieved and processed inside of a pandas dataframe. Pandas is a brilliant way to perform data cleaning, due to the way data is indexed. This allows for very quick and easy filtering of subsets of data, as well as to perform operations on those dataframes.

The three main project classes were "DataExtractor", "DatabaseConnector" and "DataCleaning". All classes have self explanatory names. The database connector was used for direct connection to both AWS DB's and our DB. The DataExtractor was then the main class responsible for getting the data from the various data sources that we had at our disposal. Finally, the DataCleaning class was used to process and clean all the data that was required.

User and order data was extracted from an AWS DB. Using a yaml file containing credentials, I was able to connect to the AWS DB. Once cleaned, this data lived in the "dim_users" and "orders_table" tables in "sales_data".

Card details data was extracted from a PDF document and cleaned. This data was accessed using a module called tabula-py, which helped us access and then convert the data into a pandas dataframe.

Store data was iteratively accessed from an API endpoint. The endpoint would only return the data for a singular store, by the id of that store. Using another endpoint, I was able to get the total number of stores to work out how long my for loop had to hit the endpoint. The for loop called the endpoint 451 times, each time appending the dictionary result to a list. This list was then processed into a pandas dataframe. 

Product details were stored in a CSV file that lived in an AWS S3 bucket. This bucket was accessed using a module called boto3 and then the address was passed through to pandas to create the dataframe. Products data had some advanced data cleaning required, in order to convert the product weights. With the values having different units of measure, all units had to be converted to a single kg unit type. As well as that, some values contained expressions (2x200g). Utilising pythons eval function, I was able to succesfully convert these to the required format also. Date event data was also retrieved from a JSON file in an S3 bucket also.

Data cleaning was undertaken for all this data, with a variety of methods employed. After intial assessment of data using .info() and .describe(), duplicate data and null values were removed. Incorrect categories were mapped correctly. Incorrect data values were removed or updated to correct values.

# Milestone 3
Once the data was processed and cleaned, it had been inserted into the various tables that lived inside of my sales_data database. However, in many cases, the pandas dataframe had processed the majority of data as simply an object. This meant that without explicit datatypes, the SQL tables understood them to just be text. 

The next job was to map the datatypes of each column in each respective table. This was done so as to aid comprehension of the data when querying. For example, if we tried to do some analysis and filtering on dates, but the datatype was a string, there would be no easy way in order to do this. Data typing is a key part of allowing the data to be accessible for analysis. New derived columns were also created, as well as dropping uneccessary ones. 

Finally, primary and foreign keys were added to create the neccessary relations between tables. Again, this aided future analysis as we would then be able to join related data together. For example, the orders table contained foreign keys to all other tables. If we wanted to find out more information about the store that a product was bought in, we needed a way to link both store and product data to an order. Primary and foreign keys allows for these neccessary relations to exist. 

# Milestone 4
Now that the data was extracted, cleaned and living inside of a relational database, I could now begin the task of data analysis. Using SQL, I was able to perform queries in order to get various metrics and trends about this data. Below are screenshots of all the finding for each one of the questions asked about the data: