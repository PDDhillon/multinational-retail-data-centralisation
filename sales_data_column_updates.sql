/* orders table*/
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;

ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;

ALTER TABLE orders_table
ALTER COLUMN card_number TYPE varchar(50);

ALTER TABLE orders_table
ALTER COLUMN store_code TYPE varchar(20);

ALTER TABLE orders_table
ALTER COLUMN product_code TYPE varchar(20);

ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE smallint;

/* users table */
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE varchar(255);

ALTER TABLE dim_users
ALTER COLUMN last_name TYPE varchar(255);

ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE DATE;

ALTER TABLE dim_users
ALTER COLUMN country_code TYPE varchar(5);

ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;

ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE;

/* store details */
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE float8 USING longitude::float8;

ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE varchar(255);

ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE varchar(20);

ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE smallint USING staff_numbers::smallint;

ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE date;

ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE varchar(255);

ALTER TABLE dim_store_details
ALTER COLUMN store_type DROP NOT NULL;

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE float8 USING latitude::float8;

ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE varchar(5);

ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE varchar(255);

ALTER TABLE dim_store_details
DROP COLUMN lat;

update dim_store_details 
set address = 'N/A', locality = 'N/A'
where index = 0;

/* strip £ */
UPDATE dim_products SET product_price = REPLACE(product_price, '£', '');

/* add weight category */
ALTER TABLE dim_products 
ADD COLUMN weight_class VARCHAR(15);

update dim_products
set weight_class =
CASE 
when CAST(weight as FLOAT) < 2 THEN 'Light'
when CAST(weight as FLOAT) >= 2 AND CAST(weight as FLOAT) < 40 THEN 'Mid_Sized' 
when CAST(weight as FLOAT) >= 40 AND CAST(weight as FLOAT) < 140 THEN 'Heavy'
when CAST(weight as FLOAT) >= 140 THEN 'Truck_Required' END;

/* rename removed to still_available */
ALTER TABLE dim_products RENAME COLUMN removed TO still_available;

UPDATE dim_products
SET still_available =
CASE WHEN still_available = 'Still_avaliable' THEN TRUE
WHEN still_available = 'Removed' THEN FALSE END;

/* update dim_products data types */
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE float8 USING product_price::float8;

ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE varchar(25);

ALTER TABLE dim_products
ALTER COLUMN product_code TYPE varchar(25);

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE date;

ALTER TABLE dim_products
ALTER COLUMN uuid TYPE uuid using uuid::uuid;

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE bool USING still_available::boolean;

ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE varchar(25);

/* update dim_date_times types */
ALTER TABLE dim_date_times
ALTER COLUMN "day" TYPE varchar(2);

ALTER TABLE dim_date_times
ALTER COLUMN "year" TYPE varchar(4);

ALTER TABLE dim_date_times
ALTER COLUMN "month" TYPE varchar(2);

ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE varchar(15);

ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE uuid using date_uuid::uuid;

/* update dim_card_details data types */
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE varchar(25);

ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE varchar(5);

ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE date;

/* update all tables to add primary keys */
ALTER TABLE dim_date_times 
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_users 
ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_card_details 
ADD PRIMARY KEY (card_number);

ALTER TABLE dim_store_details 
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_products 
ADD PRIMARY KEY (product_code);

/* add foreign keys */

ALTER TABLE orders_table 
ADD CONSTRAINT FK_dim_date_times_date_uuid 
FOREIGN KEY (date_uuid) 
REFERENCES dim_date_times (date_uuid);

ALTER TABLE orders_table 
ADD CONSTRAINT FK_dim_users_user_uuid 
FOREIGN KEY (user_uuid) 
REFERENCES dim_users (user_uuid);

ALTER TABLE orders_table 
ADD CONSTRAINT FK_dim_card_details_card_number 
FOREIGN KEY (card_number) 
REFERENCES dim_card_details (card_number);

ALTER TABLE orders_table 
ADD CONSTRAINT FK_dim_store_details_store_code
FOREIGN KEY (store_code) 
REFERENCES dim_store_details (store_code);

ALTER TABLE orders_table 
ADD CONSTRAINT FK_dim_products_product_code 
FOREIGN KEY (product_code) 
REFERENCES dim_products (product_code);