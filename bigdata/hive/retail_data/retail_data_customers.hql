CREATE DATABASE IF NOT EXISTS retail_data;

USE retail_data;

-- Table structure for table customers

CREATE EXTERNAL TABLE IF NOT EXISTS customers (
  customer_id INT,
  customer_first_name STRING,
  customer_last_name STRING,
  customer_email STRING,
  customer_password STRING,
  customer_street STRING,
  customer_city STRING,
  customer_state STRING,
  customer_zipcode STRING
)ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  LOCATION '/data/retail_data/customers';


-- Dumping data for table customers

