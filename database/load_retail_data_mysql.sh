#!/bin/bash
#======================================================================#
#Author: Nitendra Gautam
#Date: 2019-01-27 @nitendratech.com
#Description: Loads the Retail DB Data into MySQL Database Given UserName,Password.
#              It will create a retail_dba user in the MySQL database .
#=======================================================================#

NUM_ARGS=#?

if [[ $NUM_ARGS==2 ]]
	then
	USER_NAME=$1
	PASSWORD=$2
else
	echo "Give two Parameters for MySQL, USER_NAME and PASSWORD "
	exit 1
fi

echo "Starting the load_retail_data_mysql script"

cd /tmp

echo "Downloading the SQL file from Gitlab Repo"
wget https://raw.githubusercontent.com/nitendragautam/samp_data_sets/master/retail_data/retail_db.sql

RETAIL_USER=retail_dba

echo "Creating the Retail $RETAIL_USER User Name for Retail Database "
mysql -u $USER_NAME -p$PASSWORD <<EOF
SHOW DATABASES;
CREATE database IF NOT EXISTS retail_db;
CREATE user $RETAIL_USER identified by 'hadoop';
GRANT ALL ON retail_db.* to retail_dba;
flush privileges;
EOF

echo "Loading the Retail Data from SQL File"
# Login into mysql using the retail_dba user and load the retail_db data

mysql -u $USER_NAME -p$PASSWORD <<EOF
USE retail_db;
source /tmp/retail_db.sql;
SHOW TABLES;
SELECT * FROM customers LIMIT 10;
SELECT DISTINCT(Year(order_date)) FROM orders WHERE order_status='PENDING_PAYMENT';
SELECT COUNT(*) as count FROM customers c JOIN orders o ON(c.customer_id=o.order_customer_id) WHERE o.order_status='PENDING_PAYMENT';
SELECT COUNT(*) as count FROM customers c JOIN orders o ON(c.customer_id=o.order_customer_id) WHERE o.order_status='CLOSED';
SELECT c.customer_fname,c.customer_lname,o.order_date,o.order_status FROM customers c JOIN orders o ON(c.customer_id=o.order_customer_id) WHERE c.customer_state='TX' LIMIT 20
EOF

echo "Loading of Retail Data Finised"
