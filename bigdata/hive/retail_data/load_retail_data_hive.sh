#!/bin/bash
#===========================================================#
#Developer: Nitendra Gautam @nitendratech.com
#Date: 2019/04/06
#Objective: The purpose of this script is to load Retail Data into
#            External Hive Tables using the Hive Query Language Scripts.
#
##Make Sure /data Location has proper HDFS Permission
#sudo su hdfs
#hdfs dfs -mkdir - /data
#hdfs dfs -chown -R hive:hdfs /data
#
#=============================================================#

WORK_DIRECTORY=/tmp/retail_data


# Create the working directory if it does not exists

if [ ! -d "$WORK_DIRECTORY" ];then
 mkdir -p ${WORK_DIRECTORY}
fi

cd $WORK_DIRECTORY

echo "Downloading the Retail Data at work Directory $WORK_DIRECTORY"

wget https://gitlab.com/nitendragautam/blog_tutorials/raw/master/big_data/hive/retail_data/retail_data_categories_departments.hql
wget https://gitlab.com/nitendragautam/blog_tutorials/raw/master/big_data/hive/retail_data/retail_data_customers.hql
wget https://gitlab.com/nitendragautam/blog_tutorials/raw/master/big_data/hive/retail_data/retail_data_orders.hql
wget https://gitlab.com/nitendragautam/blog_tutorials/raw/master/big_data/hive/retail_data/retail_data_orders_items.hql
wget https://gitlab.com/nitendragautam/blog_tutorials/raw/master/big_data/hive/retail_data/retail_data_products.hql



sudo su maria_dev
echo "Creating the Hive Tables and Loading Data"

echo "Loading Categories Table"
hive -f $WORK_DIRECTORY/retail_data_categories_departments.hql

echo "Loading Customers Table"
hive -f $WORK_DIRECTORY/retail_data_customers.hql

echo "Loading Orders Table"
hive -f $WORK_DIRECTORY/retail_data_orders.hql

echo "Loading Order Items  Table"
hive -f $WORK_DIRECTORY/retail_data_orders_items.hql

echo "Loading Products Table"
hive -f $WORK_DIRECTORY/retail_data_products.hql


cd /tmp

rm -rf retail_data

echo "Loaded the Tables and Cleaned the Working Directory"
