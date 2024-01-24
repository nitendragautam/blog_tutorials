#!/bin/bash
#===========================================================#
#Developer: Nitendra Gautam @nitendratech.com
#Date: 2018/8/06
#Objective: The purpose of this script is to load Movies Data into Hive Tables
#===========================================================#


WORK_DIRECTORY=/tmp/movies_data
HDFS_DIR=/data/movies_data
DATABASE_NAME=moviesdata

# Create the working directory if it does not exists

if [ ! -d "$WORK_DIRECTORY" ];then
 mkdir -p ${WORK_DIRECTORY}
fi

cd $WORK_DIRECTORY

MOVIES_DATA=${WORK_DIRECTORY}/movies_data.csv

if [ ! -f "$MOVIES_DATA" ];then
# Download Movies Data
wget https://raw.githubusercontent.com/nitendragautam/samp_data_sets/master/moviesdata/movies_data.csv
fi

echo "Downloaded Data From GitHub Repo"

# Make the HDFS Directory for movies data if it does not exists
hdfs dfs -mkdir -p $HDFS_DIR


#Check if file exists in the HDFS or not
hdfs dfs -test -f ${HDFS_DIR}/movies_data.csv

#Get the Return code from the test command

RETURN_CODE=$?
if [ ! $RETURN_CODE -eq 0 ];then
#Copy the movies data from the Local directory to HDFS
echo " Copy Files to HDFS: hdfs dfs -put ${MOVIES_DATA} $HDFS_DIR"
hdfs dfs -put ${MOVIES_DATA} $HDFS_DIR
fi


echo "Creating Hive Database for Movies Data and Internal Tables"
echo "Load Data from Local Path into Internal Movies Table and Select 10 records from It"
hive -e"
CREATE DATABASE IF NOT EXISTS $DATABASE_NAME;
USE $DATABASE_NAME;

CREATE TABLE IF NOT EXISTS movies_int
(id int, name String, year int, rating double, duration int)
COMMENT 'Movies Internal Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '$MOVIES_DATA' INTO TABLE movies_int;"

echo "show 20 no of lines of Data a"
hive -e "USE $DATABASE_NAME;SELECT * FROM movies_int LIMIT 20;"

echo " Count the Total Records from Movies Table "
hive -e "USE $DATABASE_NAME; SELECT COUNT(*) FROM movies_int;"

echo "List Data from Internal TABLE whose year >1990 "
hive -e "USE $DATABASE_NAME; SELECT * FROM movies_int WHERE year> 1990 LIMIT 20;"



echo "Creates Hive EXTERNAL TABLE and Repaires the Table "

hive -e "
USE $DATABASE_NAME;
CREATE EXTERNAL TABLE IF NOT EXISTS movies_ext
(id int,name String, year int, rating double, duration int)
COMMENT 'Movies External Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '$HDFS_DIR';

Msck repair table movies_ext;
"

echo "show 20 Number of lines of Data from External Table "
hive -e "USE $DATABASE_NAME;SELECT * FROM movies_ext LIMIT 20;"

echo " Count the Total Records from External Table "
hive -e "USE $DATABASE_NAME; SELECT COUNT(*) FROM movies_ext;"


echo "List Data from External TABLE whose year >1990 "
hive -e "USE $DATABASE_NAME; SELECT * FROM movies_ext WHERE year> 1990 LIMIT 20;"
