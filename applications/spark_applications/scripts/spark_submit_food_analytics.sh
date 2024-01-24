#!/bin/bash

/usr/hdp/current/spark2-client/bin/spark-submit \
--class com.nitendratech.sparkudaf.FoodAnalytics \
--master local[4] \
--executor-memory 4G \
/home/maria_dev/spark/sparkprojects_2.11-1.0.jar \
/user/maria_dev/fooditems/online_food_items.csv

