package com.nitendratech.sparkudaf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._


/**
  * Created by nitendragautam on 6/08/2019.
  */
object FoodAnalytics {

  def main(args: Array[String]): Unit ={

  //Set the Log Level to Error
    Logger.getLogger("org").setLevel(Level.ERROR)



    println("Starting the Food Analytics Job ")


   val hdfsFilePath=args(0)

    println("Input HDFS Path: %s".format(hdfsFilePath))
    val sparkSession = SparkSession.builder()
                         .appName("Food Analytics")
                         .getOrCreate()

    val foodItemsLoad =
                      sparkSession.read.option("header","true")
                        .csv(hdfsFilePath)

    foodItemsLoad.createOrReplaceTempView("foodItemsData")

    val foodQuery =
      """
        |SELECT
        |productId,
        |foodType,
        |foodCode,
        |orderDate,
        |sellerCode,
        |cast(paidItem as int),
        |cast(unpaidItem as int)
        |from foodItemsData
        |ORDER BY productId,foodCode,orderDate
      """.stripMargin

    val foodFileDF = sparkSession.sql(foodQuery)

    val aggDF =
              foodFileDF
              .repartition(3)// Repartition the Data if the size of data is big.
              .na.fill(0,Seq("paidItem","unPaidItem")) // We replace any Null Values with Zeroes
                .groupBy("productId","foodCode","orderDate") // Group By combination of Columns
                  .agg(FoodAnalyticsUDAF(col("foodType"),
                                  col("sellerCode"),col("paidItem"),
                                  col("unpaidItem"))
                  .as("food_agg_result")) // Get the Aggregrated Results from Food Analytics
                  .select(
      col("productId"),
      col("foodCode"),
      col("orderDate"),
      col("food_agg_result.sellerCode").as("sellerCode"),
      col("food_agg_result.gmoIndicator").as("gmoIndicator"),
      col("food_agg_result.ngmoIndicator").as("ngmoIndicator"),
      col("food_agg_result.gmoPaidItem").as("gmoPaidItem"),
      col("food_agg_result.gmoUnpaidItem").as("gmoUnpaidItem"),
      col("food_agg_result.ngmoPaidItem").as("ngmoPaidItem"),
      col("food_agg_result.ngmoUnpaidItem").as("ngmoUnpaidItem")
    )


  println("Printing Aggregrated Food Data ")

  print(aggDF.show())
  }

}
