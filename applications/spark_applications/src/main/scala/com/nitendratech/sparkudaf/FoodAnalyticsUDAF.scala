package com.nitendratech.sparkudaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer,
                                        UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object FoodAnalyticsUDAF extends UserDefinedAggregateFunction with Serializable{

 def inputSchema = new StructType()
                   .add("foodType",StringType)
                   .add("sellerCode",StringType)
                   .add("paidItem",IntegerType)
                   .add("unpaidItem",IntegerType)

 def bufferSchema = new StructType()
              .add("seller_code",StringType)
             .add("gmo_paid_item",IntegerType)
             .add("gmo_unpaid_item" ,IntegerType)
             .add ("ngmo_paid_item",IntegerType)
            .add("ngmo_unpaid_item" ,IntegerType)

 def dataType: DataType = new StructType()
  .add("sellerCode",StringType)
  .add("gmoIndicator",StringType)
  .add("ngmoIndicator",StringType)
  .add("gmoPaidItem",IntegerType)
  .add("gmoUnpaidItem",IntegerType)
  .add("ngmoPaidItem",IntegerType)
  .add("ngmoUnpaidItem",IntegerType)

 def deterministic: Boolean = true

 def initialize(buffer: MutableAggregationBuffer) : Unit = {
  buffer(0) =""
  buffer(1) =0
  buffer(2) =0
  buffer(3) =0
  buffer(4) =0
 }

 def update(buffer: MutableAggregationBuffer, inputRow: Row): Unit ={
  if (buffer != null) {
   val foodType = inputRow.getString(0)

   buffer(0) = inputRow(1)
   foodType match {

    case "GMO" => {
     buffer(1) = buffer.getAs[Int](1)+inputRow.getInt(2)
     buffer(2) = buffer.getAs[Int](2) + inputRow.getInt(3)
    }
    case "NGMO" => {
     buffer(3) = buffer.getAs[Int](1)+inputRow.getInt(2)
     buffer(4) = buffer.getAs[Int](2) + inputRow.getInt(3)
    }
   }
  }
 }

 def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit ={

  if(buffer1!=null && buffer2 != null){
   buffer1(0) = buffer2.getString(0)
   buffer1(1) = buffer1.getAs[Int](1) +buffer2.getInt(1)
   buffer1(2) = buffer1.getAs[Int](2) +buffer2.getInt(2)
   buffer1(3) = buffer1.getAs[Int](3) +buffer2.getInt(3)
   buffer1(4) = buffer1.getAs[Int](4) +buffer2.getInt(4)
  }
 }

 def evaluate(buffer: Row): Any ={
  var gmoIndicator:String =""
  var nonGmoIndicator: String =""
  if(buffer.getAs[Int](1)>0 || buffer.getAs[Int](2) >0) gmoIndicator ="Y" else gmoIndicator="N"
  if(buffer.getAs[Int](3)>0 || buffer.getAs[Int](4)>0) nonGmoIndicator="Y" else nonGmoIndicator="N"

  Row(buffer.getString(0),
    gmoIndicator,
    nonGmoIndicator,
    buffer.getAs[Int](1),
    buffer.getAs[Int](2),
    buffer.getAs[Int](3),
    buffer.getAs[Int](4))

 }
}
