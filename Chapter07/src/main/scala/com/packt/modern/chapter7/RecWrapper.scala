package com.packt.modern.chapter7

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._

trait RecWrapper {

  lazy val session: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Recommendation-System")
      .getOrCreate()
  }

  val salesOrderSchema: StructType = StructType(Array(
    StructField("sCustomerId", IntegerType,false),
    StructField("sCustomerName", StringType,false),
    StructField("sItemId", IntegerType,true),
    StructField("sItemName",  StringType,true),
    StructField("sItemUnitPrice",DoubleType,true),
    StructField("sOrderSize", DoubleType,true),
    StructField("sAmountPaid",  DoubleType,true)
  ))

  val salesLeadSchema: StructType = StructType(Array(
    StructField("sCustomerId", IntegerType,false),
    StructField("sCustomerName", StringType,false),
    StructField("sItemId", IntegerType,true),
    StructField("sItemName",  StringType,true)
  ))


  /** Builds a Past Sales Order Dataframe
    *
    * @return a Dataframe with two columns
    */
  def buildSalesOrders(dataSet: String): DataFrame = {
    session.read
      .format("com.databricks.spark.csv")
      .option("header", true).schema(salesOrderSchema).option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .load(dataSet).cache()

  }

  /*
  Build a sales leads dataframe
   */

  def buildSalesLeads (dataSet: String): DataFrame = {
    session.read
      .format("com.databricks.spark.csv")
      .option("header", true).schema(salesLeadSchema).option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .load(dataSet).cache()
  }


}
