package com.packt.modern.chapter3

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.functions

import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._


/*
 Performs Stock Prices Analysis
  */

object StockPricePipeline extends App with StockWrapper {

     /*
     We have three companies: a) Johnson & Johnson (JNJ) b) The Coca Cola company c) Walt Disney
     Concerning the 3 companies listed above, lets explore the data by asking the following questions:
     1) what is the average closing price per year for the following companies:
     2) What is the average closing price per month
     3) Number of times the closing price for JNJ fluctuated in the range Price(JNJ) > 3 or Price(JNJ) < 3
   */
    /*
    Build a new Dataframe with the Coca Cola dataset
     */

   val cokeDF2 = buildStockFrame2(dataSetPath + "Coke.csv")
   println("Coco Cola Dataframe is: " + cokeDF2.show())

    /*
   Build a new Dataframe with the Walt Disney Historical Prices dataset
    */

    val waltDF2 = buildStockFrame2(dataSetPath + "WaltDisney.csv")
    println("Walt Disney Dataframe is: " + waltDF2.show)

    /*
       Build a new Dataframe with the Johnson and Johnson Historical Prices dataset
    */
    val jnjDF2 = buildStockFrame2(dataSetPath + "Johnson.csv")
    println("J and J Dataframe is: " + jnjDF2.show)

    /*
    Lets Calculate average stock prices for three stocks
     */

    /*
    Extract the year out of the date string
     */

    import session.implicits._

    val cokeAvg = cokeDF2.select( year( $"Date").alias("Year"), $"Adj_Close".alias("Adjacent_Closing_Price") )
                                .groupBy("Year").avg("Adjacent_Closing_Price").withColumnRenamed( "avg(Adjacent_Closing_Price)","Averages")
                                .sort(desc("Year"))

     println("Average Stock Price for the Coca Cola company is: ")
    cokeAvg.show()


    val wdAvg = cokeDF2.select( year( $"Date").alias("Year"), $"Adj_Close".alias("Adjacent_Closing_Price") )
      .groupBy("Year").avg("Adjacent_Closing_Price").withColumnRenamed( "avg(Adjacent_Closing_Price)","Averages")
      .sort(desc("Year"))

     println("Average Stock Price for the Walt Disney company is: ")
     wdAvg.show()


    val jnjAvg = cokeDF2.select( year( $"Date").alias("Year"), $"Adj_Close".alias("Adjacent_Closing_Price") )
      .groupBy("Year").avg("Adjacent_Closing_Price").withColumnRenamed( "avg(Adjacent_Closing_Price)","Averages")
      .sort(desc("Year"))
    println("Average Stock Price for the Johnson and Johnson company is: ")
    wdAvg.show()



}




