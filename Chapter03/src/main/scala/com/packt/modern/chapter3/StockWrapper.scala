package com.packt.modern.chapter3

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DateType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}


case class Stock(dt: String, openprice: Double, highprice: Double, lowprice: Double, closeprice: Double,
                 volume: Double, adjcloseprice: Double)

trait StockWrapper {

  lazy val session: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Stock-Price-Pipeline")
      .getOrCreate()
  }


  val stockSchema: StructType = StructType(Array(
    StructField("Date", DateType,false),
    StructField("Open", DoubleType,false),
    StructField("High", DoubleType,true),
    StructField("low",  DoubleType,true),
    StructField("Close",DoubleType,true),
    StructField("Adj_Close", DoubleType,true),
    StructField("Volume",  DoubleType,true)
  ))


  val dataSetPath = "C:\\Users\\Ilango\\Documents\\Packt\\DevProjects\\Chapter32\\"

  val pathToJar = "target\\scala-2.11\\chapter32_2.11-0.1.jar"


  def buildStockFrame(dataFile: String): DataFrame = {
    def getRows2: Array[(org.apache.spark.ml.linalg.Vector, String)] = {
      session.sparkContext.textFile(dataFile).flatMap {
        partitionLine => partitionLine.split("\n").toList
      }.map(_.split(",")).collect.drop(1).map( row => (Vectors.dense( row(1).toDouble,
                                                              row(2).toDouble,
                                                              row(3).toDouble,
                                                              row(4).toDouble,
                                                              row(5).toDouble),
                                                              row(6)
                                                         )
      )
    } //end of function
    //Create a dataframe by transforming an Array of a tuple of Feature Vectors and the Label

    //val dataFrame = session.createDataFrame(getRows2).toDF(bcwFeatures_IndexedLabel._1, bcwFeatures_IndexedLabel._2)
    val dataFrame = session.createDataFrame(getRows2).toDF
    val stockFrameCached = dataFrame.cache
    //bcFrameCached
    stockFrameCached
  }

  /*
  Get rid of nulls also
   */
  def buildStockFrame2(dataFile: String): DataFrame = {
    session.read
    .format("com.databricks.spark.csv")
    .option("header", true).schema(stockSchema).option("nullValue","")
      .option("treatEmptyValuesAsNulls","true")
    .load(dataFile).cache()
  }


  def parseStock(str: String): Stock = {
    val line = str.split(",")
    Stock(line(0), line(1).toDouble, line(2).toDouble, line(3).toDouble, line(4).toDouble, line(5).toDouble,
      line(6).toDouble)
  }

  def parseRDD(rdd: RDD[String]): RDD[Stock] = {
    val header = rdd.first
    rdd.filter(_(0) != header(0)).map(parseStock).cache()
  }


}


