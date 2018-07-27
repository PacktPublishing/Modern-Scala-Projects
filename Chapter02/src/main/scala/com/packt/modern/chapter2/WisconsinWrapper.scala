package com.packt.modern.chapter2

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}


trait WisconsinWrapper {

  //The entry point to programming Spark with the Dataset and DataFrame API.
  //This is the SparkSession

  lazy val session: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("breast-cancer-pipeline")
      .getOrCreate()
  }


  //The path to the dataset file
  val dataSetPathLr = "C:\\Users\\Ilango\\Documents\\Packt\\DevProjects\\Chapter2\\"

  val dataSetPathRf = "C:\\Users\\Ilango\\Documents\\Packt\\DevProjects\\Chapter2\\"

  val bcwFeatures_IndexedLabel = ("features","bcw-diagnoses-column", "label")

  /**
    *
    * @return a Dataframe with two columns. `irisFeatureColumn` contains the feature `Vector`s and `irisTypeColumn` contains the `String` iris types
    */

  def buildDataFrame(dataSet: String): DataFrame = {
    def getRows2: Array[(org.apache.spark.ml.linalg.Vector, String)] = {
      session.sparkContext.textFile(dataSet).flatMap {
        partitionLine => partitionLine.split("\n").toList
      }.map(_.split(",")).filter(_(6) != "?").collect.drop(1).map(row => (Vectors.dense(row(1).toDouble, row(2).toDouble, row(3).toDouble, row(4).toDouble,row(5).toDouble,row(6).toDouble,row(7).toDouble,row(8).toDouble,row(9).toDouble),row(10)))
    }
    //Create a dataframe by transforming an Array of a tuple of Feature Vectors and the Label

    val dataFrame = session.createDataFrame(getRows2).toDF(bcwFeatures_IndexedLabel._1, bcwFeatures_IndexedLabel._2)
    //val bcFrameCached = dataFrame.cache
    //bcFrameCached
    dataFrame
  }

}





















