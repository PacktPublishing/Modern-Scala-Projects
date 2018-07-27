package com.packt.modern.chapter5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoggerFactory extends Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName)
}


trait FraudDetectionWrapper {

  val trainSetFileName = "training.csv"

  val testSetFileName ="testing.csv"

  /**The entry point to programming Spark with the Dataset and DataFrame API.
  This is the SparkSession
    */

  lazy val session: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("fraud-detection-pipeline")
      .getOrCreate()
  }

  //These two statements turn off INFO statements. Feel free to turn them on


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  //The path to the dataset file
  val dataSetPath = "C:\\Users\\Ilango\\Documents\\Packt\\DevProjects\\Chapter5A\\"

  //Convenience tuples for holding feaature and label column names

  val fdTrainSet_EDA = ("summary","fdEdaFeaturesVectors")
  val fdFeatures_IndexedLabel_Train = ("fd-features-vectors","label")
  val fdFeatures_IndexedLabel_CV = ("fd-features-vectors","label")


  /*
  * this method allows us to transform our cross-validation dataset into a DataFrame
   */

  def buildTestVectors(trainPath: String): DataFrame= {
    def analyzeFeatureMeasurements: Array[(Vector,String)] = {
      val featureVectors = session.sparkContext.textFile(trainPath, 2)
        .flatMap { featureLine => featureLine.split("\n").toList }
        .map(_.split(",")).collect.map(featureLine => ( Vectors.dense( featureLine(0).toDouble,featureLine(1).toDouble),featureLine(2)))
      featureVectors
    }

    //Create a dataframe by transforming an Array of a tuple of Feature Vectors and the Label

    val fdDataFrame = session.createDataFrame(analyzeFeatureMeasurements).toDF(fdFeatures_IndexedLabel_CV._1, fdFeatures_IndexedLabel_CV._2)

    fdDataFrame.cache()
  }


}
