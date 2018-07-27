package com.packt.modern.chapter1

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

trait IrisWrapper {

  //The entry point to programming Spark with the Dataset and DataFrame API.
  //This is the SparkSession

  lazy val session: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("iris-pipeline")
      .getOrCreate()
  }

  val dataSetPath = "C:\\Users\\Ilango\\Documents\\Packt\\DevProjects\\Chapter1\\"

  val irisFeatures_CategoryOrSpecies_IndexedLabel = ("iris-features-column","iris-species-column", "label")



  /**
    * Load iris data. This data is a CSV with no header.
    *
    * The description of this dataset is found in iris.names. A detailed description of this
    * is also to be found in the Project Overview section.
    *
    * dataSetPath will be "C:/Users/Ilango/Documents/Packt-Book-Writing-Project/Trial_And_Error_Projects/chapter1/iris.data"
    * or simply "iris.data", because the Spark Shell is started from the chapter1 folder
    *
    * @return a Dataframe with two columns. `irisFeatureColumn` contains the feature `Vector`s and `irisTypeColumn` contains the `String` iris types
    */

  def buildDataFrame(dataSet: String): DataFrame = {
    def getRows: Array[(org.apache.spark.ml.linalg.Vector, String)] = {
      session.sparkContext.textFile(dataSet).flatMap {
        partitionLine => partitionLine.split("\n").toList
      }.map(_.split(",")).collect.drop(1).map(row => (Vectors.dense(row(1).toDouble, row(2).toDouble, row(3).toDouble, row(4).toDouble),row(5)))

    }
    // irisFeatures correspond to the feature columns (that are now in a Vector) and irisCategory
    // corresponds to the output label, which in the input set is the species column.
    // Available species to choose from to predict label(species) of a flower in the wild,
    // are Iris-setosa, Iris-versicolor and iris-virginica, each of which needs to be indexed to a double value

    val dataFrame = session.createDataFrame(getRows).toDF(irisFeatures_CategoryOrSpecies_IndexedLabel._1, irisFeatures_CategoryOrSpecies_IndexedLabel._2)
    dataFrame
  }

}
