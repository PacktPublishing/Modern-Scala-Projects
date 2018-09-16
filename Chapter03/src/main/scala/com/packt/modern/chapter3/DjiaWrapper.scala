package com.packt.modern.chapter3

import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

case class LabeledHeadlines (
                              hDate: String,  //Row(0)
                              Label: String,
                              TopHeadline1: String, // -- 3
                              TopHeadline2: String, // -- 4
                              TopHeadline3: String, // -- 5
                              TopHeadline4: String, // -- 6
                              TopHeadline5: String, // -- 7
                              TopHeadline6: String, // -- 8
                              TopHeadline7: String, // -- 9
                              TopHeadline8: String, // -- 10
                              TopHeadline9: String, // -- 11
                              TopHeadline10: String, // -- 12
                              TopHeadline11: String, // -- 13
                              TopHeadline12: String,  // -- 14
                              TopHeadline13: String,// -- 15
                              TopHeadline14: String, // -- 16
                              TopHeadline15: String,// -- 17
                              TopHeadline16: String,// -- 18
                              TopHeadline17: String,// -- 19
                              TopHeadline18: String,// -- 20
                              TopHeadline19: String,// -- 21
                              TopHeadline20: String,// -- 22
                              TopHeadline21: String, // -- 23
                              TopHeadline22: String, // -- 24
                              TopHeadline23: String, // -- 25
                              TopHeadline24: String, // -- 26
                              TopHeadline25: String // -- 27
                            )

    case class Reddit (
                        hDate: String,
                        News: String
                      )


   case class djiatable(
                sDate: java.sql.Date,
                Open: Double,
                High: Double,
                Low: Double,
                Close: Double,
                Volume: Double,
                Adj_Close:Double
              )

trait DjiaWrapper {

  //The entry point to programming Spark with the Dataset and DataFrame API.
  //This is the SparkSession

  lazy val session: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Stock-Price-Pipeline")
      .getOrCreate()
  }

  //val dataSetPath = "C:\\Users\\Ilango\\Documents\\Packt\\DevProjects\\Chapter33\\Combined_News_DJIA-short.csv"
  val dataSetPath1 = "C:\\Users\\Ilango\\Documents\\Packt\\DevProjects\\Chapter33\\News.csv"

  val dataSetPath2 = "C:\\Users\\Ilango\\Documents\\Packt\\DevProjects\\Chapter33\\DJIA_table.csv"

  val dataSetPath3 = "C:\\Users\\Ilango\\Documents\\Packt\\DevProjects\\Chapter33\\RedditNews.csv"

  val reg1 = raw"[^A-Za-z0-9\s]+" // remove punctuation with numbers

  val djiaSchema = StructType(Seq(
    StructField("sDate", DateType), //1
    StructField("Open", DoubleType),
    StructField("High", DoubleType),
    StructField("Low", DoubleType),
    StructField("Close", DoubleType),
    StructField("Volume", DoubleType),
    StructField("Adj_Close", DoubleType)
  )
  )

  val redditSchema = StructType(Seq(
    StructField("sDate", DateType), //1
    StructField("News", StringType)
  )
  )

  /*
 Read combined headlines + Dow Jones Industrial Average data
  */
  def newsDJIA(dataFile: String): DataFrame = {

    val ridOfPunctuation: String => String = (text: String) => {
      raw"[^A-Za-z0-9\s]+".r.replaceAllIn(text, "").toLowerCase
    }

    def getRows2 = {
      session.sparkContext.textFile(dataFile) /*.map(ridOfPunctuation) */
        .flatMap {
        partitionLine => partitionLine.split("\n").toList
      }.map( _.split(",").map(ridOfPunctuation)).collect.drop(1).map(rowArray => LabeledHeadlines(
        rowArray(0),
        rowArray(1),
        rowArray(2),
        rowArray(3),
        rowArray(4),
        rowArray(5),
        rowArray(6),
        rowArray(7),
        rowArray(8),
        rowArray(9),
        rowArray(10),
        rowArray(11),
        rowArray(12),
        rowArray(13),
        rowArray(14),
        rowArray(15),
        rowArray(16),
        rowArray(17),
        rowArray(18),
        rowArray(19),
        rowArray(20),
        rowArray(21),
        rowArray(22),
        rowArray(23),
        rowArray(24),
        rowArray(25),
        rowArray(26)
      ) //------- 2
      )
    }

    val headlines: DataFrame = session.createDataFrame(getRows2).toDF()

    headlines
  }


  def newsReddit(dataFile: String): DataFrame = {
  //def newsReddit(dataFile: String): Array[String] = {

      val ridOfPunctuation: String => String = (text: String) => {
        raw"[^A-Za-z0-9\s]+".r.replaceAllIn(text, "").toLowerCase
      }

      def getRows = {
        session.sparkContext.textFile(dataFile) /*.map(ridOfPunctuation) */
          .flatMap {
          partitionLine => partitionLine.split("\n").toList
        }.map(_.split(",").map(ridOfPunctuation) ).collect.drop(1).map(rowArray => Reddit(
          rowArray(0),
          rowArray(1)
        )
        )

      }

    val headlines: DataFrame = session.createDataFrame(getRows).toDF()
    //val headlines = getRows

      headlines
  }


}





