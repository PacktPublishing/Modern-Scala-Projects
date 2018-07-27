package com.modern.packt.chapter6

//import org.apache.logging.log4j.scala.{Logger, Logging}
//import org.apache.logging.log4j.Level
//import org.apache.logging.log4j.core.LoggerContext

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD


trait AirlineWrapper{

   System.setProperty("log4j.configurationFile", "conf/log4j2.xml")

  lazy val session: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("airline-pipeline")
      .config("spark.debug.maxToStringFields", 5000)
      .config("spark.sql.shuffle.partitions", "100")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/airline.airlineOnTimeData")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.airlineOnTimeData")
      .getOrCreate()
  }

  val conf = new SparkConf().setMaster("local[2]") //missing
  val conf2 = session.sparkContext.getConf.setMaster("local[2]")

  val bigDatasetPaths = "C:\\Users\\Ilango\\Documents\\Packt\\DevProjects\\Chapter6OtherFiles\\DATA_files_storage\\airline-data.2013-2015\\"

  val bigDatasetPaths2  = "C:\\Users\\Ilango\\Documents\\Packt\\DevProjects\\Chapter63\\"


  /*
  carrierCode - An identification number assigned by US DoT to identify a unique airline (carrier).
    Is this the same as "Code assigned by IATA and commonly used to identify a carrier. "
   */


  case class AirlineCarrier(uniqueCarrierCode: String)


  /*
  originOfFlight - origin of flight (IATA airport code)
  destOfFlight - destination of flight (IATA airport code)
   */

  case class Flight(monthOfFlight: Int, /* Number between 1 and 12 */
                    dayOfFlight: Int,  /*Number between 1 and 31 */
                    uniqueCarrierCode: String,
                    arrDelay: Int, /* Arrival Delay - Field # 15*/
                    depDelay: Int, /* Departure Delay - Field # 16 */
                    originAirportCodeOfFlight: String, /* An identification number assigned by US DOT to identify a  unique airport. */
                    destAirportCodeOfFlight: String, /* An identification number assigned by US DOT to identify a unique airport.*/
                    carrierDelay: Int, /*  Field # 25*/
                    weatherDelay: Int, /* Field # 26*/
                    lateAircraftDelay: Int /* Field # 29*/)

  /*
    iataAirportCode - the international airport abbreviation code
   */

  case class Airports(iataAirportCode: String, airportCity: String, airportCountry: String)


  //Load and Create a File object out of the Airports dataset

  val airportsData: String = loadData("data/airports.csv")

  //Load and Create a File object out of the Airline Carrier dataset

  val carriersData: String = loadData("data/airlines.csv")

  //Create a File object out of the main FAA dataset

  val faaFlightsData: String = loadData("data/faa.csv")


  /*
  this method takes in
   */


  def loadData(dataset: String) = {
    //Get file from resources folder
    val classLoader: ClassLoader = getClass.getClassLoader
    val file: File = new File(classLoader.getResource(dataset).getFile)
    val filePath = file.getPath
    println("File path is: " + filePath)
    filePath
  }


  def buildDataFrame(dataSet: String): RDD[Array[String]] = {
    //def getRows2: Array[(org.apache.spark.ml.linalg.Vector, String)] = {
    def getRows2: RDD[Array[String]] = {
      session.sparkContext.textFile(dataSet).flatMap {
        partitionLine => partitionLine.split("\n").toList
      }.map(_.split(","))/*.filter(_(6) != "?").collect.drop(1).map( row => (Vectors.dense(row(1).toDouble, row(2).toDouble, row(3).toDouble, row(4).toDouble,row(5).toDouble,row(6).toDouble,row(7).toDouble,row(8).toDouble,row(9).toDouble),row(10)))  */
    }

    //Create a dataframe by transforming an Array of a tuple of Feature Vectors and the Label
    //drop(n) : the drop method drops all elements except the first n ones
    // dropRight(n): the dropRight method drops all elements except last n ones

    //val dataFrame = session.createDataFrame(getRows2).toDF(bcwFeatures_IndexedLabel._1, bcwFeatures_IndexedLabel._2)
    //dataFrame
    //val dataFrame = session.createDataFrame(getRows2)
    getRows2
  }


  def buildAirFrame(dataSet: String): Array[(Vector, String)] = {
    def getRows2: Array[(org.apache.spark.ml.linalg.Vector, String)] = {
        session.sparkContext.textFile(dataSet).flatMap {
        partitionLine => partitionLine.split("\n").toList
    }.map(_.split(",")).collect.drop(1).map( row => (Vectors.dense(row(1).toDouble, row(2).toDouble, row(3).toDouble, row(4).toDouble,row(5).toDouble,row(6).toDouble,row(7).toDouble,row(8).toDouble,row(9).toDouble),row(10)))
    }

    //Create a dataframe by transforming an Array of a tuple of Feature Vectors and the Label

    getRows2
  }

}





