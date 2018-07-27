package com.modern.packt.chapter6

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
/*
Import the MongoDB Packages
 */
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.bson.Document


object Aircraft extends AirlineWrapper {

  def main(args: Array[String]): Unit = {

    /*
    This is a case class to represent Flights Data
     */

    case class FlightsData(
                         flightYear: String, /* 1 */
                         flightMonth : String, /* 2 */
                         flightDayOfmonth : String, /* 3 */
                         flightDayOfweek : String, /* 4 */
                         flightDepTime : String, /* 5 */
                         flightCrsDeptime : String, /* 6 */
                         flightArrtime : String, /* 7 */
                         flightCrsArrTime : String, /* 8 */
                         flightUniqueCarrier : String,/* 9 */
                         flightNumber : String, /* 10 */
                         flightTailNumber : String, /* 11 */
                         flightActualElapsedTime : String, /* 12 */
                         flightCrsElapsedTime : String, /* 13 */
                         flightAirTime : String, /* 14 */
                         flightArrDelay : String, /* 15 */
                         flightDepDelay : String, /* 16 */
                         flightOrigin : String, /* 17 */
                         flightDest : String, /* 18 */
                         flightDistance : String, /* 19 */
                         flightTaxiin : String, /* 20 */
                         flightTaxiout : String, /* 21 */
                         flightCancelled : String, /* 22 */
                         flightCancellationCode : String, /* 23 */
                         flightDiverted : String, /* 24 */
                         flightCarrierDelay : String, /* 25 */
                         flightWeatherDelay : String, /* 26 */
                         flightNasDelay : String, /* 27 */
                         flightSecuritDelay : String, /* 28 */
                         flightLateAircraftDelay : String, /* 29 */
                         record_insertion_time: String, /* 30 */
                         uuid : String /* 31 */
                        )

    /*
    Lets create a schema
     */

    val flightSchema = StructType(Seq(
      StructField("flightYear", IntegerType),
      StructField("flightMonth", StringType),
      StructField("flightDayOfmonth", StringType),
      StructField("flightDayOfweek", IntegerType),
      StructField("flightDepTime", StringType),
      StructField("flightCrsDeptime", StringType),
      StructField("flightArrtime", IntegerType),
      StructField("flightCrsArrTime", StringType),
      StructField("flightUniqueCarrier", StringType),
      StructField("flightNumber", IntegerType),
      StructField("flightTailNumber", StringType),
      StructField("flightActualElapsedTime", StringType),
      StructField("flightCrsElapsedTime", IntegerType),
      StructField("flightAirTime", StringType),
      StructField("flightArrDelay", StringType),
      StructField("flightDepDelay", IntegerType),
      StructField("flightOrigin", StringType),
      StructField("flightDest", StringType),
      StructField("flightDistance", IntegerType),
      StructField("flightTaxiin", StringType),
      StructField("flightTaxiout", StringType),
      StructField("flightCancelled", IntegerType),
      StructField("flightCancellationCode", StringType),
      StructField("flightDiverted", StringType),
      StructField("flightCarrierDelay", IntegerType),
      StructField("flightWeatherDelay", StringType),
      StructField("flightNasDelay", StringType),
      StructField("flightNasDelay", IntegerType),
      StructField("flightSecurityDelay", StringType),
      StructField("flightLateAircraftDelay", StringType),
      StructField("record_insertion_time", IntegerType),
      StructField("uuid", StringType),
    ))


     import session.implicits._
      val airFrame: DataFrame = session.read
      .format("com.databricks.spark.csv")
      .option("header", true).option("inferSchema", "true").option("treatEmptyValuesAsNulls", true)
      .load("2008.csv")

    /*
      *Print schema
      */

    println("The schema of the raw Airline Dataframe is: ")
      airFrame.printSchema()

    /*
      *Creates a local temporary view using the given name. The lifetime of this temporary view is
      *tied to the SparkSession that was used to create this Dataset.
      */

   airFrame.createOrReplaceTempView("airline_ontime")




 /*
    *Having trimmed and cast our fields and made sure the numeric columns work, we
    *can now save our data as JSON Lines and Parquet. Call the toJSON method to
    *returns the content of the Dataset as a Dataset of JSON strings.
      */


   val airFrameJSON: Dataset[String] = clippedAirFrameForDisplay.toJSON


    println("Airline Dataframe as JSON is: ")
    airFrameJSON.show(10)



   /*
    *Save our JSON Airline Dataframe as a gzipped json file
     */

   airFrameJSON.rdd.saveAsTextFile("json/airlineOnTimeDataShort.json.gz", classOf[org.apache.hadoop.io.compress.GzipCodec])*/


 /*
    *Convert our datframe to parquet records.
    *How to create a parquet file
    *first create data frame from csv file, then store this data frame
    *in parquet file and then create a new data frame from parquet file.
    *Fortunately, data frames and the Parquet file format fit the bill nicely. Parquet is a compressed columnar file format.
    *Columnar file formats greatly enhance data file interaction speed and compression by organizing data by columns
    *rather than by rows. The two main advantages of a columnar format is that queries will deserialize only that data
    *which is actually needed, and compression is frequently much better since columns frequently contained
    *highly repeated values.
      */



 clippedAirFrameForDisplay.write.format("parquet").save("parquet/airlineOnTimeDataShort.parquet")



 /*
    *Load the JSON files
      */


     val airlineOnTime_Json_Frame: DataFrame = session.read.json("json/airlineOnTimeDataShort.json.gz")
    println("JSON version of the Airline dataframe is: ")
    airlineOnTime_Json_Frame.show()



  /*
     *Lets load the parquet version as well
      */

   val airlineOnTime_Parquet_Frame: DataFrame = session.read.format("parquet").load("parquet/airlineOnTimeDataShort.parquet")

 /*
    *Print out the Parquet version of the Airline dataframe
     */

    println("Parquet version of the Airline dataframe is: ")
    airlineOnTime_Parquet_Frame.show(10)


 /*
    *Write to the Mongodb database airlineOnTimeData
    *The DataFrameWriter includes the .mode("overwrite") to drop the hundredClub collection
    *before writing the results, if the collection already exists.
     */


   MongoSpark.save( airlineOnTime_Parquet_Frame.write.option("collection", "airlineOnTimeData").mode("overwrite") )


    /*
     *To confirm data was written, read from it
     *verify the save, reads from the airlineOnTimeData collection:
      */



    MongoSpark.load[Character](session, ReadConfig(Map("collection" -> "airlineOnTimeData"), Some(ReadConfig(session)))).show()


 }


 }
