package com.packt.modern.chapter7

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object RecSystem extends App with RecWrapper {

  /*
  Load the training model and convert it to the rating format
  Load the sales order dataset
   */

  val salesOrdersDf = buildSalesOrders("sales\\PastWeaponSalesOrders.csv")

    /*
     Lets view the dataframe
      */

    println("Sales Order Dataframe is: ")
    salesOrdersDf.show

    /*
    Verify schema
     */
    println("Sales Order schema is: ")
    salesOrdersDf.printSchema()

    /*
      Create an Dataframe of Ratings
     */

  import session.implicits._

   val ratingsDf: DataFrame = salesOrdersDf.map( salesOrder =>
    Rating( salesOrder.getInt(0),
      salesOrder.getInt(2),
      salesOrder.getDouble(6)
    ) ).toDF("user", "item", "rating")

  println("Ratings dataframe is: ")
  ratingsDf.show


  /*
  Use the Rating class from the package org to represent a rating.
  Transform the ratings Dataframe obtained above to its RDD equivalent
   */

  val ratings: RDD[Rating] = ratingsDf.rdd.map( row => Rating( row.getInt(0), row.getInt(1), row.getDouble(2) ) )
  println("Ratings RDD is: " + ratings.take(10).mkString(" ") )

  import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

    /*
   Train a matrix factorization model given an RDD of ratings by users for a subset of products.
   Lets pass in the following parameters
    1) rank
    2) numIterations
    3) lambda
   */


  val ratingsModel: MatrixFactorizationModel = ALS.train(ratings,
    6, /* THE RANK */
    10,  /* Number of iterations */
    15.0)


  /*
 Next, we load the sales lead file and convert it to tuple format.
  */

  val weaponSalesLeadDf = buildSalesLeads("sales\\WeaponSalesLeads.csv")

  println("Sales Lead dataframe is: ")
  weaponSalesLeadDf.show


  val customerWeaponsSystemPairDf: DataFrame = weaponSalesLeadDf.map(salesLead => ( salesLead.getInt(0), salesLead.getInt(2) )).toDF("user","item")

  println("The Customer-Weapons System dataframe as tuple pairs looks like: ")
  customerWeaponsSystemPairDf.show

  val customerWeaponsSystemPairRDD: RDD[(Int, Int)] = customerWeaponsSystemPairDf.rdd.map(row => (row.getInt(0), row.getInt(1)) )
  println("UserProducts RDD is: " + customerWeaponsSystemPairRDD.take(10).mkString(" "))

  /*
  Finally, we can predict the future rating using a simple API.
  We predict the rating of those products that were not rated by users before. The output RDD has an element
  per each element in the input RDD (including all duplicates) unless a user or product is missing in the training set.
   */

  val weaponRecs: RDD[Rating] = ratingsModel.predict(customerWeaponsSystemPairRDD).distinct()
  println("Future ratings are: " + weaponRecs.foreach(rating => { println( "Customer Nation: " + rating.user + " Weapons System:  " + rating.product + " Rating: " + rating.rating ) } ) )


}
