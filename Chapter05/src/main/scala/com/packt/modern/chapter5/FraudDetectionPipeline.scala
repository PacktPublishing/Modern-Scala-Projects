package com.packt.modern.chapter5

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._


object FraudDetectionPipeline extends App with FraudDetectionWrapper {


    //************************Training Data*******************************
    //*******************************************************************

              //convert raw data from a Training dataset to a DataFrame
              //The test data resides in file testing.csv
              //The training Data contains two columns holding Double values
              //The first column contains Cost Data and the second column contains Distance


            val trainSetForEda: DataFrame = session.read
              .format("com.databricks.spark.csv")
              .option("header", false).option("inferSchema", "true")
              .load(dataSetPath + trainSetFileName)

            /*
            Cashe the training data
             */
            val cachedTrainSet = trainSetForEda.cache()

            /*
            Display the cached training set
             */
            // Display the first 20 rows of the Training dataset below
            println("Cached Training DataFrame looks like below: ")
            cachedTrainSet.show()

            //The raw Training Set we obtained above serves an important purpose: Exploratory Data Analysis
            //Inspect the dataframe. For now, we keep things uncomplicated by ensuring the presence of no missing values

            /*
            Stats
             */
            val trainSetEdaStats: DataFrame = cachedTrainSet.summary()
            println("Training DataFrame Summary looks like below: ")

            trainSetEdaStats.show()

    //************************Testing Data*******************************
    //*******************************************************************


          //Builds a DataFrame for a testing dataset. Its main purpose is Cross Validation
          val cachedTestingDf: DataFrame = buildTestVectors(dataSetPath + testSetFileName)


          /**
            * testingDf contains tuples of (org.apache.spark.ml.linalg.Vector,String)
            */


          //Display the new Testset DataFrame
          println("Testing DataFrame looks like below: ")
          cachedTestingDf.show()


          /** EDA: Get Standard Deviation, Mean, Variances, etc
            * Cache the trainset dataframe. In other words, Persist this Dataset with the default storage level (`MEMORY_AND_DISK`).
            */

          println("Training dataset EDA Stats from DataFrame are below:")

          //***************************Training Set Summary**************************************
         //****************************************************************************************


          val trainSetMeanDf: DataFrame = trainSetEdaStats.where("summary == 'mean'")


          print("Means dataset is: ")
          //Display the new DataFrame - a one row DataFrame
          trainSetMeanDf.show()


          //Next, convert the DataFrame to an Array of Rows; Issuing a map function call on this array
          // extracts the "mean" row into this Array containing a single Tuple containing mean values of both Cost and Distance values
          //The result: A "Mean Pairs" Array containing a Tuple of Strings


          val meanDfPairs: Array[(String, String)] = trainSetMeanDf.collect().map(row => (row.getString(1), row.getString(2)))


          //Pull out both values of mean from the "Mean Pairs Tuple"

          val transactionMean = meanDfPairs(0)._1.toDouble
          val distanceMean = meanDfPairs(0)._2.toDouble

          //Now, we want to issue the where query to extract just the values of Standard Deviation
          //from the Training DataSet EDA statistics DataFrame

          val trainSetSdDf: DataFrame = trainSetEdaStats.where("summary == 'stddev' ")
          print("SD dataset is: ")
           //Display the contents of this "Standard Deviations" Dataframe
           trainSetSdDf.show()

          //We have a DataFrame containing two Standard Deviation values.
          // Extract these values out into an Array of Tuples.
          //This Array contains just one Tuple holding two string values of Standard Deviation


           val sdDfPairs: Array[(String, String)] = trainSetSdDf.collect().map(row => (row.getString(1), row.getString(2)))


           /*Extract the values of standard deviations out of its enclosing Tuple.
           This is the Standard Deviation of the Transaction Feature
            */
          val transactionSD = sdDfPairs(0)._1.toDouble

          /*
          Extract the Standard Deviation of the Distance feature
         */
          val distanceSD = sdDfPairs(0)._2.toDouble


          /** Lets build the following tuple pair for making a broadcast variable
            *
            * (transactionMean, transactionSD), (distanceMean, distanceSD)
            *
            */

          val meanSdTupleOfTuples = ( (transactionMean,distanceMean ),(transactionSD, distanceSD) )
          println("Resulting Tuple pairs look like this: " + meanSdTupleOfTuples)


          /**Wrap the above tuple pair in a DenseVector. We do this because we
            need a Vector to send down into the
            cluster as a Broadcast variable
            */

          val meansVector = new DenseVector(Array(meanSdTupleOfTuples._1._1, meanSdTupleOfTuples._1._2))
          println("Transaction Mean and Distance Mean Vector looks like this: " + meansVector.toArray.mkString(" "))

          /*
           Create as Array of Distance vectors contain mean and standard deviation
         */
          val sdVector: DenseVector = new DenseVector(Array(meanSdTupleOfTuples._2._1, meanSdTupleOfTuples._2._2))
          println("Distance Mean and Distance SD Vector looks like this: " + sdVector.toArray.mkString(" "))


        //******************Create an instance of the Fraud Detection Algorithm*************************
        //*********************************************************************************************

          /*
          The algorithm  needs the Training Set DataFrame, Mean and Standard deviation values from Training Set summary
          and an initial value of the Error Term
           */
          val outlierAlgorithm = new OutlierAlgorithm(cachedTestingDf, meansVector, sdVector)

          outlierAlgorithm.tuneModel()

}




