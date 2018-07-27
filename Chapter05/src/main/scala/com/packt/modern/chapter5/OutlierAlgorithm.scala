package com.packt.modern.chapter5

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.sql.DataFrame
import scala.collection.immutable.ListMap


class OutlierAlgorithm(testingDframe: DataFrame, meansVector: DenseVector, sdVector: DenseVector) extends Serializable with FraudDetectionWrapper {

//class OutlierAlgorithm(testingDframe: DataFrame, meansVector: DenseVector, sdVector: DenseVector) extends FraudDetectionWrapper {
    val scores: ListMap[Int, Double] = new ListMap[Int, Double]()

      def tuneModel(): ListMap[Int, Double] = {

        val broadcastVariable = session.sparkContext.broadcast((meansVector, sdVector))
        println("Broadcast Variable is: " + broadcastVariable)

        import session.implicits._

        val fdProbabilityDensities: DataFrame = testingDframe.map(labelledFeatureVectorRow => probabilityDensity(
          labelledFeatureVectorRow.getAs(0) /* Vector containing 2 Doubles*/ ,
          broadcastVariable.value)
        ).toDF("PDF")


        // Persist the testing dataframe with teh default storage level (MEMORY_AND_DISK)
        testingDframe.persist()

        //Find the best Error Term - this error term is denoted by ε; Its an arbitrarily small positive number

         val finalScores: ListMap[Int, Double] = evalScores(testingDframe, fdProbabilityDensities)
         println("Final Scores is: " + finalScores.mkString(" "))

         finalScores
      }


  /**
    * Purpose of this function:
    * The goal is to calculate the Probability Density Function (PDF) of each datapoint
    * Since there are 2 features, there are two PDFs per datapoint
    * The combined probability then is the product of the two.
    * pDfs is a List of 3 Tuples per row in the Test dataset; each Tuple contains 3 doubles
    * What we want inside each Tuple is this: (Transaction Value, transaction Mean, transaction SD) and since there is a second tuple,
    * The second tuple looks like this: (Distance value, Distance Mean, Distance SD)
    * When the 'map' is invoked, each tuple in turn inside the List (of Tuples) is operated upon
    * 3 Values inside the Tuple are there for a reason. All three are needed to calculate the Probability density of
    * of one feature
    *
    * True if the given point is an anomaly, false otherwise
    *
    * @param labelledFeaturesVector
    * @param broadcastVariableStatsVectorPair
    * @return
    */


  private def probabilityDensity(labelledFeaturesVector: Vector, broadcastVariableStatsVectorPair: (Vector /* Transactions */ , Vector /* Distance */ )): Double = {
    /*
      Our feature Vector is X, a continuous random variable, which has a probability density function
    */
     def featureDoubles(features: Array[Double], transactionSdMeanStats: Array[Double], distanceSdMeanStats: Array[Double]): List[(Double, Double, Double)] = {
        (features, transactionSdMeanStats, distanceSdMeanStats).zipped.toList
      }

      val pDfCalculator: List[(Double, Double, Double)] = featureDoubles(
        labelledFeaturesVector.toArray,
        broadcastVariableStatsVectorPair._1.toArray,
        broadcastVariableStatsVectorPair._2.toArray)

      val probabilityDensityValue: Double = pDfCalculator.map(pDf => new NormalDistribution(pDf._2, pDf._3).density(pDf._1)).product

      probabilityDensityValue

  }



  /**
    *  Finds the best threshold to use for selecting outliers based on the results from a validation set and the ground truth.
    *
    * @return Epsilon and the F1 score
    */
  def evalScores(testingDframe: DataFrame,probabilityDensities: DataFrame): ListMap[ Int, Double] = {

        println("Entering the scores function: fdProbabilityDensities is: ")
        probabilityDensities.show()

        /*
       Extract the smallest value of probability density and the largest.
         */
        val maxMinArray: Array[Double] = probabilityDensities.collect().map(pbRow => pbRow.getDouble(0) )
        //val maxPval: Double = probabilityDensities.collect().min\
        val maxMinPair = (maxMinArray.max, maxMinArray.min)

        println("Max of probabilities is: " + maxMinPair._1)
        println("Min of probabilities is: " + maxMinPair._2)

        //maxMinPair.show


        //println("minPVal: %s, maxPVal %s".format(minPval, maxPval))

        var bestErrorTermValue = 0D
        var bestF1Measure = 0D

        val stepsize = (maxMinPair._1 - maxMinPair._2) / 1000.0
        println("Step Size is: " + stepsize)

        //find best F1 for different epsilons
        for (errorTerm <- maxMinPair._2 to maxMinPair._1 by stepsize) {

          val broadCastedErrorTerm:Broadcast[Double] = session.sparkContext.broadcast(errorTerm)

          val broadcastTerm: Double = broadCastedErrorTerm.value

          /*
          An error term in statistics is a value which represents how observed data differs from actual population data.
          It can also be a variable which represents how a given statistical model differs from reality. The error term is often written ε.
          An error term is a variable in a statistical or mathematical model, which is created when the model does not fully represent the
          actual relationship between the independent variables and the dependent variables. ... The error term is also known as the residual,
          disturbance or remainder term.
           */


          /**Generate Final Predictions here. If the Double value in the probability densities Dataframe happens to
            * be less than the broadcast Error Term, that value is flagged as 'fraud'
            * */

          import session.implicits._
          val finalPreds: DataFrame= probabilityDensities.map { probRow =>
            if (probRow.getDouble(0) < broadcastTerm) {
              1.0 /*anomaly */
            } else 0.0
          }.toDF("PDF")


          /*
          Create a new Dataframe wwith two dataframes - the testing Dataframe and, Final Predictions Dataframe
          Drop the "Label" column in testing Dataframe and do a crossjoin with Finalpreds dataframe
          DO not forget to persist the new Dataframe with the default storage level (MEMORY_AND_DISK).
           */

          val labelAndPredictions: DataFrame = testingDframe.drop("label").crossJoin(finalPreds).cache()

          //We want to know how many False Positives
          val fPs = positivesNegatives(labelAndPredictions, 0.0, 1.0)
          //println("No of false negatives is: " + fPs)



          //We want to know how many True Positives
          val tPs = positivesNegatives(labelAndPredictions, 1.0, 1.0)

          //println("No of true Positives is: " + fPs)


          //We want to know how many True Positives
          val fNs = positivesNegatives(labelAndPredictions, 1.0, 0.0)

          //println("No of false Negatives  is: " + fPs)
          //we wanted False Positives,True Positives and True Positives to calculate precision and recall

          //Calculates the Precision
          val precision = tPs / Math.max(1.0, tPs + fPs)


          //Calculates The recall
          val recall = tPs / Math.max(1.0, tPs + fNs)

          // The F1 score or the F1 measure

          val f1Measure = 2.0 * precision * recall / (precision + recall)
          //Determine the best Error Term value and the Best F1 measure

          if (f1Measure > bestF1Measure){
              bestF1Measure = f1Measure
              bestErrorTermValue = errorTerm
            //println("f1Measure > bestF1Measure")
             scores +( (1, bestErrorTermValue), (2, bestF1Measure) )
          }
        } //the end of the for loop

    //(bestEpsilon, bestF1)
    scores

  }


  def positivesNegatives(labelAndPredictions: DataFrame /* Dataset[(Double, Double)] */, targetLabel: Double, finalPrediction: Double): Double = {
    labelAndPredictions.filter( labelAndPrediction => labelAndPrediction.getAs("PDF") == targetLabel && labelAndPrediction.get(1) == finalPrediction ).count().toDouble
  }  //end of countStatisticalMeasure



}
