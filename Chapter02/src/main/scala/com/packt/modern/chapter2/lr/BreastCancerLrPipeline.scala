package com.packt.modern.chapter2.lr

import com.packt.modern.chapter2.WisconsinWrapper
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.param._
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object BreastCancerLrPipeline extends WisconsinWrapper {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.ml.feature.StringIndexer

     val dataSet = buildDataFrame(dataSetPathLr + args(0) + ".csv")

    //def setInputCols(value: Array[String]): VectorAssembler.this.type
    //def setOutputCol(value: String): VectorAssembler.this.type

    val indexer = new StringIndexer().setInputCol(bcwFeatures_IndexedLabel._2).setOutputCol(bcwFeatures_IndexedLabel._3)

    val indexerModel = indexer.fit(dataSet)
    val indexedDataFrame = indexerModel.transform(dataSet)
    indexedDataFrame.show

    //  split the dataframe into training and test data**
    //75% of the data is used to train the model, and 25% will be used for testing.

    val splitDataSet: Array[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]] = indexedDataFrame.randomSplit(Array(0.75, 0.25), 98765L)

    //we needed a column called “label” and one called features to plug into the LR algorithm.
    // the StringIndexer added a new column called label

    //Why use the ElasticNet?
    //Because our features have a higher degree of correlation
    //Spark provides suppport for ElasticNet
    //ElasticSet Regulariziation is characterised by:
    // 1) Variable Selection - In other words, it is about choosing a subset of variables for prediction a.ka. predictor variable
    // 2) Model Section - we want to build a model using the aforementioned subset of predictor variables

    val trainDataSet = splitDataSet(0)
    val testDataSet = splitDataSet(1)

    trainDataSet.count()
    testDataSet.count()

    //Create a LogisticRegression Classifier Model, and set training hyperparameter on it
    //Then fit the model on the training data
    val logitModel = new LogisticRegression()
                     .setElasticNetParam(0.75)
                     .setFamily("auto")
                       .setFeaturesCol(bcwFeatures_IndexedLabel._1)
                       .setLabelCol(bcwFeatures_IndexedLabel._3).fit(trainDataSet)

    //Next run the model on the test dataset and obtain predictions
    val testDataPredictions = logitModel.transform(testDataSet)

    //this model transformation resulted in the addition of three new columns:
    //1) rawPrediction 2) probability, and 3) prediction
    testDataPredictions.show(25)

    //We would like to view what the coefficients are
    println("Coefficient Matrix: " + logitModel.coefficientMatrix)

    //view the training summary
    println("Summary:  " + logitModel.summary.predictions)

    //Start building a pipeline that has 2 stages, an Indexer and a Classifier
    val wbcPipeline = new Pipeline().setStages( Array[PipelineStage](indexer) ++  Array[PipelineStage](logitModel))


    // Lets train our pipeline with the training dataset
    // Training it also causes our indexers to be executed, and the result
    //is a Pipeline Model
    val pipelineModel = wbcPipeline.fit(trainDataSet)

    //Next, make predictions by running a transform operation the Pipeline Model and passing to it, the testing dataset

    val predictions = pipelineModel.transform(testDataSet)

        // the Precision-recall (PR) curve Receiver Operating Characteristic (ROC)
    //(ROC) curves, are evaluators for binary classification, allowing for the visualization of performance

    //Next we want to evaluate our predictions:
    // In the Logistic Regression approach, two metrics for predictions of the binary classification task exist:
    // 1) the Area under Reciever Operating Characteristic Curve - a plot of True Positive Rate against the False Positive Rate
    // 2) The area under the Precision Recall Curve - a plot of Precison against Recall

     // to obtain our metrics we will use a BinaryClassificationEvaluator which returns a precision metric by
    // comparing the test label column with //the test prediction column. In this case, the evaluation returns 99% precision.

    val modelOutputAccuracy: Double = new BinaryClassificationEvaluator()
                     .setLabelCol("label")
                     .setMetricName("areaUnderROC")  //Area under Receiver Operating Characteristic Curve
                       .setRawPredictionCol("prediction")
                     .setRawPredictionCol("rawPrediction").evaluate(testDataPredictions)

    println("Accuracy of predictions (model output) " + modelOutputAccuracy)

    val predictionAndLabels: DataFrame = predictions.select("prediction", "label")


    val validatedRDD2: RDD[(Double, Double)] = predictionAndLabels.rdd.collect {
      case Row(predictionValue: Double, labelValue: Double) => (predictionValue,labelValue)
    }
    val classifierMetrics = new BinaryClassificationMetrics(validatedRDD2)

    val accuracyMetrics = (classifierMetrics.areaUnderROC(), classifierMetrics.areaUnderPR())

      //Area under ROC
    val aUROC = accuracyMetrics._1
    println(s"Area under Receiver Operating Characteristic (ROC) curve: ${aUROC} ")
    val aPR = accuracyMetrics._2
    println(s"Area under Precision Recall (PR) curve: ${aPR} ")






  }

}
