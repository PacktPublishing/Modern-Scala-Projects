package com.packt.modern.chapter2.rf

import com.packt.modern.chapter2.WisconsinWrapper
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object BreastCancerRfPipeline extends WisconsinWrapper {

      def main(args: Array[String]): Unit = {

            import org.apache.spark.ml.feature.StringIndexer

            val dataSet: DataFrame = buildDataFrame(dataSetPathRf + args(0) + ".csv")

            //Split the dataset in two. 85% of the dataset becomes the Training (data)set and 15% becomes the testing (data) set
            val splitDataSet: Array[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]] = dataSet.randomSplit(Array(0.85, 0.15), 98765L)
            println("Size of the new split dataset " + splitDataSet.size)

            val testDataSet = splitDataSet(1)
            println("TEST DATASET set count is: " + testDataSet.count())

            val indexer = new StringIndexer().setInputCol(bcwFeatures_IndexedLabel._2).setOutputCol(bcwFeatures_IndexedLabel._3)

            val trainDataSet = splitDataSet(0)
            println("Training set count is: " + trainDataSet.count())


            //Create a classifier and pass into it (hyper) parameters
            /*
          We will set the below parameters first
        1) Features column name
        2)Indexed Label Column Name
        3)  No of features to be considered per split (we have 150 observations and 4 features - that will make our max_features 2 (??)
          Since Iris is a classification problem, the 'sqrt' setting for featureSubsetStrategy is what we need


        In addition we will pass in other parameters like impurity, no of trees to train, etc.
        But this time we will employ an exhaustive grid search-based model selection process
        based on combinations of parameters, where parameter value ranges are specified,
        1) Impurity settings - values can be gini and entropy
        2) Number of trees to train (since the no of trees is greater than 1, we set
        3) Tree Maximum depth, which is a number equal to the no of nodes
        4) The required Minimum no feature measurements (sampled observations) aka minimum instances per Node
         */

            val randomForestClassifier = new RandomForestClassifier()
              .setFeaturesCol(bcwFeatures_IndexedLabel._1)
              .setFeatureSubsetStrategy("sqrt")

            //Start building a pipeline that has 2 stages, an Indexer and a Classifier
            val irisPipeline = new Pipeline().setStages(Array[PipelineStage](indexer) ++ Array[PipelineStage](randomForestClassifier))

            //Lets set the hyper parameter NumTrees
            val rfNum_Trees = randomForestClassifier.setNumTrees(15)
            println("Hyper Parameter num_trees is: " + rfNum_Trees.numTrees)

            //confirm that the classifier has a default value set
            println("Is Max-Depth for classifier set? - " + rfNum_Trees.hasDefault(rfNum_Trees.numTrees))
            println("Default Max_Depth set on classifier is - " + rfNum_Trees.getOrDefault(rfNum_Trees.numTrees))

            //Now lets add our NUM_TREES hyper parameter to the param grid
            val gridBuilder1 = new ParamGridBuilder().addGrid(rfNum_Trees.numTrees, Array(8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88, 96))

            //------------------------------Set next hyperparameter below-----------------
            //set this default parameter in the classifier's embedded param map
            val rfMax_Depth = rfNum_Trees.setMaxDepth(2)
            println("Hyper Parameter max_depth is: " + rfMax_Depth.maxDepth)

            //confirm that the classifier has a default value set
            println("Is max_depth for classifier set? - " + rfMax_Depth.hasDefault(rfMax_Depth.maxDepth))
            println("Default max_depth set on classifier is - " + rfMax_Depth.getOrDefault(rfMax_Depth.maxDepth))

            //Now lets add our MAX_DEPTH hyper parameter to the param grid
            val gridBuilder2 = gridBuilder1.addGrid(rfMax_Depth.maxDepth, Array(4, 10, 16, 22, 28))

            //------------------------------Set next hyperparameter below-----------------

            val rfImpurity = rfMax_Depth.setImpurity("gini")
            println("Hyper Parameter Impurity value is: " + rfImpurity.impurity)

            //confirm that the classifier has a default value set
            println("Is Impurity for classifier set?  - " + rfImpurity.hasDefault(rfImpurity.impurity))
            println("Default Impurity set on classifier is - " + rfImpurity.getOrDefault(rfImpurity.impurity))

            //Now lets add our IMPURITY hyper parameter to the param grid
            val gridBuilder3 = gridBuilder2.addGrid(rfImpurity.impurity, Array("gini", "entropy"))

            //----------Before we build the final grid, lets confirm that all 3 default hyperparameters are set

            println("Confirming that Default Max_Depth set on classifier is - " + rfImpurity.getOrDefault(rfNum_Trees.numTrees))
            println("Confirming that Default max_depth set on classifier is - " + rfImpurity.getOrDefault(rfMax_Depth.maxDepth))
            println("Confirming that Default Impurity set on classifier is - " + rfImpurity.getOrDefault(rfImpurity.impurity))

            //---------------Its time to buid the final param grid now------------------------

            val finalParamGrid: Array[ParamMap] = gridBuilder3.build()

            //Next we want to split our training set into a validation set and training set
            //The purpose of our validation set is to be able to make a choice between models
            //We want an evaluation metric and hyperparameter tuning

            //We will now create a Validation Estimator, split the train set into a validation set and training set
            //Finally we will fit this estimator over the training dataset to produce a model, a transformer that we
            // we will use to transform our testData
            // The also apply an evaluator for a metric that will do hyperparameter tuning

            val validatedTestResults: DataFrame = new TrainValidationSplit()
              .setSeed(1234567L)
              .setEstimatorParamMaps(finalParamGrid)
              .setEstimator(irisPipeline)
              .setEvaluator(new MulticlassClassificationEvaluator())
              .setTrainRatio(.80)
              .fit(trainDataSet)
              .transform(testDataSet)

            println("Validated dataset is: " + validatedTestResults.show(100))

            //Review of results: We just ran the test and our model created new columns Probability and Prediction
            //In this section we deal with things like accuracy, precision, confusion matrix, measure, etc
            //Since the Breast Cancer  dataset is a Multiclass Classification problem of Supervised Learning, there is a Spark class
            //called Multiclass.
            //The review section starts with the instantiation of the BinaryClassificationMetrics class
            //AT this point the output of our model's fit and transform process resulted in the following dataframe:
            //Display a DataFrame hereunder


            val validatedTestResultsDataset: DataFrame = validatedTestResults.select("prediction", "label")
            println("Validated TestSet Results Dataset is:  " + validatedTestResultsDataset.take(10))

            val validatedRDD2: RDD[(Double, Double)] = validatedTestResultsDataset.rdd.collect {
                  case Row(predictionValue: Double, labelValue: Double) => (predictionValue, labelValue)
            }


            //Lets evaluate Model output now
            // we pass in 3 hyper parameters
            val modelOutputAccuracy: Double = new MulticlassClassificationEvaluator()
              .setLabelCol("label")
              .setMetricName("accuracy")
              .setPredictionCol("prediction").evaluate(validatedTestResultsDataset)

            println("Accuracy of Model Output results on the test dataset: " + modelOutputAccuracy)

            //Now, lets evaluate other metrics
            //How close is the predicted label value in the predicted column compared to the actual label value in the label column
            //Lets see what the 'precision' (precision appears to be deprecated in favor of accuracy) is followed by its WeightedPrecison counterpart
            //lets see what the 'accuracy' is

            val multiClassMetrics = new MulticlassMetrics(validatedRDD2)
            val accuracyMetrics = (multiClassMetrics.accuracy, multiClassMetrics.weightedPrecision)
            val accuracy = accuracyMetrics._1
            val weightedPrecsion = accuracyMetrics._2

            println("Accuracy (precision) is " + accuracy + " Weighted Precision is: " + weightedPrecsion)

      }

}

//end of object
