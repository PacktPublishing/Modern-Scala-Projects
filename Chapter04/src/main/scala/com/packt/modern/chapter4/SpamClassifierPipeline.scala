package com.packt.modern.chapter4

import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.{DataFrame, DataFrameNaFunctions, Row, SparkSession}
import org.apache.spark.ml.feature.{HashingTF, IDF, Normalizer, Tokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.explode

/*
  How to run it:
  sbt console
  scala> import com.packt.modern.chapter4.SpamClassifierPipeline
  scala> SpamClassifierPipeline.main(Array("inbox.txt"))
  //Next time, try loading it on HDFS

*/

case class LabeledHamSpam(label: Double, mailSentence: String)

object SpamClassifierPipeline extends App with SpamWrapper {


    val reg1 = raw"[^A-Za-z0-9\s]+" // remove punctuation with numbers
    val regex2 = raw"[^A-Za-z\s]+" // remove punctuation not include numbers

    val hamRDD: org.apache.spark.rdd.RDD[String] = session.sparkContext.textFile(hamSetFileName)
    val hamRDD2 = hamRDD.map(_.replaceAll(reg1, "").trim.toLowerCase)
    val hamRDD3: RDD[LabeledHamSpam] = hamRDD2.repartition(4).map(w => LabeledHamSpam(0.0,w))
    hamRDD3.take(10)
    println("The HAM RDD looks like: " + hamRDD3.collect())


    val spamRDD = session.sparkContext.textFile(spamFileName)
    val spamRDD2 = spamRDD.map(_.replaceAll(reg1, "").trim.toLowerCase)
    val spamRDD3 = spamRDD2.repartition(4).map(w => LabeledHamSpam(1.0,w))

    //A check
    spamRDD3.take(10)

    val hamAndSpam: org.apache.spark.rdd.RDD[LabeledHamSpam] =  (hamRDD3 ++ spamRDD3)
    hamAndSpam.take(10)

    ////////////////////////////// STEP 1 //////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////

    //Or use this
    val hamAndSpamDFrame = session.createDataFrame(hamAndSpam).toDF("label","lowerCasedSentences")

    val lowerCasedDataFrame = hamAndSpamDFrame.select(hamAndSpamDFrame("lowerCasedSentences"), hamAndSpamDFrame("label"))
    println("lowerCasedDataFrame looks like this:")
    lowerCasedDataFrame.show

    lowerCasedDataFrame.printSchema

    lowerCasedDataFrame.columns

    //Create a Tokenizer that according to the Spark API tokenizes ham and spam
    //sentences into individual lowercase words by whitespaces
    val mailTokenizer2 = new Tokenizer().setInputCol("lowerCasedSentences").setOutputCol("mailFeatureWords")

    //The call to 'na' is meant for dropping any rows containing null values
    val naFunctions: DataFrameNaFunctions = lowerCasedDataFrame.na
    val nonNullBagOfWordsDataFrame = naFunctions.drop(Array("lowerCasedSentences"))

    println("Non-Null Bag Of lower-cased Words DataFrame looks like this:")
    nonNullBagOfWordsDataFrame.show()

    nonNullBagOfWordsDataFrame.columns

    nonNullBagOfWordsDataFrame.printSchema

    val tokenizedBagOfWordsDataFrame: DataFrame = mailTokenizer2.transform(nonNullBagOfWordsDataFrame)

    println("Tokenized Non-Null Bag Of lower-cased Words DataFrame looks like this: ")
    tokenizedBagOfWordsDataFrame.show()

    // StopWordRemover
    import org.apache.spark.ml.feature.StopWordsRemover

    //"features" is a "sentence"

    val stopWordRemover = new StopWordsRemover().setInputCol("mailFeatureWords").setOutputCol("noStopWordsMailFeatures") // same as "noStopWords"
    val noStopWordsDataFrame = stopWordRemover.transform(tokenizedBagOfWordsDataFrame)

    println("Tokenized Non-Null Bag Of lower-cased Words with no stopwords - this DataFrame looks like this:")
    noStopWordsDataFrame.show()

    import session.implicits._

    val noStopWordsDataFrame2 = noStopWordsDataFrame.select(explode($"noStopWordsMailFeatures").alias("noStopWordsMailFeatures"),noStopWordsDataFrame("label"))

    println("Exploded: Tokenized Non-Null Bag Of lower-cased Words with no stopwords - this DataFrame looks like this: ")
    noStopWordsDataFrame2.show()

    val hashMapper = new HashingTF().setInputCol("noStopWordsMailFeatures").setOutputCol("mailFeatureHashes").setNumFeatures(10000)
    //hashMapper: org.apache.spark.ml.feature.HashingTF = hashingTF_89eb55ea399c

    val featurizedDF1 = hashMapper.transform(noStopWordsDataFrame)

    println("Hash-Featurized AND Tokenized Non-Null Bag Of lower-cased Words with no stopwords - this DataFrame looks like this:")
    featurizedDF1.show()

    //Create Training and test datasets
    val splitFeaturizedDF1 = featurizedDF1.randomSplit(Array(0.80, 0.20), 98765L)

    val testFeaturizedDF1 = splitFeaturizedDF1(1)
    println("TEST DATASET set count is: " + testFeaturizedDF1.count())

    val trainFeaturizedDF1 = splitFeaturizedDF1(0)
    println("TRAIN DATASET set count is: " + trainFeaturizedDF1.count())

    println("trainFeaturizedDF1 looks like this: ")
    trainFeaturizedDF1.show()

    val trainFeaturizedDF1New = trainFeaturizedDF1.drop("mailFeatureWords","noStopWordsMailFeatures","mailFeatureHashes")

    println("trainFeaturizedDF1 with 3 columns mailFeatureWords,noStopWordsMailFeatures,mailFeatureHashes dropped looks like this: ")
    trainFeaturizedDF1New.show()

    val mailIDF = new IDF().setInputCol("mailFeatureHashes").setOutputCol("mailIDF")
    val mailIDFFunction = mailIDF.fit(featurizedDF1)

    val normalizer = new Normalizer().setInputCol("mailIDF").setOutputCol("features")


    //Naive Bayes Algorithm

    val naiveBayes = new NaiveBayes().setFeaturesCol("features").setPredictionCol("prediction")

     val spamPipeline1 = new Pipeline().setStages(Array[PipelineStage](mailTokenizer2) ++
                                        Array[PipelineStage](stopWordRemover) ++
                                        Array[PipelineStage](hashMapper) ++
                                        Array[PipelineStage](mailIDF) ++
                                        Array[PipelineStage](normalizer) ++
                                        Array[PipelineStage](naiveBayes))

    // Fit the pipeline to training documents.
    val mailModel1 = spamPipeline1.fit(trainFeaturizedDF1New)

    //Make predictions on test dataset

     val rawPredictions = mailModel1.transform(testFeaturizedDF1.drop("mailFeatureWords","noStopWordsMailFeatures","mailFeatureHashes"))
     println("Predictions are: ")
     rawPredictions.show(100)

     val predictions = rawPredictions.select($"lowerCasedSentences", $"prediction").cache
     println("Displaying Predictions as below:")
     predictions.show(50)


     session.stop()


}