package com.packt.modern.chapter3

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, Tokenizer}
import org.apache.spark.sql.{DataFrame, DataFrameNaFunctions, functions}
import org.apache.spark.sql.functions._

object DowJonesMovement extends App with DjiaWrapper  {

  val allHeadlines = newsDJIA(dataSetPath1)

  println("All headlines is " + allHeadlines.show(2,0,true) )

  val mergedNewsColumns: DataFrame =
    allHeadlines.withColumn( "AllMergedHeadlines",
      functions.concat_ws( " ", allHeadlines("TopHeadline1"),
        allHeadlines("TopHeadline2"),
        allHeadlines("TopHeadline3"),
        allHeadlines("TopHeadline4"),
        allHeadlines("TopHeadline5"),
        allHeadlines("TopHeadline6"),
        allHeadlines("TopHeadline7"),
        allHeadlines("TopHeadline8"),
        allHeadlines("TopHeadline9"),
        allHeadlines("TopHeadline10"),
        allHeadlines("TopHeadline11"),
        allHeadlines("TopHeadline12"),
        allHeadlines("TopHeadline13"),
        allHeadlines("TopHeadline14"),
        allHeadlines("TopHeadline15"),
        allHeadlines("TopHeadline16"),
        allHeadlines("TopHeadline17"),
        allHeadlines("TopHeadline18"),
        allHeadlines("TopHeadline19"),
        allHeadlines("TopHeadline20"),
        allHeadlines("TopHeadline21"),
        allHeadlines("TopHeadline22"),
        allHeadlines("TopHeadline23"),
        allHeadlines("TopHeadline24"),
        allHeadlines("TopHeadline25")
        //newsColsOnly("Label")
      ))select("hDate", "AllMergedHeadlines","Label")

  println("new dataframe with all headlines merged into one column is: ")
  mergedNewsColumns.show(2)

  println("the schema of Merged News Columns is: ")
  mergedNewsColumns.printSchema()

  //Create a Tokenizer to tokenize  headlines into individual lowercase words separated by whitespaces
  val headlineTokenizer = new Tokenizer().setInputCol("AllMergedHeadlines").setOutputCol("TokenizedHeadlines")

  val headlineWordCount = udf { (headlineWords: Seq[String]) => headlineWords.length }

  //The call to 'na' is meant for dropping any rows containing null values
  val naFunctions: DataFrameNaFunctions = mergedNewsColumns.na

  //Drops any rows containing any null or NaN values in the column AllMergedHeadlines
  val nonNullHeadlines = naFunctions.drop(Array("AllMergedHeadlines"))

  println("Non-Null Bag Of lower-cased Words DataFrame looks like this:")
  nonNullHeadlines.show()

  nonNullHeadlines.columns

  println("the schema of Non Null News Headlines is: ")
  nonNullHeadlines.printSchema

  val tokenizedHeadlines: DataFrame = headlineTokenizer.transform(nonNullHeadlines)
    .select("hDate", "TokenizedHeadlines", "Label")
    .withColumnRenamed("Label", "label").withColumn("tokens", headlineWordCount(col("TokenizedHeadlines")))

  println("Tokenized Headlines are: ")
  tokenizedHeadlines.show(20)

  println("********************************************")

  // StopWordRemover
  import org.apache.spark.ml.feature.StopWordsRemover

  //"features" is a "sentence"

  //Next up: Stop words:
  // Stop Words are words which should be excluded from the input,
  // typically because the words appear frequently and don’t carry as much meaning.
  //StopWordsRemover takes as input a sequence of strings (e.g. the output of a Tokenizer) and drops all the stop words from the input sequences.

  val stopWordRemover = new StopWordsRemover().setInputCol("TokenizedHeadlines").setOutputCol("StopWordFreeHeadlines") // same as "noStopWords"
  val stopWordFreeHeadlines = stopWordRemover.transform(tokenizedHeadlines)

  println("Tokenized Non-Null Bag Of lower-cased Headline words with no stopwords: ")
  stopWordFreeHeadlines.show(2)

  println("Schema for stop word free headlines is: ")
  stopWordFreeHeadlines.printSchema()

  import session.implicits._

  val noStopWordsDataFrame2 = stopWordFreeHeadlines.select( explode( $"StopWordFreeHeadlines").alias("StopWordFreeHeadlines"),
     stopWordFreeHeadlines("label"),headlineWordCount( col("StopWordFreeHeadlines") ) )

  println("Exploded: Tokenized Non-Null Bag Of lower-cased Words with no stopwords - this DataFrame looks like this: ")
  noStopWordsDataFrame2.show()

  // fit a CountVectorizerModel from the corpus
  val cvModel: CountVectorizerModel = new CountVectorizer()
    .setInputCol("StopWordFreeHeadlines")
    .setOutputCol("StopWordFreeFeatures")
    .setVocabSize(3)
    .setMinDF(2)
    .fit(stopWordFreeHeadlines)

  val cvDf = cvModel.transform(stopWordFreeHeadlines)
  println("Dataframe transformed by a CountVectorizer model is: ")
  cvDf.show(2)

  println("The schema of the Dataframe produced by the CountVectorizer model is: ")
  cvDf.printSchema()

  val djia = session.read.format("com.databricks.spark.csv").option("header",true).schema(djiaSchema).load(dataSetPath2)

   println("The DJIA table looks like this: ")
   djia.show(10)

  // Timestamp pattern found in string
  val pattern = "yyyy-MMM-dd"

  import org.apache.spark.sql.functions._

  val djiaSorted = djia.orderBy(unix_timestamp(djia("sDate"), pattern).cast("timestamp"))

  println(" Sorted DJIA table is:  ")
  djiaSorted.show(20)

  val reddit = newsReddit(dataSetPath3)

  //println("Reddit news headlines: " + reddit.mkString(""))
  reddit.show(20)


}
