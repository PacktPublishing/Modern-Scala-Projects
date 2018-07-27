name := "Chapter6"

version := "0.1"

scalaVersion := "2.11.12"

// Compiler settings. Use scalac -X for other options and their description.
// See Here for more info http://www.scala-lang.org/files/archive/nightly/docs/manual/html/scalac.html
scalacOptions ++= List("-feature","-deprecation", "-unchecked", "-Xlint")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-mllib" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1",
  "org.apache.hadoop" % "hadoop-core" % "1.2.1",
  "org.apache.hadoop" % "hadoop-client" % "2.7.2",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.0"
)