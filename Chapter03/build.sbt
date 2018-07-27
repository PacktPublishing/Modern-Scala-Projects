organization := "com.packt.modern.chapter3"
name := "Chapter32"

version := "0.1"

scalaVersion := "2.11.12"

// Compiler settings. Use scalac -X for other options and their description.
// See Here for more info http://www.scala-lang.org/files/archive/nightly/docs/manual/html/scalac.html
scalacOptions ++= List("-feature","-deprecation", "-unchecked", "-Xlint")

mainClass in (Compile, run) := Some("com.packt.modern.chapter3.StockPricePipeline")

// set the main class for packaging the main jar
//mainClass in (Compile, packageBin) := Some("com.packt.modern.chapter3.StockPricePipeline")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-mllib" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1"

)

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
resolvers += "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
fork in run := true
fork in test := true