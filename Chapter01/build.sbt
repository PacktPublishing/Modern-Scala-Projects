organization := "com.packt.modern.chapter1"
name := "Chapter1"
version := "0.1"

scalaVersion := "2.11.12"

//sparkVersion := "2.2.1"
//sparkComponents ++= Seq("sql","ml")

// Compiler settings. Use scalac -X for other options and their description.
// See Here for more info http://www.scala-lang.org/files/archive/nightly/docs/manual/html/scalac.html
scalacOptions ++= List("-feature","-deprecation", "-unchecked", "-Xlint")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "org.apache.spark" %% "spark-mllib" % "2.2.1",
  "org.apache.spark" %% "spark-sql" % "2.2.1",
  // Last stable release
  "org.scalanlp" %% "breeze" % "0.13.2",

  // Native libraries are not included by default. add this if you want them (as of 0.7)
  // Native libraries greatly improve performance, but increase jar sizes. 
  // It also packages various blas implementations, which have licenses that may or may not
  // be compatible with the Apache License. No GPL code, as best I know.

  "org.scalanlp" %% "breeze-natives" % "0.13.2",

  // The visualization library is distributed separately as well.
  // It depends on LGPL code
  "org.scalanlp" %% "breeze-viz" % "0.13.2",
  "joda-time" % "joda-time" % "2.9.9",
  "org.scalatest"   %% "scalatest"    % "3.0.1"   % "test",
  "org.slf4j" % "slf4j-api" % "1.7.22",
  "org.slf4j" % "slf4j-simple" % "1.7.22"

)

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
resolvers += "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
fork in run := true
fork in test := true
javaOptions in run += "-Dcom.github.fommil.netlib.NativeSystemBLAS.natives=mkl_rt.dll"
javaOptions in test += "-Dcom.github.fommil.netlib.NativeSystemBLAS.natives=mkl_rt.dll"

//artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  //artifact.name + "_" + sv.binary + "-" + sparkVersion.value + "_" + module.revision + "." + artifact.extension
//}
