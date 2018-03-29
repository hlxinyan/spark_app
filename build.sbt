import sbt.Keys._

name := "spark-app"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  //spark bundled with scala version
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  , "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
  , "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
  , "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  //spark ended
  //the third party library not dundled with scala version
  , "org.apache.commons" % "commons-math3" % "3.6.1"
  , "com.typesafe" % "config" % "1.3.2"
  , "joda-time" % "joda-time" % "2.9.7"
  , "au.com.bytecode" % "opencsv" % "2.4"
  , "org.apache.poi" % "poi" % "3.17"
  , "org.apache.poi" % "poi-ooxml" % "3.17"
  //ended
  //the third party library  dundled with scala version
  , "org.scala-lang" % "scala-reflect" % "2.11.8"
  , "com.github.nscala-time" %% "nscala-time" % "2.16.0"
  , "com.github.tototoshi" %% "scala-csv" % "1.3.4"
  , "com.github.scopt" %% "scopt" % "3.6.0"
  //ended

  //test begin
  , "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  , "org.scalacheck" %% "scalacheck" % "1.13.4" % "test"
  , "junit" % "junit" % "4.12" % "test"
  , "org.mockito" % "mockito-all" % "1.10.19" % "test"
  , "log4j" % "log4j" % "1.2.17" %"test"


  //test end


)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/content/repositories/releases",
  "Typesafe Repo" at "http://dl.bintray.com/sbt/sbt-plugin-releases",
  Resolver.sonatypeRepo("public"))