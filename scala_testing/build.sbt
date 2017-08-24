name := "scala_testing"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
    "mysql" % "mysql-connector-java" % "5.1.12",
    "com.typesafe" % "config" % "1.3.1",
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion
)