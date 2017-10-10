name := "scala_testing"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"
val geotoolsVersion = "17.2"
val geomesaVersion = "1.3.3"

libraryDependencies ++= Seq(
    "mysql" % "mysql-connector-java" % "5.1.12",
    "com.typesafe" % "config" % "1.3.1",
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion
)

resolvers ++= Seq(
    "boundlessgeo" at "https://repo.boundlessgeo.com/main/",
    "LocationTech" at "https://repo.locationtech.org/content/repositories/releases/",
    "Spark package" at "https://dl.bintray.com/spark-packages/maven/"
)

// Select desired modules
libraryDependencies ++= Seq(
    "org.datasyslab" % "geospark" % "0.8.2",
    "harsha2010" % "magellan" % "1.0.5-s_2.11"
)

