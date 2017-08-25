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

mainClass in assembly := some("MixTest.sparkRecommendation")
assemblyJarName := "MixTest.jar"

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
    case n if n.startsWith("reference.conf") => MergeStrategy.concat
    case n if n.endsWith(".conf") => MergeStrategy.concat
    case meta(_) => MergeStrategy.discard
    case x => MergeStrategy.first
}
