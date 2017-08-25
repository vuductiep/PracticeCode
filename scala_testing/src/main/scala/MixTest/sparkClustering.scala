package MixTest

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.clustering.KMeans

object  sparkClustering{
    def main(args: Array[String] ) : Unit = {
        val sc = new SparkContext("local", "Spark k-mean clustering")

        val dataFile = sc.textFile("data/cluster-points.csv")
        val points = dataFile.map(_.trim).filter(_.length() > 1).map(line => parsePoints(line)).cache()

        val split = points.randomSplit(Array(0.5,0.5))
        val trainingSet = split(0).cache()
        val testSet = split(1).cache()

        println("Training set count " + trainingSet.count())
        println("Testset count " + testSet.count())


        var numClusters = 2
        val numIterations = 20
        var mdlKmeans = KMeans.train(trainingSet, numClusters, numIterations)

        println(mdlKmeans.clusterCenters)

        var clusterPred = testSet.map(x => mdlKmeans.predict(x))
        var clusterMap = testSet.zip(clusterPred)

        clusterMap.foreach(println)
        clusterMap.saveAsTextFile("results/2-cluster.csv")

        numClusters = 4
        mdlKmeans = KMeans.train(trainingSet, numClusters, numIterations)
        clusterPred = testSet.map(x => mdlKmeans.predict(x))
        clusterMap = testSet.zip(clusterPred)
        clusterMap.saveAsTextFile("results/4-cluster.csv")
        clusterMap.foreach(println)

        sc.stop()
    }

    def parsePoints (inpLine : String) : Vector = {
        val values = inpLine.split(",")
        val x = values(0).toInt
        val y = values(1).toInt
        Vectors.dense(x, y)
    }
}