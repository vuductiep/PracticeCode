package MixTest

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.optimization.{LBFGS, LogisticGradient, SquaredL2Updater}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.util.MLUtils

object sparkBasicStats{
    def main(args: Array[String]): Unit = {
        println(getCurrentDirectory)
        val sc = new SparkContext("local", "Test")
        println(s"Running Spark Version ${sc.version}")

        val dataFile = sc.textFile("src/main/data//car-milage-no-hdr.csv")
        val carRDD = dataFile.map(line => parseCarData(line))
        //        val carRDD = MLUtils.loadLibSVMFile(sc, "src/main/data//car-milage-no-hdr.csv")

        val vectors = carRDD.map(v => Vectors.dense(v))
        val summary = Statistics.colStats(vectors)

        carRDD.foreach(ln => {
            ln.foreach(no => print("%6.2f ! ".format(no))); println()
        })

        print("Max :");
        summary.max.toArray.foreach(m => print("%5.1f |     ".format(m)));
        println
        print("Min :");
        summary.min.toArray.foreach(m => print("%5.1f |     ".format(m)));
        println
        print("Mean :");
        summary.mean.toArray.foreach(m => print("%5.1f     | ".format(m)));
        println

        //
        // correlations
        //
        val hp = vectors.map(x => x(2))
        val weight = vectors.map(x => x(10))
        var corP = Statistics.corr(hp, weight, "pearson") // default
        println("hp to weight : Pearson Correlation = %2.4f".format(corP))
        var corS = Statistics.corr(hp, weight, "spearman") // Need to   specify
        println("hp to weight : Spearman Correlation = %2.4f".format(corS))
        //
        val raRatio = vectors.map(x => x(5))
        val width = vectors.map(x => x(9))
        corP = Statistics.corr(raRatio, width, "pearson") // default
        println("raRatio to width : Pearson Correlation = %2.4f".format(corP))
        corS = Statistics.corr(raRatio, width, "spearman") // Need to   specify
        println("raRatio to width : Spearman Correlation = %2.4f".format(corS))
        //

        // Linear Regression
        //
        val carRDDLP = carRDD.map(x => carDataToLP(x)) // Create a labeled point RDD
        println(carRDDLP.count())
        println(carRDDLP.first().label)
        println(carRDDLP.first().features)

        //
        // let us split the data set into training and test set using a very simple filter
        //
        val carRDDLPTrain = carRDDLP.filter(x => x.features(9) <= 4000).cache()
        val carRDDLPTest = carRDDLP.filter(x => x.features(9) > 4000).cache()
        println("Training Set : " + "%3d".format(carRDDLPTrain.count()))
        println("Testing Set : " + "%3d".format(carRDDLPTest.count()))
        //
        // TRain a Linear REgression Model
        // numIterations = 100, stepsize = 0.000000001
        // without  such a small step size the algorithm will diverge
        //        val lr = new LinearRegression().setMaxIter(100).setRegParam(0.3).setElasticNetParam(0.8)
        //        val lrModel = lr.fit(carRDDLPTrain)

        val mdlLR = LinearRegressionWithSGD.train(carRDDLPTrain, 100, 0.000000001)
        println(mdlLR.intercept) // Intercept is turned off when using   LinearRegressionSGD object, so intercept will always be 0 for     this code
        println(mdlLR.weights)

        //
        // Now let us use the model to predict our test set
        //
        val valuesAndPreds = carRDDLPTest.map(p => (p.label, mdlLR.predict(p.features)))
        val mse = valuesAndPreds.map(vp => math.pow((vp._1 - vp._2), 2)).
            reduce(_ + _) / valuesAndPreds.count()
        println("Mean Squared Error     = " + "%6.3f".format(mse))
        println("Root Mean Squared Error = " + "%6.3f".format(math.sqrt(mse)))
        // Let us print what the model predicted
        valuesAndPreds.take(20).foreach(m => println("%5.2f | %5.2f |".format(m._1, m._2)))

        ///
        // New linear regression
        //
        val carData = carRDD.map(x => carDataToLP(x)) // Create a labeled point RDD
        println("Data count: " + carData.count())

        // Split data into training (60%) and test (40%).
        val splits = carData.randomSplit(Array(0.6, 0.4), seed = 11L)

        // Append 1 into the training data as intercept.
        val training = splits(0).map(x => (x.label, MLUtils.appendBias(x.features))).cache()

        val test = splits(1)

        val numFeatures = carData.take(1)(0).features.size
        val numCorrections = 10
        val convergenceTol = 1e-4
        val maxNumIterations = 20
        val regParam = 0.1
        val initialWeightsWithIntercept = Vectors.dense(new Array[Double](numFeatures + 1))

        val (weightsWithIntercept, loss) = LBFGS.runLBFGS(
            training,
            new LogisticGradient(),
            new SquaredL2Updater(),
            numCorrections,
            convergenceTol,
            maxNumIterations,
            regParam,
            initialWeightsWithIntercept)

        val model = new LogisticRegressionModel(
            Vectors.dense(weightsWithIntercept.toArray.slice(0, weightsWithIntercept.size -1 )),
            weightsWithIntercept(weightsWithIntercept.size -1 )
        )

        // clear the default threshold
        model.clearThreshold()

        val scoreAndLabels = test.map { point =>
            val score = model.predict(point.features)
            (score, point. label)
        }

        // Get evaluation metrics
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        val auRoc = metrics.areaUnderROC()
        println("Loss of each step in training process")
        loss.foreach(println)
        println("Area under ROC = " + auRoc)

        scoreAndLabels.foreach(println)

        sc.stop()
    }

    def carDataToLP(inpArray : Array[Double]) : LabeledPoint = {
        return new LabeledPoint( inpArray(0), Vectors.dense (   inpArray(1), inpArray(2), inpArray(3),  inpArray(4), inpArray(5), inpArray(6), inpArray(7), inpArray(8), inpArray(9), inpArray(10), inpArray(11) ) )
    }

    def carDataToNewRDD(inpArray : Array[Double]) : (Double, Vector) = {
        ( inpArray(0), Vectors.dense (   inpArray(1), inpArray(2), inpArray(3),  inpArray(4), inpArray(5), inpArray(6), inpArray(7), inpArray(8), inpArray(9), inpArray(10), inpArray(11) ) )
    }


    def getCurrentDirectory = new java.io.File( ".").getCanonicalPath

    def parseCarData(inpLine : String) : Array[Double] = {
        val values = inpLine.split(',')
        val mpg = values(0).toDouble
        val displacement = values(1).toDouble
        val hp = values(2).toInt
        val torque = values(3).toInt
        val CRatio = values(4).toDouble
        val RARatio = values(5).toDouble
        val CarbBarrells = values(6).toInt
        val NoOfSpeed = values(7).toInt
        val length = values(8).toDouble
        val width = values(9).toDouble
        val weight = values(10).toDouble
        val automatic = values(11).toInt
        Array(mpg,displacement,hp,
            torque,CRatio,RARatio,CarbBarrells,
            NoOfSpeed,length,width,weight,automatic)
    }
}