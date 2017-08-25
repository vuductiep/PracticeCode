package MixTest

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree

object sparkDecisionTree {
    def main (args: Array[String]) : Unit = {
        println("Start titanic at " + getCurrentDirectory)

        val sc = new SparkContext("local", "Titanic")

        val dataFile = sc.textFile("data/titanic3_01.csv")
        val titanicRDDP = dataFile.map(_.trim).filter(_.length() > 1).map(line => parsePassengerDataToLP(line))  // delete whitespaces and empty string

        // Split data into training (60%) and test (40%).
        val splits = titanicRDDP.randomSplit(Array(0.6, 0.4), seed = 11L)

        // Append 1 into the training data as intercept.
        val trainingSet = splits(0).cache()

        val testSet = splits(1).cache()

        println("Training set count " + trainingSet.count())

        val categoricalFeaturesInfo = Map[Int, Int] ()
        val mdlTRee = DecisionTree.trainClassifier(trainingSet, 2, categoricalFeaturesInfo,"gini",5,32)

        println("depth " + mdlTRee.depth)
        println(mdlTRee.toDebugString)

        //
        // Let us predict on the dataset and see how well it works.
        // In the real world, we should split the data to train & test       and then predict the test data:
        //

        val predictions = mdlTRee.predict(testSet.map(x => x.features))
        val labelsAndPreds = testSet.map( x => x.label).zip(predictions)

        val mse = labelsAndPreds.map(vp => math.pow( (vp._1 - vp._2), 2)).reduce(_+_) / labelsAndPreds.count()
        println("Mean squared error : " + "%6f".format(mse))

        val correctVals = labelsAndPreds.aggregate(0.0)( (x,rec) => x + (rec._1 ==  rec._2).compare(false), _ + _)
        val accuracy = correctVals / labelsAndPreds.count()
        println("Accuracy = " + "%3.2f%%".format(accuracy * 100))
        println("Done")

        sc.stop()
    }

    def getCurrentDirectory = new java.io.File (".").getCanonicalPath

    //
    // 0 pclass,1 survived,2 l.name,3.f.name, 4 sex,5 age,6 sibsp,7       parch,8 ticket,9 fare,10 cabin,
    // 11 embarked,12 boat,13 body,14 home.dest
    //
    def str2Double(x: String) : Double = {
        try {
            x.toDouble
        } catch{
            case e : Exception => 0.0
        }
    }

    def parsePassengerDataToLP(inpLine : String) : LabeledPoint = {
        val values = inpLine.split(",")

        val pclass = str2Double(values(0))
        val survived = str2Double(values(1))
        // skip lastname and firstname
        var sex = 0
        if (values(4) == "male"){
            sex = 1
        }
        var age = 0.0 // in case data is null, the better choice is the average of all ages
        age = str2Double(values(5))

        var sibsp = 0.0
        sibsp = str2Double(values(6))

        var parch = 0.0
        parch = str2Double(values(7))

        var fare = 0.0
        fare = str2Double(values(9))

        new LabeledPoint(survived, Vectors.dense(pclass, sex, age, sibsp,parch,fare))
    }

}
