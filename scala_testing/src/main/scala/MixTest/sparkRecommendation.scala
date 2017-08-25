package MixTest

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}

object  sparkRecommendation{
    def main(args: Array[String]) : Unit = {
        val sc = new SparkContext("local", "Recommendation")

        val moviesFile = sc.textFile("data/medium/movies.dat")
        val moviesRDD = moviesFile.map(line => line.split("::"))
        println("Movie count " + moviesRDD.count())

        val ratingsFile = sc.textFile("data/medium/ratings.dat")
        val ratingsRDD = ratingsFile.map(line => parseRating1(line))
        println("Rating count " + ratingsRDD.count())

        ratingsRDD.take(5).foreach(println)
        val numRatings = ratingsRDD.count()
        val numUsers = ratingsRDD.map(r => r._1).distinct().count()
        val numMovies = ratingsRDD.map( m => m._2).distinct().count()
        printf("Got %d ratings from %d users on %d  movies\n".format(numRatings, numUsers, numMovies))

        // Split the dataset into training, validation, and test
        val trainSet = ratingsRDD.filter(x => (x._4 % 10 ) < 6 ).map(x => parseRating(x))
        val validationSet = ratingsRDD.filter(x => (x._4 % 10) >= 6 &       (x._4 % 10) < 8).map(x=>parseRating(x))
        val testSet = ratingsRDD.filter(x => (x._4 % 10) >= 8)     .map(x=>parseRating(x))

        println("Training : " + "%d".format(trainSet.count()) +
            ", validation: " + "%d".format(validationSet.count()) + ", test: " + "%d".format(testSet.count()) + ".")

        // training
        val rank = 10
        val numIterations = 20
        val mdlALS =  ALS.train(trainSet, rank, numIterations)

        // prepare validation
        val userMovie = validationSet.map {
            case Rating(user, movie, rate) => (user, movie)
        }

        // Predict and convert to key - value PairRDD
        val predictions = mdlALS.predict(userMovie).map{
            case Rating(user, movie, rate) => ( (user, movie), rate)
        }
        println("Prediction count " + predictions.count())
        predictions.take(5).foreach(println)

        // Now convert the validation set to PairRDD:
        val validationPairRDD = validationSet.map(r => ((r.user, r.product), r.rating))
        println("Validation set count: " + validationSet.count())
        validationPairRDD.take(5).foreach(println)
        println(validationPairRDD.getClass)
        println(predictions.getClass)

        //
        // Now join the validation set with predictions.
        // Then we can figure out how good our recommendations are.
        // Tip:
        //   Need to import org.apache.spark.SparkContext._
        //   Then MappedRDD would be converted implicitly to PairRDD
        //
        val  ratingsAndPreds  = validationPairRDD.join(predictions)
        println("ratings and predictions" + ratingsAndPreds.count())
        ratingsAndPreds.take(3).foreach(println)

        val mse = ratingsAndPreds.map(r => {
            math.pow( (r._2._1  - r._2._2),  2)
        }).reduce(_+_)  / ratingsAndPreds.count()
        val rmse = math.sqrt(mse)
        println("MSE = %2.5f".format(mse) + " RMSE = %2.5f".format(rmse))
        println("Done")

        sc.stop()
    }

    def parseRating1(line : String) : (Int, Int, Double, Int) = {
        val x = line.split("::")
        val userId =x(0).toInt
        val movieId = x(1).toInt
        val rating = x (2) .toDouble
        val timeStamp = x(3).toInt/10
        (userId, movieId, rating, timeStamp)
    }

    def parseRating (x : (Int, Int, Double, Int)) : Rating = {
        val userId = x._1
        val movieId = x._2
        val rating = x._3
        val timeStamp = x._4
        new Rating (userId, movieId, rating)
    }

}