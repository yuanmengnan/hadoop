/**
  * @author xubo
  *         ref:http://spark.apache.org/docs/1.5.2/mllib-guide.html
  *         more code:https://github.com/xubo245/SparkLearning
  *         more blog:http://blog.csdn.net/xubo245
  */
package org.apache.spark.mllib.EvaluationMetrics

import org.apache.spark.util.SparkLearningFunSuite

/**
  * Created by xubo on 2016/6/13.
  */
class RankingSystemsFunSuite extends SparkLearningFunSuite {
  test("testFunSuite") {


    import org.apache.spark.mllib.evaluation.{RegressionMetrics, RankingMetrics}
    import org.apache.spark.mllib.recommendation.{ALS, Rating}

    // Read in the ratings data
    val ratings = sc.textFile("file/data/mllib/input/mllibFromSpark/sample_movielens_data.txt").map { line =>
      val fields = line.split("::")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble - 2.5)
    }.cache()

    // Map ratings to 1 or 0, 1 indicating a movie that should be recommended
    val binarizedRatings = ratings.map(r => Rating(r.user, r.product, if (r.rating > 0) 1.0 else 0.0)).cache()

    // Summarize ratings
    val numRatings = ratings.count()
    val numUsers = ratings.map(_.user).distinct().count()
    val numMovies = ratings.map(_.product).distinct().count()
    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

    // Build the model
    val numIterations = 10
    val rank = 10
    val lambda = 0.01
    val model = ALS.train(ratings, rank, numIterations, lambda)

    // Define a function to scale ratings from 0 to 1
    def scaledRating(r: Rating): Rating = {
      val scaledRating = math.max(math.min(r.rating, 1.0), 0.0)
      Rating(r.user, r.product, scaledRating)
    }

    // Get sorted top ten predictions for each user and then scale from [0, 1]
    val userRecommended = model.recommendProductsForUsers(10).map { case (user, recs) =>
      (user, recs.map(scaledRating))
    }

    // Assume that any movie a user rated 3 or higher (which maps to a 1) is a relevant document
    // Compare with top ten most relevant documents
    val userMovies = binarizedRatings.groupBy(_.user)
    val relevantDocuments = userMovies.join(userRecommended).map { case (user, (actual, predictions)) =>
      (predictions.map(_.product), actual.filter(_.rating > 0.0).map(_.product).toArray)
    }

    // Instantiate metrics object
    val metrics = new RankingMetrics(relevantDocuments)

    // Precision at K
    Array(1, 3, 5).foreach { k =>
      println(s"Precision at $k = ${metrics.precisionAt(k)}")
    }

    // Mean average precision
    println(s"Mean average precision = ${metrics.meanAveragePrecision}")

    // Normalized discounted cumulative gain
    Array(1, 3, 5).foreach { k =>
      println(s"NDCG at $k = ${metrics.ndcgAt(k)}")
    }

    // Get predictions for each data point
    val allPredictions = model.predict(ratings.map(r => (r.user, r.product))).map(r => ((r.user, r.product), r.rating))
    val allRatings = ratings.map(r => ((r.user, r.product), r.rating))
    val predictionsAndLabels = allPredictions.join(allRatings).map { case ((user, product), (predicted, actual)) =>
      (predicted, actual)
    }

    // Get the RMSE using regression metrics
    val regressionMetrics = new RegressionMetrics(predictionsAndLabels)
    println(s"RMSE = ${regressionMetrics.rootMeanSquaredError}")

    // R-squared
    println(s"R-squared = ${regressionMetrics.r2}")


  }
}
