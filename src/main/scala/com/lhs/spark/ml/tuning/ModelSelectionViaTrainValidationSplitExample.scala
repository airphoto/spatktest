package com.lhs.spark.ml.tuning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.SparkSession

object ModelSelectionViaTrainValidationSplitExample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local").getOrCreate()

    val data = spark.read.format("libsvm").load("E:\\sources\\spark-2.2.0\\data\\mllib\\sample_linear_regression_data.txt")
    data.show(false)

    val Array(training,test) = data.randomSplit(Array(0.9,0.1),seed=12345)

    val lr = new LinearRegression().setMaxIter(10)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam,Array(0.1,0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam,Array(0.0,0.5,1.0))
      .build()

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)

    val model = trainValidationSplit.fit(training)

    model.transform(test).select("features","label","prediction").show(false)

    spark.close()
  }
}
