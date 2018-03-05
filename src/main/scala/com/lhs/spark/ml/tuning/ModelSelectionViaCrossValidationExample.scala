package com.lhs.spark.ml.tuning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession

/**
  * 通过交叉验证进行模型选择
  */
object ModelSelectionViaCrossValidationExample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("classes").getOrCreate()
    import spark.implicits._

    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0),
      (4L, "b spark who", 1.0),
      (5L, "g d a y", 0.0),
      (6L, "spark fly", 1.0),
      (7L, "was mapreduce", 0.0),
      (8L, "e spark program", 1.0),
      (9L, "a e c l", 0.0),
      (10L, "spark compile", 1.0),
      (11L, "hadoop software", 0.0)
    )).toDF("id", "text", "label")

    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol("features")

    val lr = new LogisticRegression().setMaxIter(10)
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
    //我们使用一个ParamGridBuilder构建一个参数网格来搜索。对于hashingTF有3个值。lr.regParam的numFeatures和2值。这个网格将有3个x 2 = 6个参数设置，用于交叉验证器的选择。
    val paramGrid = new ParamGridBuilder().addGrid(hashingTF.numFeatures, Array(10, 100, 1000)).addGrid(lr.regParam, Array(0.1, 0.01)).build()

    //现在我们将管道视为一个估计器，将其包装在一个交叉验证器实例中。
    // 这将允许我们共同选择所有管道阶段的参数。一个交叉验证器需要一个估计器、一组估计程序参数和一个评估器。
    // 注意,这里的评估者是BinaryClassificationEvaluator及其默认度量是areaUnderROC
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(new BinaryClassificationEvaluator).setEstimatorParamMaps(paramGrid).setNumFolds(4)

    val cvModel = cv.fit(training)

    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    cvModel.transform(test).show(false)
    spark.close()
  }
}
