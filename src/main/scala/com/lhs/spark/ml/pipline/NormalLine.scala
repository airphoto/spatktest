package com.lhs.spark.ml.pipline

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.SparkSession

object NormalLine {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("NormalLine").master("local").getOrCreate()
    //训练数据
    val training = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")
    //创建一个逻辑回归实例，他是一个 Estimator
    val lr = new LogisticRegression()
    // 打印参数, 文档, 还有默认参数
    println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")
    //使用set方法设置参数
    lr.setMaxIter(10).setRegParam(0.01)
    //学习一个逻辑回归的模型
    val model1 = lr.fit(training)
    //model1（由Estimator产生的Transformer） 是一个 Model ，我们可以查看使用fit方法的时候的参数
    println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)

    //-----我们也可以使用ParamMap 去指定模型的参数
    val paramMap = ParamMap(lr.maxIter->10).put(lr.maxIter,10).put(lr.regParam->0.1).put(lr.threshold,0.55)

    //我们也可以合并参数
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")  // Change output column name.
    val paramMapCombined = paramMap ++ paramMap2

    val model2 = lr.fit(training, paramMapCombined)
    println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)


    spark.close()
  }
}
