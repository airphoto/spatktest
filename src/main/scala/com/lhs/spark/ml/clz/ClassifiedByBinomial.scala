package com.lhs.spark.ml.clz

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

/**
  * 二项式分类
  */
object ClassifiedByBinomial {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("classes").getOrCreate()
    import spark.implicits._

    val training = spark.read.format("libsvm").load("E:\\sources\\spark-2.2.0\\data\\mllib\\sample_libsvm_data.txt")

    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

    val lrModel = lr.fit(training)

    println(s"权重: ${lrModel.coefficients} 截距: ${lrModel.intercept}")

    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val mlrModel = mlr.fit(training)

    // Print the coefficients and intercepts for logistic regression with multinomial family
    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")

    spark.close()
  }
}
