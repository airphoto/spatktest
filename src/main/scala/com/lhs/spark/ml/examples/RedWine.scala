package com.lhs.spark.ml.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

object RedWine {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val alldata = spark.read.option("header", "true").option("delimiter", ";").option("inferSchema","true").csv("F:\\tmpdata\\test\\ml\\wine")
    //删除null的数据
    val data = alldata.na.drop("any")
    //创建线性回归模型
//    data.show(5,false)
//    data.printSchema()
    val colArray = "fixed acidity|volatile acidity|citric acid|residual sugar|chlorides|free sulfur dioxide|total sulfur dioxide|density|pH|sulphates|alcohol".split("\\|")
    val vecDF = new VectorAssembler().setInputCols(colArray).setOutputCol("features").transform(data)
    val Array(training,test) = vecDF.randomSplit(Array(0.8,0.2),0)
    val lineModel = new LinearRegression().setLabelCol("quality").setFeaturesCol("features").fit(training)
    lineModel.transform(test).select("quality","prediction","features").show(40,false)
    val summary = lineModel.summary
    println(summary.r2)
    println(lineModel.getElasticNetParam)
  }
}
