package com.lhs.spark.ml.examples

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{PolynomialExpansion, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

object HosingPrice {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local").getOrCreate()
    val df = spark.read.option("sep",";").option("header","false").option("inferSchema","true").csv("F:\\tmpdata\\test\\ml\\house")
    val cols = "CRIM,ZN,INDUS,CHAS,NOX,RM,AGE,DIS,RAD,TAX,PTRATIO,B,LSTAT,label".split(",")
    val data = df.toDF(cols:_*)
    val vecDF = new VectorAssembler().setInputCols(cols.slice(0,cols.length-1)).setOutputCol("features").transform(data)

    val polyDF = new PolynomialExpansion().setInputCol("features").setOutputCol("polyFeatures").setDegree(3).transform(vecDF)
    val lr = new LinearRegression().setLabelCol("label").setFeaturesCol("polyFeatures").setElasticNetParam(0)//.setRegParam(0.01)

    val Array(training,test) = polyDF.randomSplit(Array(0.8,0.2),0)
    val model = lr.fit(training)

//    println(s"系数: ${model.coefficients} \n截距（常数）: ${model.intercept}")
    val evaluator = new RegressionEvaluator()


    val predict = model.transform(test)
    println(evaluator.setMetricName("r2").evaluate(predict))
//    predict.select("label","prediction").write.csv("prediction")
    byPipeline(spark,data)

  }


  def byPipeline(sparkSession: SparkSession,data:DataFrame)={
    val Array(train,test) = data.randomSplit(Array(0.8,0.2),0)
    val cols = "CRIM,ZN,INDUS,CHAS,NOX,RM,AGE,DIS,RAD,TAX,PTRATIO,B,LSTAT".split(",")
    val vecStage = new VectorAssembler().setInputCols(cols).setOutputCol("features")
    val polyStage = new PolynomialExpansion().setInputCol("features").setOutputCol("poly_features")
    val lrStage = new LinearRegression().setFeaturesCol("poly_features").setElasticNetParam(0)
    val pipeline = new Pipeline().setStages(Array(vecStage,polyStage,lrStage))
    val paramGrid = new ParamGridBuilder().addGrid(
      polyStage.degree,Array(3,2,1)
    ).build()
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(new RegressionEvaluator()).setEstimatorParamMaps(paramGrid)
    val model = cv.fit(train)

    val best = model.bestModel
    val pip = best.parent.asInstanceOf[Pipeline]
    val eva = model.getEvaluator.asInstanceOf[RegressionEvaluator]
    val predict = best.transform(test)
    println(eva.setMetricName("r2").evaluate(predict))


//    val model = pipeline.fit(train)
//    model.stages.foreach(println)
//    val evaluator = new RegressionEvaluator().setMetricName("r2")
//    val predict = model.transform(test)
//    println(evaluator.evaluate(predict))
  }
}
