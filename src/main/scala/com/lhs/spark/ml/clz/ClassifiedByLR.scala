package com.lhs.spark.ml.clz

import breeze.linalg.Matrix
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, max, when}

import scala.collection.mutable.ArrayBuffer

object ClassifiedByLR {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("classes").getOrCreate()
    import spark.implicits._

    //开始四列分别包含 URL 、页面的 ID 、原始的文本内容和分配给页面的类别。接下来 22 列包含各种各样的数值或者类属特征。
    //最后一列为目标值， -1 为长久，0 为短暂。
    val rawData = spark.read.textFile("F:\\tmpdata\\test\\ml\\Classification\\train.tsv")
//    val rawData = spark.read.textFile("/test/input/ml/classification")

    //由于数据格式的问题，我们做一些数据清理的工作，在处理过程中把额外的（ " ）去掉。
    //数据集中还有一些用 "?" 代替的缺失数据，本例中，我们直接用 0 替换那些缺失数据：
    val records = rawData.map(line=>{
      val trimmed = line.split("\t").map(_.replaceAll("\"",""))
      val label = trimmed.last.toDouble
      val features = trimmed.slice(4,trimmed.length-1).map{d=>if(d=="?") 0.0 else d.toDouble}
      (label,features)
    })
    //目的是添加表头,并将array转化成vector
    val data = records.map{case (label,features) => LabeledPoint(label,Vectors.dense(features))}
    data.show(false)
//    val features = data.map(r=>r.features)

    //逻辑回归
    val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")

    val lrModel = lr.fit(data)
    // 打印系数和截距
    println(s"系数: ${lrModel.coefficients} 截距（常数）: ${lrModel.intercept}")



    val trainingSummary = lrModel.summary
    val objectiveHistory = trainingSummary.objectiveHistory
//    println("损失:")//每次迭代的损失，一般会逐渐减小
    objectiveHistory.foreach(loss => println(loss))

    val binaryLogisticRegressionSummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]
    binaryLogisticRegressionSummary.roc
    println(binaryLogisticRegressionSummary.areaUnderROC)



    lrModel.summary.predictions.select("label","features").show(false)
//    lrModel.coefficientMatrix
//    lrModel.getThreshold
//    lrModel.numClasses
//    lrModel.hasSummary
//    lrModel.uid
//    lrModel.numFeatures
//    lrModel.getStandardization

    binaryLogisticRegressionSummary.predictions.show(50,false)
//    binaryLogisticRegressionSummary.pr.show(false)
//    binaryLogisticRegressionSummary.featuresCol
//    binaryLogisticRegressionSummary.labelCol
//    binaryLogisticRegressionSummary.probabilityCol
//    binaryLogisticRegressionSummary.recallByThreshold.show(false)
//    binaryLogisticRegressionSummary.precisionByThreshold.show(false)
//    binaryLogisticRegressionSummary.fMeasureByThreshold.show(false)
//----------------------------------------------------------



    //添加类别数据
    val categories = rawData.map(r=>r.split("\t")(3).replaceAll("\"","").trim).distinct().collect().zipWithIndex.toMap
    val numCategories = categories.size
    val broadCategory = spark.sparkContext.broadcast(categories)

    categories.contains("business")

    val dataCategories = rawData.map(r=>{
      val trimed = r.split("\t").map(_.replaceAll("\"","").trim)
      val label = trimed.last.toInt
      val categoryID = broadCategory.value(trimed(3))
      val categoryFeatures = Array.ofDim[Double](numCategories)
      categoryFeatures(categoryID) = 1.0
      val otherFeatures = trimed.slice(4,r.size-1).map(d=>if(d=="?") 0.0 else d.toDouble)
      val features = categoryFeatures ++ otherFeatures
      LabeledPoint(label,Vectors.dense(features))
    })

    //将数据分为训练集和测试集
    val Array(trainDF,testDF) = dataCategories.randomSplit(Array(0.8,0.2),123)

    val lrModelWithCategory = lr.fit(trainDF)
    // 打印系数和截距
    println(s"系数: ${lrModelWithCategory.coefficients} 截距（常数）: ${lrModelWithCategory.intercept}")

    val trainingSummaryWithCategory = lrModelWithCategory.summary
    val objectiveHistoryWithCategory = trainingSummaryWithCategory.objectiveHistory
    //    println("损失:")//每次迭代的损失，一般会逐渐减小
    objectiveHistoryWithCategory.foreach(loss => println(loss))

    val binaryLogisticRegressionSummaryWithCategory = trainingSummaryWithCategory.asInstanceOf[BinaryLogisticRegressionSummary]
    binaryLogisticRegressionSummaryWithCategory.roc
    println(binaryLogisticRegressionSummaryWithCategory.areaUnderROC)

//    lrModelWithCategory.coefficientMatrix
//    lrModelWithCategory.getThreshold
    //    lrModel.getThresholds
//    lrModelWithCategory.numClasses
//    lrModelWithCategory.hasSummary
//    lrModelWithCategory.uid
//    lrModelWithCategory.numFeatures
//    lrModelWithCategory.getStandardization
//    binaryLogisticRegressionSummaryWithCategory.predictions.select("label","prediction").show(50,false)
//    binaryLogisticRegressionSummaryWithCategory.pr.show(false)
//    binaryLogisticRegressionSummaryWithCategory.featuresCol
//    binaryLogisticRegressionSummaryWithCategory.labelCol
//    binaryLogisticRegressionSummaryWithCategory.probabilityCol
//    binaryLogisticRegressionSummaryWithCategory.recallByThreshold.show(false)
//    binaryLogisticRegressionSummaryWithCategory.precisionByThreshold.show(false)
//    binaryLogisticRegressionSummaryWithCategory.fMeasureByThreshold.show(false)

    binaryLogisticRegressionSummaryWithCategory.predictions.groupBy("label").agg(count("label")).show(false)

    val testResult = lrModelWithCategory.transform(testDF)
    testResult.show(false)
    val labelWithPrediction = testResult.select("label","prediction")

    labelWithPrediction.groupBy("label").agg(count("label")).show(false)
    labelWithPrediction.agg(count("label").as("all_count"),count(when($"label"===$"prediction",1)).as("accuracy")).show(false)

    val fMeasure = binaryLogisticRegressionSummaryWithCategory.fMeasureByThreshold
    fMeasure.show(false)

    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure).select("threshold").head().getDouble(0)
    lrModelWithCategory.setThreshold(bestThreshold)
    lrModelWithCategory.getThreshold

    spark.close()
  }
}
