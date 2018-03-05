package com.lhs.spark.ml.clz

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession

/**
  * 决策树分类器
  */
object ClassifiedByDecision {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("classes").getOrCreate()
    import spark.implicits._

    val data = spark.read.format("libsvm").load("E:\\sources\\spark-2.2.0\\data\\mllib\\sample_libsvm_data.txt")


    // 索引标签，并在标签列添加元数据
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)
    labelIndexer.labels
    //自动识别分类特征，并索引它们。具有4个以上不同值的特性被视为连续的。
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)
    featureIndexer.numFeatures
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3),123)

    //模型
    val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")

    //将索引标签转换回原始标签。
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    labelConverter.getLabels

    val pipeline = new Pipeline().setStages(Array(labelIndexer,featureIndexer,dt,labelConverter))

    //pipeline 模型
    val model = pipeline.fit(trainingData)

    // 预测测试集
    val predictions = model.transform(testData)

    predictions.show()

    //评估
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)

    println("Test Error = " + (1.0 - accuracy))

    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)

    spark.close()
  }
}
