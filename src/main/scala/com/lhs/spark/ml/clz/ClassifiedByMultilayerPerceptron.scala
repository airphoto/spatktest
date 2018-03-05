package com.lhs.spark.ml.clz

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

/**
  * 多层感知器分类
  */
object ClassifiedByMultilayerPerceptron {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("classes").getOrCreate()
    import spark.implicits._

    val data = spark.read.format("lbsvm").load("E:\\sources\\spark-2.2.0\\data\\mllib\\sample_multiclass_classification_data.txt")

    val Array(train,test) = data.randomSplit(Array(0.6,0.4),seed = 123L)

    //指定神经网络的层数
    //输入层的大小是4（4个特征），中间层大小分别是 5 和 4 ，输出的大小是 3（3类）
    val layers = Array(4,5,4,3)

    val trainer = new MultilayerPerceptronClassifier().setLayers(layers).setBlockSize(128).setSeed(1234L).setMaxIter(100)

    val model = trainer.fit(train)

    val result = model.transform(test)

    val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")

    println("Test set accuracy = " + evaluator.evaluate(result))
    spark.close()
  }
}
