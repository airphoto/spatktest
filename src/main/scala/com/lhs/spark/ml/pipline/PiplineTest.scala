package com.lhs.spark.ml.pipline

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.Vector

object PiplineTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("NormalLine").master("local").getOrCreate()
    //训练集
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    //配置 pipeline
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.001)
    val stages = Array(tokenizer,hashingTF,lr)
    val pipeline = new Pipeline().setStages(stages)

    //训练模型
    val model = pipeline.fit(training)
    //可把模型存储到磁盘
//    model.write.overwrite().save("pipeline-lr-model")
    //也可以吧pipeline存储到磁盘
//    pipeline.write.overwrite().save("pipeline-model")
    //加载模型
//      val lrModel = PipelineModel.load("pipeline-lr-model")
    //测试集
    model.stages.last.asInstanceOf[LogisticRegressionModel].summary.predictions.show(false)
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark hadoop spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")
    model.transform(test).show(false)
    spark.close()
  }
}
