package com.lhs.spark.ml.pipeline

import com.lhs.spark.ml.MLUtils
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}

/**
  * 描述
  *
  * MLlib 将机器学习算法的API标准化，以便将多种算法更容易地组合成单个 Pipeline （管道）或者工作流。本节介绍Pipelines API 的关键概念,其中 Pipeline（管道）的概念主要是受到 scikit-learn 项目的启发.
  *
  * DataFrame（数据模型）：ML API 将从Spark SQL查出来的 DataFrame 作为 ML 的数据集,数据集支持许多数据类型。
  * 例如,一个 DataFrame 可以有不同的列储存 text（文本）、feature（特征向量）、true labels（标注）、predictions（预测结果）等机器学习数据类型.
  *
  *
  * Transformer（转换器）：使用 Transformer 将一个 DataFrame 转换成另一个 DataFrame 的算法，
  * 例如，一个 ML Model 是一个 Transformer,它将带有特征的 DataFrame 转换成带有预测结果的 DataFrame.
  *
  *
  * Estimator（模型学习器）：Estimator 是一个适配 DataFrame 来生成 Transformer（转换器）的算法.
  * 例如,一个学习算法就是一个 Estimator 训练 DataFrame 并产生一个模型的过程.
  *
  * Pipeline（管道）：Pipeline 将多个 Transformers 和 Estimators 绑在一起形成一个工作流.
  *
  * Parameter（参数）：所有的 Transformers 和 Estimators 都已经使用标准的 API 来指定参数.
  *
  * @author lihuasong
  *
  *         2019-03-13 09:33
  **/
object PipelineTest {
  def main(args: Array[String]): Unit = {

    val spark = MLUtils.getSpark
    import spark.implicits._

    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    // 配置一个ML的pipeline，含有3个stage   tokenizer，hashingTF 还有 lr
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    val hasingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer,hasingTF,lr))

    // pipeline生的模型
    val model = pipeline.fit(training)

    // 可以将生成的模型放到磁盘上
    model.write.overwrite().save("model/lr")

    // 也可以将pipeline保存起来
    pipeline.write.overwrite().save("pipeline/lr")

    // 可以加载存放在磁盘上的模型
    val saveModel = PipelineModel.load("model/lr")

    // 准备测试数据集
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark hadoop spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    model.transform(test)
      .show(false)

    spark.stop()














  }
}
