package com.lhs.spark.ml.extracting

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

//Word2Vec是一个Estimator(评估器)，它采用表示文档的单词序列，并训练一个Word2VecModel。
// 该模型将每个单词映射到一个唯一的固定大小向量。 Word2VecModel使用文档中所有单词的平均值将每个文档转换为向量; 该向量然后可用作预测，文档相似性计算等功能。
object Word2VecExample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("NormalLine").master("local").getOrCreate()
    import spark.implicits._

    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)

    val result = model.transform(documentDF)
    result.show(false)

    spark.close()
  }
}
