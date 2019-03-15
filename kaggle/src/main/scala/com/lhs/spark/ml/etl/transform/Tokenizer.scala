package com.lhs.spark.ml.etl.transform

import com.lhs.spark.ml.MLUtils
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.functions._

/**
  * tokenizer 主要有两个功能，1. 将词语转为小写字母 2. 按空格划分字符串
  *
  * @author lihuasong
  *
  *         2019-03-12 12:51
  **/
object Tokenizer {
  def main(args: Array[String]): Unit = {

    val spark = MLUtils.getSpark
    import spark.implicits._

    val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("id", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val regxTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W")
//      .setGaps(false)

    // 自定义一个函数
    val countTokens = udf { (words: Seq[String]) => words.length }

    val tokenized = tokenizer.transform(sentenceDataFrame)
    val regexTokenized = regxTokenizer.transform(sentenceDataFrame)

    tokenized.select("sentence","words")
      .withColumn("tokens",countTokens(col("words")))
      .show(false)


    regexTokenized.select("sentence","words")
      .withColumn("tokens",countTokens($"words"))
      .show(false)

//    很明显的可以看到二者的区别，tokenized是将非空格分割的字符串当做 一个整体。
  }
}
