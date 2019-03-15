package com.lhs.spark.ml.etl.transform

import com.lhs.spark.ml.MLUtils
import org.apache.spark.ml.feature.NGram

/**
  * 描述
  * 一个 n-gram是一个长度为n（整数）的字的序列。NGram可用于将输入特征转换成n-grams。
  *
  * N-Gram 的输入为一系列的字符串（例如：Tokenizer分词器的输出）。
  * 参数 n 表示每个 n-gram 中单词（terms）的数量。
  * 输出将由 n-gram 序列组成，
  * 其中每个 n-gram 由空格分隔的 n 个连续词的字符串表示。
  * 如果输入的字符串序列少于n个单词，NGram 输出为空。
  *
  * @author lihuasong
  *
  *         2019-03-13 20:08
  **/
object NGramTest {
  def main(args: Array[String]): Unit = {
    val spark = MLUtils.getSpark
    import spark.implicits._

    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat")),
      (2, Array("Logistic2"))
    )).toDF("id", "words")

    val ngram = new NGram()
      .setN(2)
      .setInputCol("words")
      .setOutputCol("features")

    val df = ngram.transform(wordDataFrame)

    df.show(false)

  }
}
