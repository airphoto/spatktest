package com.lhs.spark.kaggle

import com.lhs.spark.ml.MLUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
object test {
  def main(args: Array[String]): Unit = {
    val spark = MLUtils.getSpark
    import spark.implicits._
    val df = spark.read.textFile("")

  }
}
