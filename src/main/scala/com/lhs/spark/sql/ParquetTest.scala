package com.lhs.spark.sql

import org.apache.spark.sql.SparkSession

object ParquetTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val df = spark.read.json("F:\\tmpdata\\json\\client\\clientinfologs\\2018-05-30")
    df.write.mode("append").partitionBy("version").parquet("F:\\tmpdata\\json\\client\\clientinfologs\\save")
  }
}
