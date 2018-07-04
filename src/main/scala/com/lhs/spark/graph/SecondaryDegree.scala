package com.lhs.spark.graph

import org.apache.spark.sql.SparkSession

object SecondaryDegree {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val df = spark.createDataset(Seq("1,2,3","1,2,4","1,2,5","1,2,6","1,3,6"))

    spark.stop()
  }
}
