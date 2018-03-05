package com.lhs.spark.ml.regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object BikeRegression {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("classes").getOrCreate()
    import spark.implicits._

//    val hour

    spark.close()
  }
}
