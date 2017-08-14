package com.lhs.spark.core

import org.apache.spark.sql.SparkSession

object HdfsTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hdfsTest").master("local[*]").getOrCreate()

    val file = spark.read.text("F:\\tmpdata\\test").rdd
    val mapped = file.map(s=>(s,s.length)).cache()
    for (iter <- 1 to 10){
      val start = System.currentTimeMillis()
      for (x<-mapped){println(x._1,x._2 + 2)}
      val end = System.currentTimeMillis()
      val duration= end-start
      println(s"iteration $iter took $duration ms")
    }
    spark.stop()
  }
}
