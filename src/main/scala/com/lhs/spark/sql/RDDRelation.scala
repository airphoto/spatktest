package com.lhs.spark.sql

import org.apache.spark.sql.SparkSession

case class Record(key:Int,value:String)

object RDDRelation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RDDRelation").master("local").getOrCreate()
    import spark.implicits._

    val df = spark.createDataFrame((1 to 100000).map(i=>Record(i,s"val_$i")))
    df.repartition(10).createOrReplaceTempView("records")

    val allData = spark.sql("select * from records")

    val count = spark.sql("select count(*) from records")

//    df.where($"key".contains("1")).orderBy($"value".desc).select($"key").show()

//    df.where($"key".startsWith("1")).select($"value".as("a")).show()
    df.repartition(1).write.parquet("F:\\tmpdata\\test\\result\\test.parquet")
    df.repartition(1).rdd.saveAsTextFile("F:\\tmpdata\\test\\result\\test.txt")
    spark.stop()
  }
}
