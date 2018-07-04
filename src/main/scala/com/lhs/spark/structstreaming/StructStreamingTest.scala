package com.lhs.spark.structstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

object StructStreamingTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder().master("local[4]").getOrCreate()
    import spark.implicits._

    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "structed")
      .load()

    val words = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val wordCounts = words.flatMap(_._2.split(" ")).groupBy("value").count()
    val query = wordCounts.writeStream
      .outputMode("complete")
      .foreach(RedisWriterTest())
//      .format("console")
      .start()

    query.awaitTermination()
  }
}
