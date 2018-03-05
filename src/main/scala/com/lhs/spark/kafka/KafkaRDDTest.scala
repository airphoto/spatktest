package com.lhs.spark.kafka

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils, OffsetRange}

object KafkaRDDTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("classes").getOrCreate()
    import spark.implicits._
    val offsetRanges = Array(

      OffsetRange("some-topic", 0, 110, 220),

      OffsetRange("some-topic", 1, 100, 313),

      OffsetRange("another-topic", 0, 456, 789)

    )

    val cluster = new KafkaCluster(Map(""->""))
    cluster
    KafkaUtils.createRDD(spark.sparkContext,Map("key"->"value"),offsetRanges)

    spark.close()
  }
}
