package com.lhs.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by abel on 16-9-21.
  */
object KafkaString {
  def main(args: Array[String]) {


    Logger.getRootLogger.setLevel(Level.ERROR)

    val Array(zkQuorum,group,topics,numThreads) = Array("datanode1:2181,datanode2:2181,datanode3:2181","streaming_test","test","2")
    val sparkConf = new SparkConf().setAppName("kafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap).map(_._2)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x=>(x,1L))
      .reduceByKeyAndWindow(_+_,_-_,Seconds(20),Seconds(2),2)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
