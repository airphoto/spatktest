package com.lhs.spark.test.streaming

import com.lhs.spark.hbase.HBaseUtils
import com.lhs.spark.kafka.KafkaUtil
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ ZKGroupTopicDirs, ZkUtils}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by abel on 16-11-7.
  */
object DirectStreaming {
  def main(args: Array[String]) {

    val Array(kafkaBrokers,zkServers,topic,groupId) = Array("192.168.88.128:9092","192.168.88.128:2181","direct","spark")
    val kafkaParams = Map("bootstrap.servers" -> kafkaBrokers,
                          "group.id" -> groupId
                          )
    val (fromOffsets,zkClient) = KafkaUtil.getFromOffset(zkServers,topic,groupId)
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    var offsetRanges = Array[OffsetRange]()

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("log streaming")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerialization")

    sparkConf.registerKryoClasses(Array(classOf[KafkaUtil]))

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    messages.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD(rdd => {
      offsetRanges.foreach(o => {
        val topicDirs = new ZKGroupTopicDirs(groupId, o.topic)
        val zkOffsetPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
//        println(zkOffsetPath + " -> " + o.untilOffset.toString)
        ZkUtils.updatePersistentPath(zkClient, zkOffsetPath, o.untilOffset.toString)
      })
    })

    val line = messages.mapPartitions(x=>x.flatMap(_._2.split(" ")))
    val wordCounts = line.mapPartitions(x=>{
      x.map((_,1L))
    })

    wordCounts.foreachRDD(rdd=>{
      if (rdd!=null)
        rdd.foreachPartition(par=>{
        val connection = HBaseUtils.getConnection
        val table = HBaseUtils.getTable(connection,"word")
        par.map(x=> {
          val rk = Bytes.toBytes("10005")
          val cf = Bytes.toBytes("info")
          val word = Bytes.toBytes(x._1)
          (rk,cf,word,x._2)
        }
        ).foreach(x=>table.incrementColumnValue(x._1,x._2,x._3,x._4))
        HBaseUtils.close(connection,table)// 必须关闭connection和table
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
