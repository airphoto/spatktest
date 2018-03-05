package com.lhs.spark.streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKStringSerializer, ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by abel on 16-11-7.
  */
object DirectStreaming {
/*//  def main(args: Array[String]) {
//    val ssc = StreamingContext.getOrCreate(".",
//      () => {
//        createContext()
//      })
//
//    ssc.start()
//    ssc.awaitTermination()
//  }

  def main (args: Array[String]) {
    val Array(kafkaBrokers,zkServers,topic,groupId) = Array("rg-storm1:9092,rg-storm2:9092,rg-storm3:9092","rg-storm1:2181,rg-storm2:2181,rg-storm3:2181","tk","spark")
    val kafkaParams = Map("bootstrap.servers" -> kafkaBrokers,
      "group.id" -> groupId
    )

    @transient val zkClient = new ZkClient(zkServers, 2000, 2000, ZKStringSerializer)
    //topic 对应的partitions
    val partitionsAndTopics = ZkUtils.getPartitionsForTopics(zkClient, List(topic))
    //在kafka中每个partition上消费开始的offset ， createDirectStream 需要的参数
    var fromOffsets: Map[TopicAndPartition, Long] = Map()


    partitionsAndTopics.foreach(topic2Partitions => {
      val topic : String = topic2Partitions._1
      val partitions : Seq[Int] = topic2Partitions._2
      //kafka 消费这组对应的topic路径
      val topicDirs = new ZKGroupTopicDirs(groupId, topic)

      partitions.foreach(partition => {
        //partition 在zk中的路径
        val zkPath = s"${topicDirs.consumerOffsetDir}/$partition"
        //确保路径存在
        ZkUtils.makeSurePersistentPathExists(zkClient, zkPath)
        //在zk中读取partition的offset
        val untilOffset = zkClient.readData[String](zkPath)
        val tp = TopicAndPartition(topic, partition)
        val offset = try {
          //如果在zk中没有读取数据的话就在其他的znode中读取
          if (untilOffset == null || untilOffset.trim == "")
            KafkaUtil.getMaxOffset(tp, zkClient)
          else
            untilOffset.toLong
        } catch {
          case e: Exception => KafkaUtil.getMaxOffset(tp, zkClient)
        }
        fromOffsets += (tp -> offset)
        println(s"Offset init: set offset of $topic/$partition as $offset")
      })
    })


//    val (fromOffsets,zkClient) = KafkaUtil.getFromOffset(zkServers,topic,groupId)
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    var offsetRanges = Array[OffsetRange]()

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("log streaming")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerialization")

    sparkConf.registerKryoClasses(Array(classOf[KafkaUtil]))

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))
//    ssc.checkpoint(".")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

    messages.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD(rdd => {
      offsetRanges.foreach(o => {
        val topicDirs = new ZKGroupTopicDirs(groupId, o.topic)
        val zkOffsetPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        val zkGroupPath = s"${topicDirs.consumerGroupDir}/owner/tk/${o.partition}"
        println(zkGroupPath+"->"+o.topicAndPartition())
        println(zkOffsetPath + " -> " + o.untilOffset.toString)
        ZkUtils.updatePersistentPath(zkClient, zkOffsetPath, o.untilOffset.toString)
        ZkUtils.updatePersistentPath(zkClient,zkGroupPath,o.untilOffset.toString)
      })
    })

    val line = messages.mapPartitions(x=>x.flatMap(_._2.split(" ")))
    line.print(10)
    ssc.start()
    ssc.awaitTermination()
  }*/
}
