package com.lhs.spark.test.streaming

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.{BrokerNotAvailableException, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{Json, ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by abel on 16-11-7.
  */
object DirectStreaming {
  def main(args: Array[String]) {
    val kafkaBrokers ="192.168.88.128:9092"
    val zkServers = "192.168.88.128:2181"
    val topic = "direct"
    val groupid = "spark"
    val kafkaParams = Map("bootstrap.servers" -> kafkaBrokers,
      "group.id" -> groupid
    )

    val zkClient = new ZkClient(zkServers, 2000, 2000, ZKStringSerializer)
    val partitionsAndTopics = ZkUtils.getPartitionsForTopics(zkClient, List(topic))
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    partitionsAndTopics.foreach(topic2Partitions => {
      val topic : String = topic2Partitions._1
      val partitions : Seq[Int] = topic2Partitions._2
      val topicDirs = new ZKGroupTopicDirs(groupid, topic)

      partitions.foreach(partition => {
        val zkPath = s"${topicDirs.consumerOffsetDir}/$partition"
        ZkUtils.makeSurePersistentPathExists(zkClient, zkPath)
        val untilOffset = zkClient.readData[String](zkPath)
        val tp = TopicAndPartition(topic, partition)
        val offset = try {
          if (untilOffset == null || untilOffset.trim == "")
            getMaxOffset(tp, zkClient)
          else
            untilOffset.toLong
        } catch {
          case e: Exception => getMaxOffset(tp, zkClient)
        }
        fromOffsets += (tp -> offset)
        println(s"Offset init: set offset of $topic/$partition as $offset")
      })
    })

    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())


    var offsetRanges = Array[OffsetRange]()

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("log streaming")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(30))
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)


    messages.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD(rdd => {
      offsetRanges.foreach(o => {
        val topicDirs = new ZKGroupTopicDirs(groupid, o.topic)
        val zkOffsetPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        println(zkOffsetPath + " -> " + o.untilOffset.toString)
        ZkUtils.updatePersistentPath(zkClient, zkOffsetPath, o.untilOffset.toString)
      })
    })



    ssc.start()
    ssc.awaitTermination()
  }



  private def getMaxOffset(tp : TopicAndPartition, zkClient: ZkClient):Long = {

    val request = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))

    ZkUtils.getLeaderForPartition(zkClient, tp.topic, tp.partition) match {
      case Some(brokerId) => {
        ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId)._1 match {
          case Some(brokerInfoString) => {
            Json.parseFull(brokerInfoString) match {
              case Some(m) =>
                val brokerInfo = m.asInstanceOf[Map[String, Any]]
                val host = brokerInfo.get("host").get.asInstanceOf[String]
                val port = brokerInfo.get("port").get.asInstanceOf[Int]
                new SimpleConsumer(host, port, 10000, 100000, "getMaxOffset")
                  .getOffsetsBefore(request)
                  .partitionErrorAndOffsets(tp)
                  .offsets
                  .head
              case None =>
                throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
            }
          }
          case None =>
            throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
        }
      }
      case None =>
        throw new Exception("No broker for partition %s - %s".format(tp.topic, tp.partition))
    }
  }
}
