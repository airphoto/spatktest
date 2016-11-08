package com.lhs.spark.kafka

import kafka.api.{PartitionOffsetRequestInfo, OffsetRequest}
import kafka.common.{BrokerNotAvailableException, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, Json, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{OffsetRange, HasOffsetRanges}

/**
 * Created by Administrator on 2016/11/8.
 */
object KafkaUtil {
  /**
   * 获取offset
   * @param tp
   * @param zkClient
   * @return
   */
  def getMaxOffset(tp : TopicAndPartition, zkClient: ZkClient):Long = {

    val request = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))

    ZkUtils.getLeaderForPartition(zkClient, tp.topic, tp.partition) match {                         //每个partition的leader
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

  def getFromOffset(zkServers:String,topic:String,groupId:String)={
    val zkClient = new ZkClient(zkServers, 2000, 2000, ZKStringSerializer)
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
    (fromOffsets,zkClient)
  }

  def writeToZK(messages:InputDStream[(String,String)]){


  }
}

class KafkaUtil{

}
