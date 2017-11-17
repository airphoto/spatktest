package com.lhs.spark.streaming

/**
  * 1. 程序创建StreamingContext时，如果存在checkpoint目录，则直接根据checkpoint目录中的数据恢复一个StreamingContext，
  * 如果不存在，则会从zookeeper中读取当前consumer groupId消费的offset，然后再从kafka中读取到该topic下最早的的offset，
  * 如果从zookeeper中读取到的offset为空或者小于从kafka中读取到该topic下最早的offset，则采用从kafka中读取的offset，
  * 否则就用zookeeper中存储的offset。
  * 2. 每次生成KafkaRDD之后，都会将它对应的OffsetRange中的untilOffset存入到zookeeper
  */

import java.io.File
import java.util.Properties

import com.lhs.scala.jdbc.ConnectionPool
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.{BrokerNotAvailableException, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._

/**
  * Created by wangwei01 on 2016/11/18.
  *
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: DirectKafkaWordCount <brokers> <topics>
  * <brokers> is a list of one or more Kafka brokers
  * <topics> is a list of one or more kafka topics to consume from
  * <checkpointDirectory> is a string of directory for checkpoint
  *
  * Example:
  * $ spark-submit --master spark://hm01:8070 \
  * --packages org.apache.spark:spark-streaming_2.10:1.6.1,org.apache.spark:spark-streaming-kafka_2.10:1.6.1,org.json4s:json4s-native_2.10:3.4.0,com.alibaba:druid:1.0.26  \
  *         resys.jar \
  * ukafka-4rieyu-2-bj03.service.ucloud.cn:2181 ukafka-4rieyu-2-bj03.service.ucloud.cn:9092 proxy-access-log spark /wv/ch 5 20 20
  */
object SparkStream_kafka {

  def createContext(zkServers: String, brokers: String, topics: String,
                    groupId: String, checkpointDirectory: String, batchDuration: Int,
                    windowDuration: Int, slideDuration: Int): StreamingContext = {

    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    println("-----------------Creating new context!-----------------")
    val checkpointFile = new File(checkpointDirectory)
    if (checkpointFile.exists()) checkpointFile.delete()

    val sparkConf = new SparkConf()
      .setAppName("SparkStream_kafka")
      .setMaster("local[*]")

    // Create the context with a specify second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(batchDuration))
    ssc.checkpoint(checkpointDirectory)

//    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId
      //            "auto.offset.reset" -> OffsetRequest.SmallestTimeString
    )

    val offsets = getOffsets(zkServers, groupId, topics)
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message()) //定义kafka接收的数据格式
    //    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message())

    // Create direct kafka stream with brokers and topics
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
      ssc, kafkaParams, offsets, messageHandler)

    // Hold a reference to the current offset ranges, so it can be used downstream
    var offsetRanges = Array[OffsetRange]()

    val sta = messages.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map(_._2)
      .map(ip => (ip, 1))
      .reduceByKey(_+_)
      .transform(rdd => rdd.sortBy(_._2, ascending = false)) // 按照访问次数降序排列


    // 将kafkaRDD的untilOffset更新到zookeeper中
    messages.foreachRDD { rdd =>
      // val zkClient = new ZkClient(zkServers)
      val zkClient = new ZkClient(zkServers, Integer.MAX_VALUE, 10000, ZKStringSerializer)
      val topicDirs = new ZKGroupTopicDirs(groupId, topics)
      //      println(s"rdd: $rdd")
      for (offset <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${offset.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, offset.untilOffset.toString)
        //        println(s"[${offset.topic},${offset.partition}]: ${offset.fromOffset},${offset.untilOffset}")
      }
    }

    sta.print()

    ssc
  }

  def main(args: Array[String]): Unit = {
//    if (args.length < 8) {
//      System.err.println("Your arguments were " + args.mkString("[", ", ", "]"))
//      System.err.println(
//        s"""
//           |Usage: SparkStream_kafka <brokers> <topics> <groupId> <checkpointDirectory> <batchDuration> <windowDuration> <slideDuration>
//           |  <brokers> is a list of one or more Kafka brokers
//           |  <topics> is a list of one or more kafka topics to consume from
//           |  <groupId> is a kafka consumer group
//           |  <checkpointDirectory> is a string of directory for checkpoint
//           |  <batchDuration> is the time interval at which streaming data will be divided into batches
//           |  <windowDuration> is width of the window; must be a multiple of this DStream's batching interval
//           |  <slideDuration> sliding interval of the window (i.e., the interval after which the new DStream will generate RDDs); must be a multiple of this DStream's batching interval
//""".stripMargin)
//      System.exit(1)
//    }

    //    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    Logger.getRootLogger.setLevel(Level.WARN)

    val Array(zkServers, brokers, topics, groupId, checkpointDirectory, batchDurationStr, windowDurationStr, slideDurationStr) =
      if(args.length == 8) args
      else Array("datanode1:2181,datanode2:2181,datanode3:2181","datanode1:9092,datanode2:9092,datanode3:9092","test","streaming_test","sss","20","10","10")
    val batchDuration = batchDurationStr.toInt
    val windowDuration = windowDurationStr.toInt
    val slideDuration = slideDurationStr.toInt


    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => createContext(zkServers, brokers, topics, groupId, checkpointDirectory, batchDuration, windowDuration, slideDuration))


    ssc.start()
    ssc.awaitTermination()

  }


  def getOffsets(zkServers: String, groupId: String, topics: String): Map[TopicAndPartition, Long] = {
    //  val zkClient = new ZkClient(zkServers)
    // 必须要使用带有ZkSerializer参数的构造函数来构造，否则在之后使用ZkUtils的一些方法时会出错，而且在向zookeeper中写入数据时会导致乱码
    // org.I0Itec.zkclient.exception.ZkMarshallingError: java.io.StreamCorruptedException: invalid stream header: 7B227665
    val zkClient = new ZkClient(zkServers, Integer.MAX_VALUE, 10000, ZKStringSerializer)

    val topicPartitions = ZkUtils.getPartitionsForTopics(zkClient, topics.split(",")).head

    val topic = topicPartitions._1
    val partitions = topicPartitions._2

    val topicDirs = new ZKGroupTopicDirs(groupId, topic)

    var offsetsMap: Map[TopicAndPartition, Long] = Map()

    partitions.foreach { partition =>
      val zkPath = s"${topicDirs.consumerOffsetDir}/$partition" // /consumers/[groupId]/offsets/[topic]/partition
      //      ZkUtils.makeSurePersistentPathExists(zkClient, zkPath) // 如果zookeeper之前不存在该目录，就直接创建

      val tp = TopicAndPartition(topic, partition)
      // 得到kafka中该partition的最早时间的offset
      val offsetForKafka = getOffsetFromKafka(zkServers, tp, OffsetRequest.EarliestTime)

      // 得到zookeeper中存储的该partition的offset
      val offsetForZk = ZkUtils.readDataMaybeNull(zkClient, zkPath) match {
        case (Some(offset), stat) =>
          Some(offset)
        case (None, stat) => // zookzeeper中未存储偏移量
          None
      }

      if (offsetForZk.isEmpty || offsetForZk.get.toLong < offsetForKafka) {
        // 如果zookzeeper中未存储偏移量或zookzeeper中存储的偏移量已经过期
        println("Zookeeper don't save offset or offset has expire!")
        offsetsMap += (tp -> offsetForKafka)
      } else {
        offsetsMap += (tp -> offsetForZk.get.toLong)
      }

    }
    println(s"offsets: $offsetsMap")
    offsetsMap
  }

  private def getOffsetFromKafka(zkServers: String, tp: TopicAndPartition, time: Long): Long = {
    val zkClient = new ZkClient(zkServers, Integer.MAX_VALUE, 10000, ZKStringSerializer)
    val request = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(time, 1)))

    //  得到每个分区的leader（某个broker）
    ZkUtils.getLeaderForPartition(zkClient, tp.topic, tp.partition) match {
      case Some(brokerId) =>
        // 根据brokerId得到leader这个broker的详细信息
        ZkUtils.getBrokerInfo(zkClient, brokerId) match {
          case Some(broker) =>
            // 使用leader的host和port来创建一个SimpleConsumer
            val consumer = new SimpleConsumer(broker.host, broker.port, 10000, 100000, "getOffsetShell")
            val offsets = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(tp).offsets
            offsets.head
          case None =>
            throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
        }
      case None =>
        throw new Exception("No broker for partition %s - %s".format(tp.topic, tp.partition))
    }

  }

  def generateUniqueID(timeStamp:Long,partitionID:Int)={
    s"$timeStamp,$partitionID"
  }

}

object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}


