package com.lhs.spark.streaming

import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random


/**
 * Hello world!
 *
 */
object KafkaProducer{
  def main(args: Array[String]) {
    val Array(brokers,topic,mesagePersec,wordsPermessage) = if(args.length==0) Array("datanode1:9092,datanode2:9092,datanode3:9092","test","80000","10") else args
    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Send some messages
    for(i <- 1 to 100) {
      val r = new Random()
      (1 to mesagePersec.toInt).foreach { messageNum =>
        val str = "bswf|[2017-07-19 23:08:02 658]|70088|31|100;73;54;33;106;223;224;104|2|13|0|994530;741041;700648;682002|10.105.109.230|6015|115|[2017-07-19 22:39:09 000]|608248|1|2|1|1|V4.00.00|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
        val message = new ProducerRecord[String, String](topic, null, str)
//        println(message)
        producer.send(message)
      }
    println(i+" ok")
      Thread.sleep(1000)
    }
  }
}
