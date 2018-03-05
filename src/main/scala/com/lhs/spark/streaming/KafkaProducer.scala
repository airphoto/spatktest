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
    val Array(brokers,topic,mesagePersec) = if(args.length==0) Array("10.51.237.173:9092","logs","10") else args
    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    val str = "bswf|[2017-12-20 00:00:00 325]|18001|29|32;1;2;4;3;18;38|8|8|0|10787460;15437977;0;0|10.161.174.24|6007|307|[2017-12-18 23:50:57 000]|699800|1|1|3|3|V4.00.00"



    // Send some messages
    for(i <- 1 to 10) {
      val r = new Random()
      (1 to mesagePersec.toInt).foreach { messageNum =>
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }
    println(i+" ok")
      Thread.sleep(1000)
    }
  }
}
