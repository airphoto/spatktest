package com.lhs.spark.core.streaming

import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


/**
 * Hello world!
 *
 */
object KafkaProducer{
  def main(args: Array[String]) {
    val Array(brokers,topic,mesagePersec,wordsPermessage) = Array("192.168.0.103:9092","test","1","10")
    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Send some messages
    while(true) {
      (1 to mesagePersec.toInt).foreach { messageNum =>
        val str = (1 to wordsPermessage.toInt).map(x => "d")
          .mkString(" ")

        val message = new ProducerRecord[String, String](topic, null, str)
        println(message)
        producer.send(message)
      }
      Thread.sleep(10)
    }
  }
}
