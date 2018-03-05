package com.lhs.spark.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val flume = spark.read.textFile("/test/input/logs/2017-09-01/flumedir")
    val flumeMapData = flume.map(line=>{
      (encoderByMD5(line),line,2)
    }).toDF("encode","line","tag")

    val scp = spark.read.textFile("/test/input/logs/2017-09-01/scpdir")
    val scpMapData = scp.map(line=>{
      (encoderByMD5(line),line,1)
    }).toDF("encode","line","tag")

    import org.apache.spark.sql.functions._
    val unionData = flumeMapData.union(scpMapData).groupBy("encode").agg(count("encode").as("e"),sum("tag").as("tags"))
    unionData.printSchema()
    val tagData = unionData.filter($"e" < 2l || $"tags" < 3l).cache()
    val targetData= flumeMapData.union(scpMapData).join(broadcast(tagData),"encode")
    targetData.coalesce(1).map(_.mkString(",")).rdd.saveAsTextFile("/test/output/flume/other")
    val otherData = unionData.join(broadcast(tagData),"encode").persist(StorageLevel.MEMORY_AND_DISK_SER)
    otherData.unpersist()
    spark.stop()
  }

  def encoderByMD5(str: String)={
    import sun.misc.BASE64Encoder
    import java.security.MessageDigest
    //确定计算方法
    val md5 = MessageDigest.getInstance("MD5")
    val base64en = new BASE64Encoder
    //加密后的字符串
    base64en.encode(md5.digest(str.getBytes("utf-8")))
  }
}
