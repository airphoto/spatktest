package com.lhs.spark.core.streaming

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Administrator on 2016/12/14.
 */

/**
 * <P>@ObjectName : FileStreaming</P>
 *
 * <P>@USER : abel.li </P>
 *
 * <P>@CREATE_AT : 2016/12/14 </P>
 *
 * <P>@DESCRIPTION : TODO </P>
 */

object FileStreaming {

  def getContext(checkdir:String,sparkConf: SparkConf) = {
    val Array(zkQuorum,groupID,topic) = Array("192.168.2.52:2181,192.168.2.53:2181,192.168.2.54:2181","streaming","tk")
    val topicsMap = Map[String,Int](topic->1)

    val ssc = new StreamingContext(sparkConf,Seconds(5))
    ssc.checkpoint(checkdir)
    val sources = (1 to 3).map(x=>KafkaUtils.createStream(ssc,zkQuorum,groupID,topicsMap).map(_._2))
    val messages = ssc.union(sources)
    val pair = messages.map(x=>(x,(x,1)))
    pair.map(x=>"pair-->"+x).print()
    val result = pair.updateStateByKey(addFunc)
    result.map(x=>"result-->"+x).print()
    ssc
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("fileStreaming")
    val checkDir = "./update"
    val ssc = StreamingContext.getOrCreate(checkDir,
      ()=>getContext(checkDir,sparkConf)
    )
    ssc.start()
    ssc.awaitTermination()
  }

  //updateStateByKey 使用的函数
  val addFunc2 = (currValues: Seq[Int], prevValueState: Option[Int]) => {
    //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和

    val currentCount = currValues.sum
    // 已累加的值
    val previousCount = prevValueState.getOrElse(0)
    // 返回累加后的结果，是一个Option[Int]类型
    Some(currentCount + previousCount)
  }

  val addFunc = (currValues: Seq[Tuple2[String,Int]], prevValueState: Option[Tuple2[String,Int]]) => {
    //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和

    println("current-> "+currValues.mkString(","))
    println("pre ->> "+prevValueState)
    val prev = prevValueState.getOrElse(("",0))
//    val prev = prevValueState.getOrElse(("",0))
    val key = if(!currValues.isEmpty)currValues.head._1 else prev._1
    if(key != "" && !getCurrentMinute.contains(key)){
      None
    }
    else {
      val currentCount = currValues.map(_._2).sum
      // 已累加的值
      val previousCount = prev._2
      // 返回累加后的结果，是一个Option[Int]类型
      Some((key,currentCount + previousCount))
    }
  }

  def getCurrentMinute={
    val format = new SimpleDateFormat("yyyyMMddHHmm")
    val calendar = Calendar.getInstance()
    val minute = format.format(calendar.getTime)
    minute
  }

}
