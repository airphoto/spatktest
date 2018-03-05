package com.lhs.spark.core.multipleOutPut

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * <P>@ObjectName : Dependence</P>
 *
 * <P>@USER : abel.li </P>
 *
 * <P>@CREATE_AT : 2016/2/7 </P>
 *
 * <P>@DESCRIPTION : TODO </P>
 */

object Dependence {

  class RDDMutipleTextOutputFormat extends org.apache.hadoop.mapred.lib.MultipleTextOutputFormat[org.apache.hadoop.io.NullWritable,Any]{
    override def generateFileNameForKeyValue(key: org.apache.hadoop.io.NullWritable, value: Any, name: String): String =
      value.asInstanceOf[String].split(",")(1)
//    key.asInstanceOf[String]
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("dep")
    val sc = new SparkContext(sparkConf)
    val keyMinuteMap = sc.broadcast(getMinuteKey(month = 3))
    val df = sc.textFile("E:\\tmpdata\\stat_date=20170320")
    val pairData = df.map(x=>{
      val endTime = x.split("\\|")(11).substring(0,16)
      val index = keyMinuteMap.value.getOrElse(endTime,0)
      (null,x+"||xx"+","+index)
    })
//    pairData.partitionBy(new HashPartitioner(100)).saveAsHadoopFile("E:\\tmpdata\\index",classOf[String],classOf[String],classOf[RDDMutipleTextOutputFormat])
    pairData.saveAsHadoopFile("E:\\tmpdata\\minutes",classOf[NullWritable],classOf[String],classOf[RDDMutipleTextOutputFormat])
    sc.stop()


  }

  def getMinuteKey(year:Int=2017,month:Int=3,day:Int=20,hour:Int=0,minute:Int=0)={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val calendar = Calendar.getInstance()
    (0 until 1440).map(x=>{
      calendar.set(year,month-1,day,hour,minute)
      calendar.add(Calendar.MINUTE,x)
      (format.format(calendar.getTime),(x/5).toString)
    }).toMap
  }
}
