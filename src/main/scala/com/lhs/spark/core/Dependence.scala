package com.lhs.spark.core

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.io.{NullWritable, LongWritable, Text}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession

/**
 * Created by Administrator on 2017/2/7.
 */

/**
 * <P>@ObjectName : Dependence</P>
 *
 * <P>@USER : abel.li </P>
 *
 * <P>@CREATE_AT : 2017/2/7 </P>
 *
 * <P>@DESCRIPTION : TODO </P>
 */

object Dependence {

  class RDDMutipleTextOutputFormat extends MultipleTextOutputFormat[NullWritable,Any]{
    override def generateFileNameForKeyValue(key: NullWritable, value: Any, name: String): String =
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
    pairData.partitionBy(new HashPartitioner(288)).saveAsHadoopFile("E:\\tmpdata\\minutes",classOf[NullWritable],classOf[String],classOf[RDDMutipleTextOutputFormat])
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
