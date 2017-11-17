package com.lhs.spark.core.session

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2017/6/16.
 */

/**
 * <P>@ObjectName : CallingSort</P>
 *
 * <P>@USER : abel.li </P>
 *
 * <P>@CREATE_AT : 2017/6/16 </P>
 *
 * <P>@DESCRIPTION : TODO </P>
 */

object CallingSort {
  def main(args: Array[String]) {
    val spark = SparkSession
                        .builder()
                        .master("local[*]")
                        .appName("CallingSort")
                        .getOrCreate()

    import spark.implicits._

    //群注册数据
    val df = spark.read.option("header","true").csv("F:\\tmpdata\\data\\all_qun.csv")
    val qunDabiaoData = df.map(x=>{
      val qunID = x.getAs[String]("qun_id")
      val tijiao = x.getAs[String]("tijiao_time")
      val shenhe =  x.getAs[String]("shenhe_time")
      val dabiao = x.getAs[String]("dabiao_time")
      val agentID = x.getAs[String]("agent_id")
      val tijiaoTime = if(tijiao=="-") shenhe else tijiao
      val dabiaoTime = if(dabiao=="-") getTimeDaysLater(tijiaoTime,5) else dabiao
      (qunID,(dabiaoTime,agentID))
    }).rdd.collectAsMap()

    //达标时间和群ID的映射
    val broadDabiao = spark.sparkContext.broadcast(qunDabiaoData)

    //具体数据
    val detail = spark.read.option("header","true").csv("F:\\tmpdata\\data\\detail_information_0814.csv")

    val detailRDD = detail.mapPartitions(it=>{
      it.map(x=>{
      val qunID = x.getAs[String]("qun_id")
      val day = x.getAs[String]("day")
      val newBarNum = x.getAs[String]("newbarnum")
      (qunID,newBarNum,day,daysOfTwo(broadDabiao.value(qunID)._1,day))
    })}).rdd

    val filterData = detailRDD.mapPartitions(it=>{
      it.map(x=>{
        val (dabiaoTime,agentID) = broadDabiao.value(x._1)
        val (qunID,newBarNum,day,days) = x
        (qunID,dabiaoTime,day,newBarNum,agentID,days)
      })
    })
    filterData.take(10).foreach(println)
//
//    //计算出最近的一次时间
//    val zero = ArrayBuffer[(String,String,String,String)]()
//    val addToList = (buffer:ArrayBuffer[(String,String,String,String)],v:(String,String,String,String)) => buffer += v
//    val combine = (buffer1:ArrayBuffer[(String,String,String,String)],buffer2:ArrayBuffer[(String,String,String,String)]) => (buffer1 ++= buffer2).sortWith((x,y)=>maxOfTwo(x._2,y._2) < 0)
//    val daysData = filterData.map(x=>(x._1,(x._2,x._3,x._4,x._5))).aggregateByKey(zero)(addToList,combine)
    val daysData2 = filterData.map(x=>(x._1,(x._2,x._3,x._4,x._5,x._6))).reduceByKey((x,y)=>{
      if(x._5>y._5) x else y
    }).filter(x=>((x._2._5<=30 && daysOfTwo(x._2._1,"2017/8/16")>30)) || (x._2._5>30 && daysOfTwo(x._2._1,"2017/8/16")>1))
//
//    daysData2.take(10).foreach(println)
//    println(daysData2.count())
    daysData2.map(x=>x._1+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4+","+x._2._5).coalesce(1).saveAsTextFile("F:\\tmpdata\\liushi4")
//    println(daysData2.map(_._1).distinct().count())

    spark.stop()
  }


  def getTimeDaysLater(baseTime:String,days:Int)={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val calendar = Calendar.getInstance()
    calendar.setTime(format.parse(baseTime))
    calendar.set(Calendar.DATE,calendar.get(Calendar.DATE)+days)
    format.format(calendar.getTime)
  }

  def daysOfTwo(dabiao:String, detail:String) = {
    val dabiaoFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val detailFormat = new SimpleDateFormat("yyyy/MM/dd")
    val aCalendar = Calendar.getInstance();
    aCalendar.setTime(dabiaoFormat.parse(dabiao));
    val day1 = aCalendar.get(Calendar.DAY_OF_YEAR);
    aCalendar.setTime(detailFormat.parse(detail));
    val day2 = aCalendar.get(Calendar.DAY_OF_YEAR);
    day2 - day1
  }

  def maxOfTwo(tmp1:String, tmp2:String) = {
    val format = new SimpleDateFormat("yyyy/MM/dd")
    val d1 = format.parse(tmp1)
    val d2 = format.parse(tmp2)
    d1.getTime - d2.getTime
  }
}
