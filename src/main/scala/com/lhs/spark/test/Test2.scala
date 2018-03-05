package com.lhs.spark.test

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, TextInputFormat}
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.sql.{SQLContext, SparkSession}

object Test2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val agent = spark.read.textFile("/dbdata/agent/all/2017-12-01")
    val agentDF = agent.map(line=>{
      val field = line.split("\t")
      (field(1),field(23),field(13).toInt)
    }).toDF("agent","server","remain")

    val charges = spark.sparkContext.hadoopFile("/dbdata/player_charge/byday/2017-11*,/dbdata/player_charge/byday/2017-12-01",classOf[TextInputFormat],classOf[LongWritable],classOf[Text])
    val chargeHadoopRDD = charges.asInstanceOf[HadoopRDD[LongWritable,Text]]
    val chargeWithDay = chargeHadoopRDD.mapPartitionsWithInputSplit((input,it)=>{
      val fileSplit = input.asInstanceOf[FileSplit]
      it.map(line=>{
        val field = line._2.toString.split("\t")
        (fileSplit.getPath.getParent.getName,field(2),field(8),field(3))
      })
    }).toDF("data_day","agent","server","charge_type")

    chargeWithDay.filter($"data_day"==="2017-12-01").groupBy("charge_type").count()

    val orders = spark.sparkContext.hadoopFile("/dbdata/order_card/byday/2017-11*,/dbdata/order_card/byday/2017-12-01",classOf[TextInputFormat],classOf[LongWritable],classOf[Text])
    val orderHadoopRDD = orders.asInstanceOf[HadoopRDD[LongWritable,Text]]
    val orderWithDay = orderHadoopRDD.mapPartitionsWithInputSplit((input,it)=>{
      val fileSplit = input.asInstanceOf[FileSplit]
      it.map(line=>{
        val field = line._2.toString.split("\t")
        (fileSplit.getPath.getParent.getName,field(13),field(17),field(11),field(14))
      })
    }).toDF("data_day","agent","server","buyWay","player_typpe")

    orderWithDay.filter($"data_day"==="2017-12-01" && ($"buyWay"==="ONPAY" || $"buyWay"==="ONPAY_DO" || $"player_typpe" === "V")).groupBy("buyWay","player_typpe").count()
    import org.apache.spark.sql.functions._

    val unionDF = chargeWithDay.union(orderWithDay).toDF("data_day","agent","server").filter($"data_day"=!="2017-11-01")

    unionDF.select("agent","server")


    unionDF.select("agent").distinct()

    val activeToday = agentDF.join(unionDF.filter($"data_day"==="2017-12-01").distinct(),Seq("agent","server"))
    val activeAll = agentDF.join(unionDF.select("agent","server").distinct(),Seq("agent","server"))
//
//      val sortData = unionDF.sort($"data_day".desc)
//    unionDF.filter($"agent"==="277332000")

    val maxData = unionDF.groupBy("agent","server").agg(max($"data_day").as("max_day"))
//    maxData.filter($"agent"==="277332000")
    val joinData = agentDF.join(maxData,Seq("agent","server"),"left")
    val selectData = joinData.select($"agent",$"server",$"remain",when($"max_day".isNull,2).otherwise(when($"max_day"==="2017-12-01",0).otherwise(1)).as("count_type"))
    val countData = selectData.groupBy("server","count_type","remain").agg(countDistinct("agent").as("agent_count")).persist()


    countData.groupBy("count_type").agg(sum($"remain"*$"agent_count")).show(false)
//
//    val charge = spark.read.textFile("/dbdata/player_charge/byday/2017-11-30")
//    val order = spark.read.textFile("/dbdata/order_card/byday/2017-11-30")
//
//
//    val chargeDF = charge.map(line=>{
//      val field = line.split("\t")
//      (field(2),field(8))
//    })
//
//    val orderDF = order.map(line=>{
//      val field = line.split("\t")
//      (field(13),field(17))
//    })


//    val unionDF = chargeDF.union(orderDF).distinct().toDF("agent","server")


    val activeDf = agentDF.join(unionDF,Seq("agent","server")).select("agent","server","remain")
    activeDf.select("server","remain")
    val cardDist = activeDf.groupBy("remain","server").agg(count("remain").as("count")).cache()
    cardDist.filter($"server"==="sichuan_db")
    cardDist.sort($"remain".cast("int").desc)
    spark.close()
  }
}
