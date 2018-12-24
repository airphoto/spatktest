package com.lhs.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by abel on 16-11-6.
  */
object SparkHbaseRead {

  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]) {
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE,"hb_user_info")
    val sparkConf = new SparkConf().setAppName("sparkHbase").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val userRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    val count = userRDD.count()

    println("users count "+count)

    userRDD.map(x=>x._2).foreachPartition(x=>{
      x.foreach(println)
    })


    userRDD.filter(p=> "0002d4cf0b_13001_23431304"==Bytes.toString(p._2.getRow)).map(p=>{
      val result = p._2
      val key = Bytes.toString(result.getRow)
      val bsall7 = Bytes.toString(result.getValue(Bytes.toBytes("cf_user_game"),Bytes.toBytes("bs_all_last_7_days")))
      val bsf7 = Bytes.toString(result.getValue(Bytes.toBytes("cf_user_game"),Bytes.toBytes("bs_finish_last_7_days")))
      val bsall = Bytes.toString(result.getValue(Bytes.toBytes("cf_user_game"),Bytes.toBytes("history_bs_all")))
      val bsfall = Bytes.toString(result.getValue(Bytes.toBytes("cf_user_game"),Bytes.toBytes("history_bs_finish")))
      val lastlog = Bytes.toString(result.getValue(Bytes.toBytes("cf_user_game"),Bytes.toBytes("last_log_time")))
      val prefers = Bytes.toString(result.getValue(Bytes.toBytes("cf_user_game"),Bytes.toBytes("prefers")))
      Array(key,bsall,bsall7).mkString("|")
    }).collect().foreach(println)


    val userCatchData = userRDD.filter(p=> "0002d4cf0b_13001_23431304"==Bytes.toString(p._2.getRow) || "0002e49a95_88888_838285"==Bytes.toString(p._2.getRow)).cache()
    val flatData = userCatchData.flatMap(p=>{
      val result = p._2
      val key = Bytes.toString(result.getRow)
      val bsall7 = Bytes.toString(result.getValue(Bytes.toBytes("cf_user_game"),Bytes.toBytes("bs_all_last_7_day")))
      val bsf7Data = result.getValue(Bytes.toBytes("cf_user_game"),Bytes.toBytes("bs_finish_last_7_days"))
      val bsf7 = try{Bytes.toInt(bsf7Data).toString} catch {case e:Exception=>Bytes.toString(bsf7Data)}
      val bsall = Bytes.toString(result.getValue(Bytes.toBytes("cf_user_game"),Bytes.toBytes("history_bs_all")))
      val bsfall = Bytes.toString(result.getValue(Bytes.toBytes("cf_user_game"),Bytes.toBytes("history_bs_finish")))
      val lastlog = Bytes.toString(result.getValue(Bytes.toBytes("cf_user_game"),Bytes.toBytes("last_log_time")))
      val prefers = Bytes.toString(result.getValue(Bytes.toBytes("cf_user_game"),Bytes.toBytes("prefers")))
      Array(
        (key,"cf_user_game","bs_all_last_7_days",bsall7),
        (key,"cf_user_game","bs_finish_last_7_days",bsf7),
        (key,"cf_user_game","history_bs_all",bsall),
        (key,"cf_user_game","history_bs_finish",bsfall)
      )
    }).toDF("key","cf","c","v").orderBy($"key",$"cf",$"c")

    flatData.map(r=>{
      val key = r.getAs[String]("key")
      val cf = r.getAs[String]("cf")
      val c = r.getAs[String]("c")
      val v = r.getAs[String]("v")
      val kv = new KeyValue(Bytes.toBytes(key),Bytes.toBytes(cf),Bytes.toBytes(c),Bytes.toBytes(v))
      (new ImmutableBytesWritable(), kv)
    }).rdd.saveAsNewAPIHadoopFile("/test/output/hbase/bulk",classOf[ImmutableBytesWritable], classOf[KeyValue],classOf[HFileOutputFormat2])



    userRDD.foreach{case(_,result)=>

        val key = Bytes.toInt(result.getRow)
        val name = Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("name")))
        val age = Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("age")))
        val sex = Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("sex")))

        println(s"row key $key name $name age $age sex $sex")
    }

    sc.stop()
  }

}


object SparkHbaseWrite{
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("sparkHbaseWrite").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val jobConf = new JobConf(conf,this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"user")

    val rawData = List(("1004","lilei","24","man"),("1005","hameimei","25","woman"),("1006","polly","23","man"))
    val localData = sc.parallelize(rawData).map(convert)

    localData.saveAsHadoopDataset(jobConf)
    sc.stop()
  }

  def convert(triple:(String,String,String,String))={
    val p = new Put(Bytes.toBytes(triple._1))
    p.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(triple._2))
    p.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(triple._3))
    p.addColumn(Bytes.toBytes("info"),Bytes.toBytes("sex"),Bytes.toBytes(triple._4))
    (new ImmutableBytesWritable,p)
  }
}
