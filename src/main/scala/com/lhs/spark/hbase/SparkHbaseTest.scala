package com.lhs.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by abel on 16-11-6.
  */
object SparkHbaseRead {

  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]) {
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE,"callingnumber_count_day")
    val sparkConf = new SparkConf().setAppName("sparkHbase").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val userRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    val count = userRDD.count()

    println("users count "+count)

    userRDD.map(x=>x._2).foreachPartition(x=>{
      x.foreach(println)
    })


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
