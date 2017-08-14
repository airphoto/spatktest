package com.lhs.spark.core.checkInput

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, TextInputFormat}
import org.apache.spark.rdd.{HadoopRDD, NewHadoopRDD}
import org.apache.spark.sql.SparkSession

object WithPartitionPath {
  def main(args: Array[String]): Unit = {
    val path = "F:\\tmpdata\\test\\resources\\people.txt"
    val spark = SparkSession.builder().master("local").appName("withPartitionPath").getOrCreate()
    readWithHadoopFile(spark,path)
    readWithNewAPIHadoopFile(spark,path)
    spark.close()
  }

  def readWithHadoopFile(spark: SparkSession,path:String): Unit ={
    val rdd = spark.sparkContext.hadoopFile(path,classOf[TextInputFormat],classOf[LongWritable],classOf[Text])
    val hadoopRDD = rdd.asInstanceOf[HadoopRDD[LongWritable,Text]]
    val withName = hadoopRDD.mapPartitionsWithInputSplit((input,it)=>{
      val fileSplit = input.asInstanceOf[FileSplit]
      it.map(x=>{
        fileSplit.getPath.getName+","+x._2
      })
    })

    withName.collect().foreach(println)
  }

  def readWithNewAPIHadoopFile(sparkSession: SparkSession,path:String)={
    val rdd = sparkSession.sparkContext.newAPIHadoopFile[LongWritable,Text,org.apache.hadoop.mapreduce.lib.input.TextInputFormat](path)
    val hadoopRDD = rdd.asInstanceOf[NewHadoopRDD[LongWritable,Text]]
    val withPath = hadoopRDD.mapPartitionsWithInputSplit((inputSplit,it)=>{
      val fileSplit = inputSplit.asInstanceOf[org.apache.hadoop.mapreduce.lib.input.FileSplit]
      it.map(x=>fileSplit.getPath.getName+","+x._2.toString)
    })

    withPath.collect().foreach(println)
  }
}
