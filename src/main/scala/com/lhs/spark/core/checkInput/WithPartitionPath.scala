package com.lhs.spark.core.checkInput

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, TextInputFormat}
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.sql.SparkSession

object WithPartitionPath {
  def main(args: Array[String]): Unit = {
    val path = "F:\\tmpdata\\test\\resources\\people.txt"
    val spark = SparkSession.builder().master("local").appName("withPartitionPath").getOrCreate()
    val rdd = spark.sparkContext.hadoopFile(path,classOf[TextInputFormat],classOf[LongWritable],classOf[Text])
    val hadoopRDD = rdd.asInstanceOf[HadoopRDD[LongWritable,Text]]
    val withName = hadoopRDD.mapPartitionsWithInputSplit((input,it)=>{
      val fileSplit = input.asInstanceOf[FileSplit]
      it.map(x=>{
        fileSplit.getPath.getName+","+x._2
      })
    })

    withName.collect().foreach(println)
    spark.close()
  }
}
