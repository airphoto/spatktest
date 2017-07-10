package com.lhs.spark.core.customs.format

import com.lhs.spark.core.custom.format.{CustomInputFormat, FindMaxValueInputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.io.{ArrayWritable, IntWritable, LongWritable, Text}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2017/6/22.
 */

/**
 * <P>@ObjectName : FormatTest</P>
 *
 * <P>@USER : abel.li </P>
 *
 * <P>@CREATE_AT : 2017/6/22 </P>
 *
 * <P>@DESCRIPTION : TODO </P>
 */

object FormatTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("format");
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()

    val df = sc.newAPIHadoopFile[Text,Text,CustomInputFormat]("hdfs://192.168.2.32:8020/data/test/tmp/format")
//    val df = sc.newAPIHadoopFile[Text,Text,CustomInputFormat]("dfs:/data/test/tmp/format")
    df.collect().foreach(println)
    sc.stop()
  }
}
